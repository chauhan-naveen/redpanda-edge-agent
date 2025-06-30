package main

import (
	"crypto/tls"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/providers/file"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kversion"
	"github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/tlscfg"

	log "github.com/sirupsen/logrus"
)

const schemaTopic = "_schemas"

var (
	lock   = &sync.Mutex{}
	config = koanf.New(".")
)

// Configuration prefix
type Prefix string

const (
	Source      Prefix = "source"
	Destination Prefix = "destination"
)

type Direction int8

const (
	Push Direction = iota // Push from source topic to destination topic
	Pull                  // Pull from destination topic to source topic
)

func (d Direction) String() string {
	switch d {
	case Push:
		return "push"
	case Pull:
		return "pull"
	default:
		return fmt.Sprintf("%d", int(d))
	}
}

type Config struct {
	Topics map[byte]Topic
}

type Topic struct {
	sourceName            string
	destinationName       string
	direction             Direction
	destinationReplicas   int
	destinationPartitions int
	customPartitioning    bool
	configs               map[string]string `koanf:"configs"`
}

func (t Topic) String() string {
	configStr := ""
	if len(t.configs) > 0 {
		configStr = fmt.Sprintf(", configs=%v", t.configs)
	}
	if t.direction == Push {
		return fmt.Sprintf("%s > %s (partitions=%d, replicas=%d, custom_partitioning=%v%s)",
			t.sourceName, t.destinationName, t.destinationPartitions, t.destinationReplicas, t.customPartitioning, configStr)
	} else {
		return fmt.Sprintf("%s < %s (partitions=%d, replicas=%d, custom_partitioning=%v%s)",
			t.sourceName, t.destinationName, t.destinationPartitions, t.destinationReplicas, t.customPartitioning, configStr)
	}
}

// Returns the name of the topic to consume from.
func (t Topic) consumeFrom() string {
	if t.direction == Push {
		return t.sourceName
	} else {
		return t.destinationName
	}
}

// Returns the name of the topic to produce to.
func (t Topic) produceTo() string {
	if t.direction == Push {
		return t.destinationName
	} else {
		return t.sourceName
	}
}

type SASLConfig struct {
	SaslMethod   string `koanf:"sasl_method"`
	SaslUsername string `koanf:"sasl_username"`
	SaslPassword string `koanf:"sasl_password"`
}

type TLSConfig struct {
	Enabled        bool   `koanf:"enabled"`
	ClientKeyFile  string `koanf:"client_key"`
	ClientCertFile string `koanf:"client_cert"`
	CaFile         string `koanf:"ca_cert"`
}

var defaultConfig = confmap.Provider(map[string]interface{}{
	"id":                            defaultID,
	"create_topics":                 false,
	"max_poll_records":              1000,
	"max_backoff_secs":              600, // ten minutes
	"source.name":                   "source",
	"source.bootstrap_servers":      "127.0.0.1:19092",
	"source.consumer_group_id":      defaultID,
	"destination.name":              "destination",
	"destination.bootstrap_servers": "127.0.0.1:29092",
	"destination.consumer_group_id": defaultID,
}, ".")

// Returns the hostname reported by the kernel to use as the default ID for the
// agent and consumer group IDs
var defaultID = func() string {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("Unable to get hostname from kernel. Set Id in config")
	}
	log.Debugf("Hostname: %s", hostname)
	return hostname
}()

// Initialize the agent configuration from the provided .yaml file
func InitConfig(path *string) {
	lock.Lock()
	defer lock.Unlock()

	config.Load(defaultConfig, nil)
	log.Infof("Init config from file: %s", *path)
	if err := config.Load(file.Provider(*path), yaml.Parser()); err != nil {
		log.Errorf("Error loading config: %v", err)
	}
	log.Debugf("Configuration loaded: %s", config.Sprint())
	validate()
}

// Parse topic configuration
func parseTopics(config *koanf.Koanf, direction Direction) []Topic {
	var all []Topic
	configStr := "source.topics"
	if direction == Pull {
		configStr = "destination.topics"
	}

	// Get all configuration keys
	keys := config.Keys()
	log.Debugf("Keys for %s: %v", configStr, keys)
	processed := make(map[string]bool)
	for _, key := range keys {
		if !strings.HasPrefix(key, configStr+".") {
			continue
		}
		// Extract topic ID (e.g., topic0, topic1)
		parts := strings.Split(key, ".")
		if len(parts) < 3 || parts[1] != "topics" {
			log.Debugf("Skipping key %s: not a topic field", key)
			continue
		}
		topicID := parts[2]
		if processed[topicID] {
			continue
		}
		topicConfig := configStr + "." + topicID
		topic := Topic{
			destinationName:       config.String(topicConfig+".destination"),
			sourceName:            config.String(topicConfig+".source"),
			destinationReplicas:   config.Int(topicConfig+".replicas"),
			destinationPartitions: config.Int(topicConfig+".partition_count"),
			customPartitioning:    config.Bool(topicConfig+".custom_partitioning_enabled"),
			direction:             direction,
		}
		if topic.sourceName == "" || topic.destinationName == "" {
			log.Errorf("Skipping topic %s: source and destination must be set", topicID)
			continue
		}
		if topic.destinationPartitions <= 0 || topic.destinationReplicas <= 0 {
			log.Errorf("Skipping topic %s: partition_count and replicas must be positive", topicID)
			continue
		}
		// Parse topic configurations
		configPath := topicConfig + ".configs"
		if config.Exists(configPath) {
			configs := make(map[string]string)
			if err := config.Unmarshal(configPath, &configs); err != nil {
				log.Errorf("Skipping topic %s: failed to parse configs: %v", topicID, err)
				continue
			}
			topic.configs = configs
		}
		log.Debugf("Discovered topic %s: %v", topicID, topic)
		all = append(all, topic)
		processed[topicID] = true
	}

	log.Debugf("Parsed %d topics for %s: %v", len(all), configStr, all)
	return all
}

func GetTopics(p Prefix) []Topic {
	if p == Source {
		return parseTopics(config, Push)
	} else {
		return parseTopics(config, Pull)
	}
}

func AllTopics() []Topic {
	return append(GetTopics(Source), GetTopics(Destination)...)
}

// Check for circular dependency
func circular(t1, t2 *Topic) bool {
	if t1.direction != t2.direction {
		if t1.sourceName == t2.sourceName && t1.destinationName == t2.destinationName {
			return true
		}
	}
	return false
}

// Validate the config
func validate() {
	config.MustString("id")
	config.MustString("source.bootstrap_servers")
	config.MustString("destination.bootstrap_servers")

	topics := AllTopics()
	if len(topics) == 0 {
		log.Warn("No push or pull topics configured")
		return
	}
	for i, t1 := range topics {
		for k, t2 := range topics {
			// Compare fields explicitly to avoid map comparison
			if i != k {
				if t1.sourceName == t2.sourceName &&
					t1.destinationName == t2.destinationName &&
					t1.direction == t2.direction &&
					t1.destinationPartitions == t2.destinationPartitions &&
					t1.destinationReplicas == t2.destinationReplicas &&
					t1.customPartitioning == t2.customPartitioning {
					log.Fatalf("Duplicate topic configured: %s", t1.String())
				}
			}
			if circular(&t1, &t2) {
				log.Fatalf("Topic circular dependency configured: (%s) (%s)",
					t1.String(), t2.String())
			}
		}
	}
	log.Infof("Validated %d topics: %v", len(topics), topics)
}

// Initializes the necessary TLS configuration options
func TLSOpt(tlsConfig *TLSConfig, opts []kgo.Opt) []kgo.Opt {
	if tlsConfig.Enabled {
		if tlsConfig.CaFile != "" ||
			tlsConfig.ClientCertFile != "" ||
			tlsConfig.ClientKeyFile != "" {
			tc, err := tlscfg.New(
				tlscfg.MaybeWithDiskCA(
					tlsConfig.CaFile, tlscfg.ForClient),
				tlscfg.MaybeWithDiskKeyPair(
					tlsConfig.ClientCertFile, tlsConfig.ClientKeyFile),
			)
			if err != nil {
				log.Fatalf("Unable to create TLS config: %v", err)
			}
			opts = append(opts, kgo.DialTLSConfig(tc))
		} else {
			opts = append(opts, kgo.DialTLSConfig(new(tls.Config)))
		}
	}
	return opts
}

// Initializes the necessary SASL configuration options
func SASLOpt(config *SASLConfig, opts []kgo.Opt) []kgo.Opt {
	if config.SaslMethod != "" ||
		config.SaslUsername != "" ||
		config.SaslPassword != "" {
		if config.SaslMethod == "" ||
			config.SaslUsername == "" ||
			config.SaslPassword == "" {
			log.Fatalln("All of SaslMethod, SaslUsername, SaslPassword " +
				"must be specified if any are")
		}
		method := strings.ToLower(config.SaslMethod)
		method = strings.ReplaceAll(method, "-", "")
		method = strings.ReplaceAll(method, "_", "")
		switch method {
		case "plain":
			opts = append(opts, kgo.SASL(plain.Auth{
				User: config.SaslUsername,
				Pass: config.SaslPassword,
			}.AsMechanism()))
		case "scramsha256":
			opts = append(opts, kgo.SASL(scram.Auth{
				User: config.SaslUsername,
				Pass: config.SaslPassword,
			}.AsSha256Mechanism()))
		case "scramsha512":
			opts = append(opts, kgo.SASL(scram.Auth{
				User: config.SaslUsername,
				Pass: config.SaslPassword,
			}.AsSha512Mechanism()))
		case "awsmskiam":
			opts = append(opts, kgo.SASL(aws.Auth{
				AccessKey: config.SaslUsername,
				SecretKey: config.SaslPassword,
			}.AsManagedStreamingIAMMechanism()))
		default:
			log.Fatalf("Unrecognized sasl method: %s", method)
		}
	}
	return opts
}

// Set the maximum Kafka protocol version to try
func MaxVersionOpt(version string, opts []kgo.Opt) []kgo.Opt {
	ver := strings.ToLower(version)
	ver = strings.ReplaceAll(ver, "v", "")
	ver = strings.ReplaceAll(ver, ".", "")
	ver = strings.ReplaceAll(ver, "_", "")
	verNum, _ := strconv.Atoi(ver)
	switch verNum {
	case 330:
		opts = append(opts, kgo.MaxVersions(kversion.V3_3_0()))
	case 320:
		opts = append(opts, kgo.MaxVersions(kversion.V3_2_0()))
	case 310:
		opts = append(opts, kgo.MaxVersions(kversion.V3_1_0()))
	case 300:
		opts = append(opts, kgo.MaxVersions(kversion.V3_0_0()))
	case 280:
		opts = append(opts, kgo.MaxVersions(kversion.V2_8_0()))
	case 270:
		opts = append(opts, kgo.MaxVersions(kversion.V2_7_0()))
	case 260:
		opts = append(opts, kgo.MaxVersions(kversion.V2_6_0()))
	default:
		opts = append(opts, kgo.MaxVersions(kversion.Stable()))
	}
	return opts
}
