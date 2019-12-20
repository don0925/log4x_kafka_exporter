package main

import (
	"encoding/json"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	kazoo "github.com/krallistic/kazoo-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	plog "github.com/prometheus/common/log"
	"github.com/prometheus/common/version"
	"github.com/rcrowley/go-metrics"
	"github.com/samuel/go-zookeeper/zk"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	namespace = "log4x_kafka"
	clientID  = "log4x_kafka_exporter"
)

var (
	// 主题某个分区的消息偏移量
	topicCurrentOffset *prometheus.Desc

	// 消费组消费某个主题的消息偏移量
	consumergroupCurrentOffset *prometheus.Desc

	// 消费组消息积压量
	consumergroupLag *prometheus.Desc
)

// KafkaOpts kafka配置
type KafkaOpts struct {
	kafkaZookeeperURI       string
	consumerZookeeperURI    []string
	consumerZookeeperRoot   string
	kafkaVersion            string
	metadataRefreshInterval string
	labels                  string
}

// Exporter kafka监控状态
type Exporter struct {
	kafkaClient             sarama.Client
	topicFilter             *regexp.Regexp
	zookeeperClient         *zk.Conn
	consumerZookeeperRoot   string
	nextMetadataRefresh     time.Time
	metadataRefreshInterval time.Duration
	mu                      sync.Mutex
}

// NewExporter 通过kafka配置新建一个
func NewExporter(opts KafkaOpts, topicFilter string) (*Exporter, error) {
	kafkaZookeeperClient, err := kazoo.NewKazooFromConnectionString(opts.kafkaZookeeperURI, nil)
	if err != nil {
		plog.Errorln("连接kafka zookeeper失败，请检查kafka zookeeper配置.")
		panic(err)
	}

	interval, err := time.ParseDuration(opts.metadataRefreshInterval)
	if err != nil {
		plog.Warnln("无法解析监控数据周期时间，使用默认值5s.")
		interval = 5 * time.Second
	}

	plog.Infoln("kafka zookeeper 客户端完成初始化.")

	config := sarama.NewConfig()
	config.ClientID = clientID
	kafkaVersion, err := sarama.ParseKafkaVersion(opts.kafkaVersion)
	if err != nil {
		plog.Errorln("kafka版本不支持. %s", opts.kafkaVersion)
		return nil, err
	}
	config.Version = kafkaVersion

	brokers, err := kafkaZookeeperClient.Brokers()
	if err != nil {
		plog.Errorln("从zookeeper上未发现kafka broker, 请检查配置.")
		return nil, err
	}

	addrs := make([]string, 0, len(brokers))
	for _, addr := range brokers {
		addrs = append(addrs, addr)
	}

	kafkaClient, err := sarama.NewClient(addrs, config)
	if err != nil {
		plog.Errorln("初始化kafka客户端失败.")
		panic(err)
	}

	plog.Infoln("kafka 客户端完成初始化.")

	zookeeperClient, _, err := zk.Connect(opts.consumerZookeeperURI, 5*time.Second)
	if err != nil {
		plog.Errorln("连接consumer zookeeper失败，请检查kafka zookeeper配置.")
		panic(err)
	}

	plog.Errorln("客户端初始完成.")

	return &Exporter{
		kafkaClient:             kafkaClient,
		zookeeperClient:         zookeeperClient,
		consumerZookeeperRoot:   opts.consumerZookeeperRoot,
		topicFilter:             regexp.MustCompile(topicFilter),
		nextMetadataRefresh:     time.Now(),
		metadataRefreshInterval: interval,
	}, nil
}

// Describe 描述kafka的监控统计数据metrics，实现prometheus的Describe接口
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- topicCurrentOffset
	ch <- consumergroupCurrentOffset
	ch <- consumergroupLag
}

// Collect 采集kafka的metrics，实现prometheus的Collect接口
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	var wg = sync.WaitGroup{}

	offset := make(map[string]map[int32]int64)

	now := time.Now()

	if now.After(e.nextMetadataRefresh) {
		plog.Info("Refreshing client metadata")
		if err := e.kafkaClient.RefreshMetadata(); err != nil {
			plog.Errorf("Cannot refresh topics, using cached data: %v", err)
		}
		e.nextMetadataRefresh = now.Add(e.metadataRefreshInterval)
	}

	// 获取topic
	topics, err := e.kafkaClient.Topics()
	if err != nil {
		plog.Errorf("Cannot get topics: %v", err)
		return
	}

	//

	getMetrics := func(topic string) {
		defer wg.Done()

		if e.topicFilter.MatchString(topic) {
			// 获取分区
			partitions, err := e.kafkaClient.Partitions(topic)
			if err != nil {
				plog.Errorf("Cannot get partitions of topic %s: %v", topic, err)
				return
			}

			e.mu.Lock()
			offset[topic] = make(map[int32]int64, len(partitions))
			e.mu.Unlock()

			// 消费组基础目录
			basepath := e.consumerZookeeperRoot + "/" + topic
			exists, _, err := e.zookeeperClient.Exists(basepath)
			if err != nil || !exists {
				return
			}

			// 获取消费组
			consumerGroupNames, _, err := e.zookeeperClient.Children(basepath)
			if err != nil {
				plog.Errorf("Cannot get consumer group of topic %s %v", topic, err)
				return
			}

			for _, partition := range partitions {
				currentOffset, err := e.kafkaClient.GetOffset(topic, partition, sarama.OffsetNewest)
				if err != nil {
					plog.Errorf("Cannot get current offset of topic %s partition %d: %v", topic, partition, err)
				} else {
					e.mu.Lock()
					offset[topic][partition] = currentOffset
					e.mu.Unlock()

					// producer 偏移量
					ch <- prometheus.MustNewConstMetric(topicCurrentOffset, prometheus.GaugeValue, float64(currentOffset), topic, strconv.FormatInt(int64(partition), 10))
				}

				for _, consumerGroup := range consumerGroupNames {
					data, _, err := e.zookeeperClient.Get(basepath + "/" + consumerGroup + "/partition_" + strconv.FormatInt(int64(partition), 10))
					if err != nil {
						plog.Errorf("Error for  consumer group %d :%v", consumerGroup, err.Error())
						continue
					}
					var offsetData map[string]interface{}
					// 不做错误处理
					json.Unmarshal(data, &offsetData)

					consumerOffset := offsetData["offset"].(float64)

					// consumer 偏移量
					ch <- prometheus.MustNewConstMetric(consumergroupCurrentOffset, prometheus.GaugeValue, offsetData["offset"].(float64), consumerGroup, topic, strconv.FormatInt(int64(partition), 10))

					lag := currentOffset - int64(consumerOffset)
					// lag 消息积压
					ch <- prometheus.MustNewConstMetric(consumergroupLag, prometheus.GaugeValue, float64(lag), consumerGroup, topic, strconv.FormatInt(int64(partition), 10))
				}
			}
		}
	}

	for _, topic := range topics {
		wg.Add(1)
		go getMetrics(topic)
	}

	wg.Wait()

}

func init() {
	metrics.UseNilMetrics = true
	prometheus.MustRegister(version.NewCollector("log4x_kafka_exporter"))
}

func main() {
	var (
		listenAddress = kingpin.Flag("web.listen-address", "Address to listen on for web interface and telemetry.").Default(":9108").String()
		metricsPath   = kingpin.Flag("web.telemetry-path", "Path under which to expose metrics.").Default("/metrics").String()
		topicFilter   = kingpin.Flag("topic.filter", "Regex that determines which topics to collect.").Default(".*").String()

		opts = KafkaOpts{}
	)

	kingpin.Flag("kafka.zookeeper.server", "Address (hosts) of kafka zookeeper server.").Default("localhost:2181/kafka").StringVar(&opts.kafkaZookeeperURI)
	kingpin.Flag("kafka.version", "Kafka broker version").Default(sarama.V0_10_2_1.String()).StringVar(&opts.kafkaVersion)
	kingpin.Flag("refresh.metadata", "Metadata refresh interval").Default("30s").StringVar(&opts.metadataRefreshInterval)
	kingpin.Flag("consumer.zookeeper.server", "Address (hosts) of consumser zookeeper server.").Default("localhost:2181").StringsVar(&opts.consumerZookeeperURI)
	kingpin.Flag("consumer.zookeeper.root", "log4x consumser zookeeper root path.").Default("/log4x/consumerbigdata").StringVar(&opts.consumerZookeeperRoot)
	plog.AddFlags(kingpin.CommandLine)
	kingpin.Version(version.Print("log4x_kafka_exporter"))
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()

	plog.Infoln("Starting log4x_kafka_exporter", version.Info())
	plog.Infoln("Build context", version.BuildContext())

	labels := make(map[string]string)

	if opts.labels != "" {
		for _, label := range strings.Split(opts.labels, ",") {
			splitted := strings.Split(label, "=")
			if len(splitted) >= 2 {
				labels[splitted[0]] = splitted[1]
			}
		}
	}

	topicCurrentOffset = prometheus.NewDesc(prometheus.BuildFQName(namespace, "topic", "partition_current_offset"), "Current Offset of a Broker at Topic/Partition", []string{"topic", "partition"}, labels)

	consumergroupCurrentOffset = prometheus.NewDesc(prometheus.BuildFQName(namespace, "consumergroup", "current_offset"), "Current Offset of a ConsumerGroup at Topic/Partition", []string{"consumergroup", "topic", "partition"}, labels)

	consumergroupLag = prometheus.NewDesc(prometheus.BuildFQName(namespace, "consumergroup", "lag"), "Current Approximate Lag of a ConsumerGroup at Topic/Partition", []string{"consumergroup", "topic", "partition"}, labels)

	exporter, err := NewExporter(opts, *topicFilter)
	if err != nil {
		plog.Fatalln(err)
	}
	defer exporter.kafkaClient.Close()
	defer exporter.zookeeperClient.Close()

	prometheus.MustRegister(exporter)

	http.Handle(*metricsPath, promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html><head><title>Kafka Exporter</title></head><body><h1>Kafka Exporter</h1><p><a href='` + *metricsPath + `'>Metrics</a></p></body></html>`))
	})

	plog.Infoln("Listening on", *listenAddress)
	plog.Fatal(http.ListenAndServe(*listenAddress, nil))
}
