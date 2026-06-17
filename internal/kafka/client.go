package kafka

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

// ClusterSnapshot is a point-in-time view of the entire Kafka cluster.
type ClusterSnapshot struct {
	Brokers     []BrokerInfo
	Topics      []TopicInfo
	Groups      []GroupInfo
	CollectedAt time.Time
}

// BrokerInfo holds metadata about one broker.
type BrokerInfo struct {
	ID           int32
	Addr         string
	IsController bool
	Rack         string
}

// TopicInfo holds metadata and metrics for one topic.
type TopicInfo struct {
	Name              string
	Partitions        []PartitionInfo
	ReplicationFactor int16
	TotalMessages     int64
	MsgPerSec         float64 // set by EnrichWithThroughput in lag.go
}

// PartitionInfo holds per-partition metadata and offsets.
type PartitionInfo struct {
	ID           int32
	Leader       int32
	Replicas     []int32
	ISR          []int32
	LogEndOffset int64
	OldestOffset int64
}

// GroupInfo holds metadata for one consumer group.
type GroupInfo struct {
	Name     string
	State    string
	Members  int
	TotalLag int64
	Offsets  []GroupPartitionOffset
}

// GroupPartitionOffset is a per-partition offset record for a consumer group.
// Lag == -1 means the group has no committed offset for that partition.
type GroupPartitionOffset struct {
	Topic        string
	Partition    int32
	Committed    int64
	LogEndOffset int64
	Lag          int64
}

// ClusterClient is the interface the poller uses. Nothing outside internal/kafka
// should depend on sarama directly.
type ClusterClient interface {
	FetchSnapshot() (*ClusterSnapshot, error)
	Close() error
}

// NewClient creates a production ClusterClient backed by sarama.
func NewClient(brokers []string) (ClusterClient, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_6_0_0
	cfg.ClientID = "kafkascope"
	cfg.Metadata.RefreshFrequency = 0 // manual refresh only

	client, err := sarama.NewClient(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("sarama client: %w", err)
	}

	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("cluster admin: %w", err)
	}

	return &saramaClusterClient{client: client, admin: admin}, nil
}

type saramaClusterClient struct {
	client sarama.Client
	admin  sarama.ClusterAdmin
}

func (c *saramaClusterClient) Close() error {
	_ = c.admin.Close()
	return c.client.Close()
}

func (c *saramaClusterClient) FetchSnapshot() (*ClusterSnapshot, error) {
	if err := c.client.RefreshMetadata(); err != nil {
		return nil, fmt.Errorf("refresh metadata: %w", err)
	}

	brokers := c.fetchBrokers()

	topics, err := c.fetchTopics()
	if err != nil {
		return nil, fmt.Errorf("fetch topics: %w", err)
	}

	groups, _ := c.fetchGroups(topics) // non-fatal if groups fail

	return &ClusterSnapshot{
		Brokers:     brokers,
		Topics:      topics,
		Groups:      groups,
		CollectedAt: time.Now(),
	}, nil
}

func (c *saramaClusterClient) fetchBrokers() []BrokerInfo {
	brokers := c.client.Brokers()

	controllerID := int32(-1)
	if ctrl, err := c.client.Controller(); err == nil && ctrl != nil {
		controllerID = ctrl.ID()
	}

	result := make([]BrokerInfo, 0, len(brokers))
	for _, b := range brokers {
		result = append(result, BrokerInfo{
			ID:           b.ID(),
			Addr:         b.Addr(),
			IsController: b.ID() == controllerID,
			Rack:         b.Rack(),
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

func (c *saramaClusterClient) fetchTopics() ([]TopicInfo, error) {
	topicList, err := c.admin.ListTopics()
	if err != nil {
		return nil, err
	}

	result := make([]TopicInfo, 0, len(topicList))

	for name, detail := range topicList {
		if strings.HasPrefix(name, "__") {
			continue
		}

		partitions, err := c.client.Partitions(name)
		if err != nil {
			continue
		}

		pInfos := make([]PartitionInfo, 0, len(partitions))
		var totalMessages int64

		for _, pid := range partitions {
			leader, _ := c.client.Leader(name, pid)
			replicas, _ := c.client.Replicas(name, pid)
			isr, _ := c.client.InSyncReplicas(name, pid)
			newest, _ := c.client.GetOffset(name, pid, sarama.OffsetNewest)
			oldest, _ := c.client.GetOffset(name, pid, sarama.OffsetOldest)

			leaderID := int32(-1)
			if leader != nil {
				leaderID = leader.ID()
			}

			pInfos = append(pInfos, PartitionInfo{
				ID:           pid,
				Leader:       leaderID,
				Replicas:     replicas,
				ISR:          isr,
				LogEndOffset: newest,
				OldestOffset: oldest,
			})

			if newest > 0 {
				totalMessages += newest
			}
		}

		result = append(result, TopicInfo{
			Name:              name,
			Partitions:        pInfos,
			ReplicationFactor: detail.ReplicationFactor,
			TotalMessages:     totalMessages,
		})
	}

	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

type topicPartKey struct {
	topic string
	pid   int32
}

func (c *saramaClusterClient) fetchGroups(topics []TopicInfo) ([]GroupInfo, error) {
	groupMap, err := c.admin.ListConsumerGroups()
	if err != nil {
		return nil, err
	}
	if len(groupMap) == 0 {
		return nil, nil
	}

	groupNames := make([]string, 0, len(groupMap))
	for name := range groupMap {
		groupNames = append(groupNames, name)
	}
	sort.Strings(groupNames)

	// Member counts and states from describe.
	stateMap := make(map[string]string, len(groupNames))
	membersMap := make(map[string]int, len(groupNames))
	if descs, err := c.admin.DescribeConsumerGroups(groupNames); err == nil {
		for _, d := range descs {
			stateMap[d.GroupId] = d.State
			membersMap[d.GroupId] = len(d.Members)
		}
	}

	// Build topic/partition → logEnd lookup and the partitions map for OffsetFetch.
	logEnds := make(map[topicPartKey]int64, 64)
	topicPartitions := make(map[string][]int32, len(topics))
	for _, t := range topics {
		pids := make([]int32, 0, len(t.Partitions))
		for _, p := range t.Partitions {
			pids = append(pids, p.ID)
			logEnds[topicPartKey{t.Name, p.ID}] = p.LogEndOffset
		}
		topicPartitions[t.Name] = pids
	}

	result := make([]GroupInfo, 0, len(groupNames))

	for _, name := range groupNames {
		committed, err := c.admin.ListConsumerGroupOffsets(name, topicPartitions)
		if err != nil {
			continue
		}

		var offsets []GroupPartitionOffset
		var totalLag int64

		for topic, partitions := range committed.Blocks {
			for pid, block := range partitions {
				if block.Err != sarama.ErrNoError || block.Offset < 0 {
					continue // skip partitions the group hasn't committed to
				}
				logEnd := logEnds[topicPartKey{topic, pid}]
				lag := ComputeLag(block.Offset, logEnd)
				if lag > 0 {
					totalLag += lag
				}
				offsets = append(offsets, GroupPartitionOffset{
					Topic:        topic,
					Partition:    pid,
					Committed:    block.Offset,
					LogEndOffset: logEnd,
					Lag:          lag,
				})
			}
		}

		sort.Slice(offsets, func(i, j int) bool {
			if offsets[i].Topic != offsets[j].Topic {
				return offsets[i].Topic < offsets[j].Topic
			}
			return offsets[i].Partition < offsets[j].Partition
		})

		result = append(result, GroupInfo{
			Name:     name,
			State:    stateMap[name],
			Members:  membersMap[name],
			TotalLag: totalLag,
			Offsets:  offsets,
		})
	}

	return result, nil
}
