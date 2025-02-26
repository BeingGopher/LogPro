package kafka

import (
	"fmt"
	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"
	"log"
	"sync"
)

var (
	producer sarama.SyncProducer
	msgChan  chan *sarama.ProducerMessage //这里存放一个指针，相比于直接存放string更省内存
)

func InitPro(address []string, chanSize int) error {
	// 创建 Kafka 生产者配置
	var err error
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll          //ACK
	config.Producer.Partitioner = sarama.NewRandomPartitioner //分区
	config.Producer.Return.Successes = true                   // 回复确认
	config.Producer.Return.Errors = true

	// 创建同步生产者
	producer, err = sarama.NewSyncProducer(address, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	msgChan = make(chan *sarama.ProducerMessage, chanSize)
	//在初始化的步骤里循环发送给Kafka
	go sendMsg()

	return err
}

// 从MsgChan中读取msg，发送给Kafka
func sendMsg() {
	for {
		select {
		case msg := <-msgChan:
			_, _, err := producer.SendMessage(msg)
			if err != nil {
				log.Fatalf("Failed to send message: %v", err)
			}
			logrus.Info("Message sent")
		}
	}
}

func InitCon(address []string) {
	consumer, err := sarama.NewConsumer(address, nil)
	if err != nil {
		fmt.Printf("fail to start consumer, err:%v\n", err)
		return
	}
	// 拿到指定topic下面的所有分区列表
	partitionList, err := consumer.Partitions("web_log") // 根据topic取到所有的分区
	if err != nil {
		fmt.Printf("fail to get list of partition:err%v\n", err)
		return
	}
	fmt.Println(partitionList)
	var wg sync.WaitGroup
	for partition := range partitionList { // 遍历所有的分区
		// 针对每个分区创建一个对应的分区消费者
		pc, err := consumer.ConsumePartition("test", int32(partition), sarama.OffsetNewest)
		if err != nil {
			fmt.Printf("failed to start consumer for partition %d,err:%v\n",
				partition, err)
			return
		}
		defer pc.AsyncClose()
		// 异步从每个分区消费信息
		wg.Add(1)
		go func(sarama.PartitionConsumer) {
			for msg := range pc.Messages() {
				fmt.Printf("Partition:%d Offset:%d Key:%s Value:%s",
					msg.Partition, msg.Offset, msg.Key, msg.Value)
			}
		}(pc)
	}
	wg.Wait()
}

func MsgChan(msg *sarama.ProducerMessage) {
	msgChan <- msg
}
