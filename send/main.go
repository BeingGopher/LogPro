package main

import (
	"fmt"
	"github.com/IBM/sarama"
	"log"
)

func main() {
	// 创建 Kafka 生产者配置
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll          //ACK
	config.Producer.Partitioner = sarama.NewRandomPartitioner //分区
	config.Producer.Return.Successes = true                   // 回复确认
	config.Producer.Return.Errors = true

	// 创建同步生产者
	producer, err := sarama.NewSyncProducer([]string{"192.168.204.128:9092"}, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// 发送消息
	msg := &sarama.ProducerMessage{
		Topic: "test",
		Value: sarama.StringEncoder("Hello, Kafka!"),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}

	fmt.Printf("Message is stored in partition %d at offset %d\n", partition, offset)
}
