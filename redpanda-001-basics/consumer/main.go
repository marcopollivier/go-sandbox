package main

import (
	"fmt"
	"github.com/IBM/sarama"
	"sync"
)

var (
	wg    sync.WaitGroup
	topic = "chat-room"
)

func main() {

	consumer, err := sarama.NewConsumer([]string{"localhost:19092"}, nil)
	if err != nil {
		fmt.Println("Error creating consumer:", err)
		return
	}

	partitions, err := consumer.Partitions(topic)
	if err != nil {
		fmt.Println("Error getting list of partitions:", err)
		return
	}

	for partition := range partitions {
		pc, err := consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			fmt.Println("Error starting consumer partition:", partition, err)
			return
		}
		defer pc.AsyncClose()
		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			for msg := range pc.Messages() {
				fmt.Printf("Partition:%d, Offset:%d, Key:%s, Value:%s", msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
				fmt.Println()
			}
		}(pc)
	}
	wg.Wait()
	err = consumer.Close()
	if err != nil {
		fmt.Println("Error closing consumer:", err)
		return
	}
}
