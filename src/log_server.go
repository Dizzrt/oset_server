//
// File: log_server.go
// Created by Dizzrt on 2023/02/03.
//
// Copyright (C) 2023 The oset Authors.
// This source code is licensed under the MIT license found in
// the LICENSE file in the root directory of this source tree.
//

package oset

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/Dizzrt/etlog"
	"github.com/Shopify/sarama"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

func StartLogServer() {
	consumer, err := sarama.NewConsumer([]string{viper.GetString("server.log.kafka_host")}, nil)
	if err != nil {
		panic("unable to start log server, because create kafka consumer failed")
	}

	partitionList, err := consumer.Partitions("log")
	if err != nil {
		panic("unable to start log server, because get kafka partitions failed")
	}

	for partition := range partitionList {
		pconsumer, err := consumer.ConsumePartition("log", int32(partition), sarama.OffsetNewest)
		if err != nil {
			panic("unable to start log server, because create partition consumer failed")
		}

		defer pconsumer.AsyncClose()

		go func(sarama.PartitionConsumer) {
			for msg := range pconsumer.Messages() {
				go logProcessor(msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
			}
		}(pconsumer)
	}

	defer consumer.Close()
	select {}
}

func logProcessor(partition int32, offset int64, key string, msg string) {
	for i := 0; i < 1; i += 1 {
		l, err := etlog.Stash(msg)
		if err != nil {
			etlog.L().Warn("unable to process the log, because parse the log failed", zap.Int32("partition", partition), zap.Int64("offset", offset), zap.String("key", key), zap.String("raw_log", msg), zap.Error(err))
			break
		}

		escfg := elasticsearch.Config{
			Addresses: []string{viper.GetString("server.log.es_host")},
		}

		es, err := elasticsearch.NewClient(escfg)
		if err != nil {
			etlog.L().Warn("unable to create elasticsearch client", zap.Int32("partition", partition), zap.Int64("offset", offset), zap.String("key", key), zap.String("raw_log", msg), zap.Error(err))
			break
		}

		data, err := json.Marshal(l)
		if err != nil {
			etlog.L().Warn("unable to convert log to json", zap.Int32("partition", partition), zap.Int64("offset", offset), zap.String("key", key), zap.String("raw_log", msg), zap.Error(err))
			break
		}

		req := esapi.IndexRequest{
			Index:   "log",
			Body:    bytes.NewReader(data),
			Refresh: "true",
		}

		res, err := req.Do(context.Background(), es)
		if err != nil {
			etlog.L().Warn("esapi request failed", zap.Int32("partition", partition), zap.Int64("offset", offset), zap.String("key", key), zap.String("raw_log", msg), zap.Error(err))
			break
		}

		fmt.Println(res)
	}
}
