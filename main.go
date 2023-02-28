//
// File: main.go
// Created by Dizzrt on 2023/02/02.
//
// Copyright (C) 2023 The oset Authors.
// This source code is licensed under the MIT license found in
// the LICENSE file in the root directory of this source tree.
//

package main

import (
	oset "oset_server/src"
	"sync"

	"github.com/Dizzrt/etlog"
	"github.com/Dizzrt/etstream/kafka"
	"github.com/spf13/viper"
)

var (
	viperInitOnce sync.Once
	etlogInitOnce sync.Once
)

func init() {
	viperInit()
	etlogInit()
}

func main() {
	if viper.GetBool("server.log.enable") {
		go oset.StartLogServer()
	}

	if viper.GetBool("server.event.enable") {
		go oset.StartEventServer()
	}

	select {}
}

func viperInit() {
	viperInitOnce.Do(func() {
		viper.SetConfigName(".config")
		viper.SetConfigType("toml")
		viper.AddConfigPath(".")

		err := viper.ReadInConfig()
		if err != nil {
			panic("read config failed: " + err.Error())
		}
	})
}

func etlogInit() {
	etlogInitOnce.Do(func() {
		reporterType := viper.GetString("log.reporter_type")
		reportername := viper.GetString("log.reporter_name")
		logFilePath := viper.GetString("log.file_path")
		maxFileSize := viper.GetInt("log.max_file_size")
		maxBackups := viper.GetInt("log.max_backups")
		maxAge := viper.GetInt("log.max_age")
		compress := viper.GetBool("log.is_compress")

		kafkaEnable := viper.GetBool("log.kafka.is_enable")
		kafkaHost := viper.GetString("log.kafka.host")
		kafkaTopic := viper.GetString("log.kafka.topic")

		kconfig := kafka.KafkaConfig{
			SaramaConfig: kafka.DefaultProducerConfig(),
			Host:         kafkaHost,
			Topic:        kafkaTopic,
		}

		lconfig := etlog.LogConfig{
			ReporterType: reporterType,
			ReporterName: reportername,
			FilePath:     logFilePath,
			MaxFileSize:  maxFileSize,
			MaxBackups:   maxBackups,
			MaxAge:       maxAge,
			Compress:     compress,
			KafkaEnable:  kafkaEnable,
			KafkaConfig:  kconfig,
		}

		etlog.NewLogger(lconfig, "server_log")
	})
}
