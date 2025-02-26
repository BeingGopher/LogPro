package main

import (
	"LogPro/configLearn/etcd"
	"LogPro/configLearn/kafka"
	"LogPro/configLearn/tailfile"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/hpcloud/tail"
	"github.com/sirupsen/logrus"
	"gopkg.in/ini.v1"
	"strings"
	"time"
)

// 收集指定目录下的日志文件，发送到Kafka

// 整个logagent的配置结构体
type Config struct {
	KafkaConfig   `ini:"kafka"`
	CollectConfig `ini:"collect"`
	EtcdConfig    `ini:"etcd"`
}

type KafkaConfig struct {
	Address  string `ini:"address"`
	Topic    string `ini:"topic"`
	ChanSize int    `ini:"chanSize"`
}

type CollectConfig struct {
	LogFilePath string `ini:"logFilePath"`
}

type EtcdConfig struct {
	Address    string `ini:"address"`
	CollectKey string `ini:"collectKey"`
}

//连接Kafka
//连接日志（tail）
//向Kafka发送日志

// 业务逻辑
func run() (err error) {
	var (
		lines *tail.Line
		ok    bool
	)
	for {
		lines, ok = <-tailfile.TailObj.Lines
		if !ok {
			logrus.Warn("tail file reopen, filename:%s\n", tailfile.TailObj.Filename)
			time.Sleep(1 * time.Second)
			continue
		}
		if len(strings.Trim(lines.Text, "\r")) == 0 { //如果是空行，就不需要读，直接跳
			continue
		}
		//利用chan将同步代码改为异步
		//把读出来的一行日志包装成Kafka里的msg类型，放到chan中
		msg := &sarama.ProducerMessage{
			Topic: "key1",
			Value: sarama.StringEncoder(lines.Text),
		}
		kafka.MsgChan(msg) //取一行写一行,通过函数暴露，而不是暴露对象

	}
}

func main() {
	//加载配置文件
	var configObj = new(Config)
	err := ini.MapTo(configObj, "D:\\gitDoc\\LogPro\\configLearn\\conf\\config.ini")
	if err != nil {
		logrus.Errorf("load config failed,err:%v", err)
		return
	}
	fmt.Printf("%#v\n", configObj)

	err = kafka.InitPro([]string{configObj.KafkaConfig.Address}, configObj.KafkaConfig.ChanSize)
	if err != nil {
		logrus.Errorf("init kafka failed,err:%v", err)
		return
	}
	logrus.Info("init kafka success")

	//从etcd中拉取要收集的日志的配置项
	err = etcd.Init([]string{configObj.EtcdConfig.Address})
	if err != nil {
		logrus.Errorf("init etcd failed,err:%v", err)
	}

	allConf, err := etcd.GetConf(configObj.EtcdConfig.CollectKey)
	if err != nil {
		logrus.Errorf("get conf from etcd failed,err:%v", err)
	}
	err = tailfile.Init(allConf)

	//加载tail
	err = tailfile.Init(allConf) //把从etcd中加载的配置项都传进去
	if err != nil {
		logrus.Errorf("init tail failed,err:%v", err)
		return
	}
	logrus.Info("init tail success")
	//从TailObj中取出日志往Kafka发送，
	//TailObj --> log --> Producer --> kafka
	err = run()
	if err != nil {
		logrus.Errorf("run failed,err:%v", err)
		return
	}
	fmt.Println(allConf)
}
