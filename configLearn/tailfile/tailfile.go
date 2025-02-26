package tailfile

import (
	"LogPro/configLearn/common"
	"LogPro/configLearn/kafka"
	"github.com/IBM/sarama"
	"github.com/hpcloud/tail"
	"github.com/sirupsen/logrus"
	"strings"
	"time"
)

type tailTask struct {
	path  string
	topic string
	tObj  *tail.Tail
}

var (
	confChan chan []*common.CollectEntry
)

func newTailTask(path string, topic string) *tailTask {
	tt := &tailTask{
		path:  path,
		topic: topic,
	}
	//tt.tObj, err = tail.TailFile(tt.path, config)
	return tt
}

func (t *tailTask) Init() (err error) {
	config := tail.Config{
		Follow:    true,
		ReOpen:    true,
		MustExist: false,
		Poll:      true,
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2},
	}
	t.tObj, err = tail.TailFile(t.path, config)
	return
}

func (t *tailTask) run() { //读取日志，发送kafka
	logrus.Infof("collect for path:%s is running...", t.path)
	var (
		lines *tail.Line
		ok    bool
	)
	for {
		lines, ok = <-t.tObj.Lines
		if !ok {
			logrus.Warn("tail file reopen, path:%s\n", t.path)
			time.Sleep(1 * time.Second)
			continue
		}
		if len(strings.Trim(lines.Text, "\r")) == 0 { //如果是空行，就不需要读，直接跳
			continue
		}
		//利用chan将同步代码改为异步
		//把读出来的一行日志包装成Kafka里的msg类型，放到chan中
		msg := &sarama.ProducerMessage{
			Topic: "t.topic",
			Value: sarama.StringEncoder(lines.Text),
		}
		kafka.MsgChan(msg) //取一行写一行,通过函数暴露，而不是暴露对象

	}
}

func Init(allConf []*common.CollectEntry) (err error) {
	// 配置 tail

	for _, conf := range allConf {
		tt := newTailTask(conf.Path, conf.Topic)
		err = tt.Init()
		if err != nil {
			logrus.Errorf("create tailObj from path:%s failed, err:%v", conf.Path, err)
			continue
		}
		//收集日志
		logrus.Infof("create tailObj from path:%s success.", conf.Path)
		go tt.run()
	}
	confChan = make(chan []*common.CollectEntry) //做阻塞的通道

	newConf := <-confChan //取到值说明新的配置来了
	logrus.Infof("get new conf from etcd, conf:%v\n", newConf)
	return
}

func SendNewConf(newConf []*common.CollectEntry) {
	confChan <- newConf
}
