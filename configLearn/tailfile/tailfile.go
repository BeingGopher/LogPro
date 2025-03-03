package tailfile

import (
	"LogPro/configLearn/common"
	"LogPro/configLearn/kafka"
	"context"
	"github.com/IBM/sarama"
	"github.com/hpcloud/tail"
	"github.com/sirupsen/logrus"
	"strings"
	"time"
)

type tailTask struct {
	path   string
	topic  string
	tObj   *tail.Tail
	ctx    context.Context
	cancel context.CancelFunc
}

var (
	confChan chan []*common.CollectEntry
)

func newTailTask(path string, topic string) *tailTask {
	ctx, cancel := context.WithCancel(context.Background())
	tt := &tailTask{
		path:   path,
		topic:  topic,
		ctx:    ctx,
		cancel: cancel,
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
		select {
		case <-t.ctx.Done(): //只要调用cancel方法，就会收到信号
			logrus.Infof("path:%s is stop...", t.path)
			//t.instance.Cleanup()//清理掉不需要监听的日志的对象
			return
		case lines, ok = <-t.tObj.Lines:
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
}
