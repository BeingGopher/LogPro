package tailfile

import (
	"LogPro/configLearn/common"
	"github.com/sirupsen/logrus"
)

type tailTaskManage struct {
	tailTaskMap      map[string]*tailTask
	collectEntryList []*common.CollectEntry
	confChan         chan []common.CollectEntry
}

var (
	ttMgr *tailTaskManage
)

func Init(allConf []*common.CollectEntry) (err error) {
	// 配置 tail

	ttMgr = &tailTaskManage{
		tailTaskMap:      make(map[string]*tailTask, 20),         //所有tailTask任务
		confChan:         make(chan []common.CollectEntry, 1024), //等待新配置的通道
		collectEntryList: allConf,                                //所有配置项
	}

	for _, conf := range allConf {
		tt := newTailTask(conf.Path, conf.Topic)
		err = tt.Init()
		if err != nil {
			logrus.Errorf("create tailObj from path:%s failed, err:%v", conf.Path, err)
			continue
		}
		//收集日志
		logrus.Infof("create tailObj from path:%s success.", conf.Path)

		ttMgr.tailTaskMap[tt.path] = tt //把创建的tailTask任务保存起来，方便后续管理
		go tt.run()
	}

	go ttMgr.watch() //在后台等新的配置来

	return
}

func (t *tailTaskManage) watch() {
	newConf := <-t.confChan //取到值说明新的配置来了
	logrus.Infof("get new conf from etcd, conf:%v\n", newConf)

	//原来有，现在没有的要停掉

	for _, conf := range newConf {
		//原来有的，就不用动
		if t.isExist(conf) {
			continue
		}
		//原来没有的新创建一个tailTask任务
		tt := newTailTask(conf.Path, conf.Topic)
		err := tt.Init()
		if err != nil {
			logrus.Errorf("create a new tailTask from path:%s failed, err:%v", conf.Path, err)
		}
		logrus.Infof("create a new tailTask from path:%s succecc", conf.Path)
		t.tailTaskMap[tt.path] = tt
		go tt.run()
	}
	//原来有，现在没有的要停掉
	//找出tailTaskMap中存在，但是newConf不存在的那些tailTask，全部停掉
	for key, task := range t.tailTaskMap {
		var found bool
		for _, conf := range newConf {
			if key == conf.Path {
				found = true
				break
			}
		}
		if !found {
			logrus.Infof("the task collect path:%s need to stop", task.path)
			delete(t.tailTaskMap, key) //从管理类中删除
			task.cancel()              //停掉goroutine，利用上下文的cancel方法。goroutine结束的标志就是函数结束
		}
	}
}

func SendNewConf(newConf []common.CollectEntry) {
	ttMgr.confChan <- newConf
}

func (t *tailTaskManage) isExist(conf common.CollectEntry) bool {
	_, ok := t.tailTaskMap[conf.Path]
	return ok
}
