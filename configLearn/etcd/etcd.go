package etcd

import (
	"LogPro/configLearn/common"
	"LogPro/configLearn/tailfile"
	"context"
	"encoding/json"
	"fmt"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)

var (
	//err error
	cli *clientv3.Client
)

func Init(address []string) (err error) {
	cli, err = clientv3.New(clientv3.Config{
		Endpoints:   address,
		DialTimeout: 5 * time.Second,
	})

	if err != nil {
		fmt.Printf("connect etcd failed, err:%v\n", err)
		return
	}
	return
}

func GetConf(key string) (collectEntryList []*common.CollectEntry, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	resp, err := cli.Get(ctx, key)
	if err != nil {
		logrus.Errorf("get conf from etcd by key:%s failed, err:%v\n", key, err)
		return
	}
	if len(resp.Kvs) == 0 {
		logrus.Warningf("get conf from etcd by key:%s failed, key is empty\n", key)
	}
	value := resp.Kvs[0]
	err = json.Unmarshal(value.Value, &collectEntryList)
	if err != nil {
		logrus.Errorf("json unmarshal failed, err:%v\n", err)
		return
	}
	return
}

func WatchConf(key string) {
	var newConf []common.CollectEntry
	watchChan := cli.Watch(context.Background(), key)
	for watchResponse := range watchChan {
		for _, event := range watchResponse.Events {
			fmt.Printf("type:%s key:%s event:%#v\n", event.Type, event.Kv.Key, event.Kv.Value)
			err := json.Unmarshal(event.Kv.Value, &newConf)
			if err != nil {
				logrus.Errorf("json unmarshal failed, err:%v\n", err)
				continue
			}
			tailfile.SendNewConf(newConf)
		}
	}
}
