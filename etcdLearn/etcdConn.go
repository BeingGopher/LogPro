package main

import (
	"context"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)

func main() {
	// 配置 etcd 客户端连接参数
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"}, // 指定 etcd 服务地址
		DialTimeout: 5 * time.Second,            // 连接超时时间
	})
	if err != nil {
		fmt.Printf("连接 etcd 失败：%v\n", err)
		return
	}
	defer client.Close()

	fmt.Println("成功连接到 etcd")

	////写入数据
	ctx := context.Background()
	//str := "[{\"path\":\"D:/gitDoc/LogPro/configLearn/log/logs.log\",\"topic\":\"test\"},{\"path\":\"D:/gitDoc/LogPro/configLearn/log/logs1.log\",\"topic\":\"test1\"}]"
	str := "[{\"path\":\"D:/gitDoc/LogPro/configLearn/log/logs.log\",\"topic\":\"test\"},{\"path\":\"D:/gitDoc/LogPro/configLearn/log/logs1.log\",\"topic\":\"test1\"},{\"path\":\"D:/gitDoc/LogPro/configLearn/log/logs2.log\",\"topic\":\"test2\"},{\"path\":\"D:/gitDoc/LogPro/configLearn/log/logs3.log\",\"topic\":\"test3\"}]"
	_, err = client.Put(ctx, "collect_log_path", str)
	if err != nil {
		fmt.Printf("写入数据失败：%v\n", err)
		return
	}
	fmt.Println("成功写入数据")

	////读取数据
	resp, err := client.Get(ctx, "key1")
	if err != nil {
		fmt.Printf("读取数据失败：%v\n", err)
		return
	}
	for _, ev := range resp.Kvs {
		fmt.Printf("键：%s，值：%s\n", ev.Key, ev.Value)
	}
	//
	////删除数据
	//_, err = client.Delete(ctx, "key1")
	//if err != nil {
	//	fmt.Printf("删除数据失败：%v\n", err)
	//	return
	//}
	//fmt.Println("成功删除数据")

	////监听数据变化
	//watchChan := client.Watch(ctx, "key1")
	//for watchResponse := range watchChan {
	//	for _, event := range watchResponse.Events {
	//		if event.Type == clientv3.EventTypePut {
	//			fmt.Printf("键 %s 被更新为：%s\n", event.Kv.Key, event.Kv.Value)
	//		} else if event.Type == clientv3.EventTypeDelete {
	//			fmt.Printf("键 %s 被删除\n", event.Kv.Key)
	//		}
	//	}
	//}

	////事务
	//	txn := client.Txn(ctx)
	//	resp, err := txn.If(clientv3.Compare(clientv3.Value("key"), "=", "oldValue")).
	//		Then(clientv3.OpPut("key", "newValue")).
	//		Else(clientv3.OpPut("key", "otherValue")).
	//		Commit()
	//	if err != nil {
	//		fmt.Printf("事务失败：%v\n", err)
	//		return
	//	}
	//	fmt.Println("事务执行成功")
}
