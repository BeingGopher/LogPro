package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/hpcloud/tail"
)

func main() {
	// 指定日志文件路径
	filePath := "D:\\gitDoc\\LogPro\\readLog\\logfile.log"

	// 检查文件是否存在
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		log.Printf("File does not exist: %s", filePath)
	} else {
		log.Printf("File exists: %s", filePath)
	}

	// 配置 tail
	config := tail.Config{
		Follow:    true,
		ReOpen:    true,
		MustExist: false,
		Poll:      true,
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2},
	}

	// 创建 Tail 对象
	log.Printf("Starting to tail file: %s", filePath)
	tailObj, err := tail.TailFile(filePath, config)
	if err != nil {
		log.Fatalf("无法打开文件：%v", err)
	}
	defer tailObj.Stop()

	// 循环读取日志文件内容
	var (
		msg *tail.Line
		ok  bool
	)
	for {
		msg, ok = <-tailObj.Lines
		if !ok {
			log.Printf("tail file close, waiting to reopen: %s", filePath)
			time.Sleep(1 * time.Second)
			continue
		}
		fmt.Println("message:", msg.Text)
	}
}
