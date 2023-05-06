package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/pkg/errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"
)

var strChan = make(chan int, 256)
var wg sync.WaitGroup
var filePath = flag.String("f", "./url.txt", "")
var skipLine = flag.Int("s", 0, "")

var client *http.Client
var ossClient *oss.Client
var ossBucket *oss.Bucket
var count = 0

func init() {
	client = &http.Client{
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          600,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   5 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}

	ossClient, _ = oss.New("oss-cn-hangzhou-internal.aliyuncs.com", "", "")
	// 填写存储空间名称
	ossBucket, _ = ossClient.Bucket("")
}

// 读取文件的每一行
func main() {

	flag.Parse()

	file, err := os.Open(*filePath)
	if err != nil {
		log.Fatal(err.Error())
		return
	}
	defer file.Close()
	//lineReader := bufio.NewReader(file)
	fmt.Println("start")
	go func() {
		t := time.NewTicker(10 * time.Second)
		for {
			<-t.C
			log.Println("count:", count)
		}
	}()

	// 上传图片
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if count < *skipLine {
			continue
		}
		line := scanner.Text()

		strChan <- 1
		// 添加一个任务到 WaitGroup 中
		wg.Add(1)
		go func() {
			err := downloadImage(line)
			if err != nil {
				log.Fatal("error downloading", err)
			}
			count++
			wg.Done() // 表示一个任务已完成
			<-strChan
		}()
	}
	if err := scanner.Err(); err != nil {
		log.Fatal("error scanning", err)
	}
	wg.Wait() // 等待所有任务都完成
	fmt.Println("All tasks finished")
}

// 下载 oss 图片
func downloadImage(picUrl string) error {
	u, err := url.Parse(picUrl)
	if err != nil {
		return errors.Wrap(err, "url parse")
	}
	if u.Host == "img.alicdn.com" {
		return nil
	}
	// 发送 GET 请求获取文件流
	resp, err := client.Get(picUrl)
	if err != nil {
		return errors.Wrap(err, "http get")
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrap(err, "io readAll")
	}

	// 将文件流上传 oss
	err = ossBucket.PutObject(u.Path[1:], bytes.NewReader(data))
	if err != nil {
		return errors.Wrap(err, "oss bucket")
	}
	return nil
}
