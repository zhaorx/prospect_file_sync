package util

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"prospect_file_sync/config"
)

var cfg = config.Cfg

// FetchDataByDate 获取某天的数据
func FetchDataByDate(date string, dataurl string) (data []byte, err error) {
	client := &http.Client{
		Timeout: time.Duration(10) * time.Second,
	}

	dataurl += date
	method := "GET"
	req, err := http.NewRequest(method, dataurl, nil)
	if err != nil {
		fmt.Println(err)
		return
	}

	// set header
	req.Header.Add("host", "agsi.gie.eu")
	req.Header.Add("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9")
	req.Header.Add("sec-ch-ua", "\".Not/A)Brand\";v=\"99\", \"Google Chrome\";v=\"103\", \"Chromium\";v=\"103\"")
	req.Header.Add("sec-ch-ua-mobile", "?0")
	req.Header.Add("sec-ch-ua-platform", "\"Windows\"")
	req.Header.Add("sec-fetch-dest", "document")
	req.Header.Add("sec-fetch-mode", "navigate")
	req.Header.Add("sec-fetch-site", "none")
	req.Header.Add("sec-fetch-user", "?1")
	req.Header.Add("upgrade-insecure-requests", "1")
	req.Header.Add("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.66 Safari/537.36")

	res, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer res.Body.Close()

	data, err = ioutil.ReadAll(res.Body)
	if err != nil {
		fmt.Println(err)
		return
	}

	// fmt.Println(string(data))
	return
}
