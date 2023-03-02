package util

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
)

// DownloadFile 下载文件落盘
func DownloadFile(filepath string, url string) error {
	// Create the file
	EnsureBaseDir(filepath)
	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()

	// Get the data
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return errors.New(fmt.Sprintf("文件下载失败(code[%d])，请检查用户密码是否正确", resp.StatusCode))
	}
	defer resp.Body.Close()

	// Write the body to file
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}

	return nil
}

// DeleteFile 删除已落盘的文件
func DeleteFile(filepath string) error {
	err := os.Remove(filepath)
	if err != nil {
		return err
	}

	return nil
}

// EnsureBaseDir 确保文件所在目录已经创建
func EnsureBaseDir(fpath string) {
	baseDir := path.Dir(fpath)
	_, err := os.Stat(baseDir)
	if err != nil {
		os.MkdirAll(baseDir, 0755)
	}
}
