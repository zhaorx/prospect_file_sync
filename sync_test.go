package main

import (
	"fmt"
	"path"
	"testing"
)

func TestPath(t *testing.T) {
	// 返回路径的最后一个元素
	fmt.Println(path.Base("./a/b/c.log"))
	// 如果路径为空字符串，返回.
	fmt.Println(path.Base(""))

}
