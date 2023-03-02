package main

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"prospect_file_sync/config"
	"prospect_file_sync/database"
)

var targetDB *sqlx.DB
var regionPrefix = "cnpc_"

func SyncFiles(rc config.RegionConfig) {
	// 1. init db
	db, err := database.ConnectDB(rc.DB)
	if err != nil {
		logger.Printf("%s db init error: %s\n", rc.Name, err.Error())
	}
	defer db.Close()

	// 2. queryFileLogsToSync
	fls := make([]FileLog, 0)
	rows, err := queryFileLogsToSync(db, rc.DB.LogTable)
	if err != nil {
		logger.Printf("%s queryFileLogsToSync error：%s\n", rc.Name, err.Error())
	}

	for rows.Next() {
		var fl FileLog
		if err := rows.StructScan(&fl); err != nil {
			logger.Printf("%s StructScan error：%s\n", rc.Name, err.Error())
		}

		fls = append(fls, fl)
	}

	// 3. foreach files
	for _, fl := range fls {
		switch fl.DMLTYPE {
		case "I":
			fmt.Println("IIIIIIIIIII")
			addFile(db, rc, fl)
		case "D":
			fmt.Println("DDDDDDDDDDD")
		case "U":
			fmt.Println("UUUUUUUUUUU")
		default:
			logger.Printf("%s DMLTYPE error：%s is not in ['I','D','U']\n", rc.Name, fl.DMLTYPE)
		}

	}
}

// action I : 同步insert文件和文件表记录
func addFile(db *sqlx.DB, rc config.RegionConfig, fl FileLog) {
	// 查询文件详情
	ft, err := queryFile(db, rc.DB.FileTable, fl)
	if err != nil {
		logger.Printf("%s queryFile error：%s\n", rc.Name, err.Error())
		return
	}

	// 下载文件
	// 源头服务器文件下载地址 == BaseUrl + 截取RootDir之后的剩余path
	restPath := strings.Split(ft.CFLJ, rc.RootDir)[1]
	downloadUrl, _ := url.JoinPath(rc.BaseUrl, strings.ReplaceAll(restPath, "\\", "/"))
	filename := path.Base(strings.ReplaceAll(ft.CFLJ, "\\", "/"))

	// 目标服务器文件落盘地址 == RootDir + cnpc_dq + 井号第一个字 + 井号 + 文件名
	filepath := path.Join(cfg.Target.RootDir, regionPrefix+rc.Name, fl.JH[0:3], fl.JH, filename)
	// filepath := path.Join(cfg.Target.RootDir, filename)

	err = downloadFile(filepath, downloadUrl)
	if err != nil {
		logger.Printf("%s downloadFile error：%s\n", rc.Name, err.Error())
		return
	}

	// 写文件表
	err = insertFileTable(db, ft, cfg.Target.DB.FileTable)
	if err != nil {
		logger.Printf("%s insertFileTable error：%s\n", rc.Name, err.Error())
		return
	}
}

// 查询文件详情 FileTable
func queryFile(db *sqlx.DB, fileTable string, fl FileLog) (FileTable, error) {
	if len(fileTable) == 0 {
		return FileTable{}, errors.New("fileTable is null")
	}

	var ft FileTable
	sql := fmt.Sprintf("SELECT * FROM \"%s\" WHERE DW =:DW and JH =:JH and WDMC =:WDMC", fileTable)
	nstmt, err := db.PrepareNamed(sql)
	err = nstmt.Get(&ft, fl)
	if err != nil {
		return FileTable{}, err
	}

	if err != nil {
		return FileTable{}, err
	}

	return ft, nil
}

// 下载文件落盘
func downloadFile(filepath string, url string) error {
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

// EnsureBaseDir 确保文件所在目录已经创建
func EnsureBaseDir(fpath string) {
	baseDir := path.Dir(fpath)
	_, err := os.Stat(baseDir)
	if err != nil {
		os.MkdirAll(baseDir, 0755)
	}
}

// insert 文件表FileTable
func insertFileTable(db *sqlx.DB, ft FileTable, fileTable string) error {
	sqlStr := fmt.Sprintf(`
		insert into "%s"(DW, JH, WDMC, CFLJ, WDLX, WDZY, SJLB, BXDW, BXRQ, BZ, LRR, LRRQ) 
		values (:DW, :JH, :WDMC, :CFLJ, :WDLX, :WDZY,:SJLB, :BXDW, :BXRQ, :BZ, :LRR, :LRRQ)
	`, fileTable)

	ret, err := db.NamedExec(sqlStr, &ft)
	if err != nil {
		return err
	}
	num, err := ret.RowsAffected()
	if err != nil {
		return err
	}

	fmt.Printf("insert %d line success\n", num)
	return nil
}

// 初始化目标数据库连接
func InitTargetDB(c config.Config) {
	var err error
	targetDB, err = database.ConnectDB(c.Target.DB)
	if err != nil {
		logger.Fatalln("targetDB init error: " + err.Error())
	}
}

// 查询需要同步的文件列表
func queryFileLogsToSync(db *sqlx.DB, logTable string) (*sqlx.Rows, error) {
	if len(logTable) == 0 {
		return nil, errors.New("logTable is null")
	}

	sql := fmt.Sprintf("SELECT * FROM \"%s\"", logTable)
	udb := db.Unsafe()
	rows, err := udb.Queryx(sql)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

type FileLog struct {
	DW       string `db:"DW"`
	JH       string `db:"JH"`
	WDMC     string `db:"WDMC"`
	SEQUENCE string `db:"SEQUENCE$$"`
	DMLTYPE  string `db:"DMLTYPE$$"`
}

type FileTable struct {
	DW   string    `db:"DW"`
	JH   string    `db:"JH"`
	WDMC string    `db:"WDMC"`
	CFLJ string    `db:"CFLJ"`
	WDLX string    `db:"WDLX"`
	WDZY string    `db:"WDZY"`
	SJLB string    `db:"SJLB"`
	BXDW string    `db:"BXDW"`
	BXRQ time.Time `db:"BXRQ"`
	BZ   string    `db:"BZ"`
	LRR  string    `db:"LRR"`
	LRRQ time.Time `db:"LRRQ"`
}
