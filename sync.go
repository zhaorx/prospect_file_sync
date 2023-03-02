package main

import (
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"prospect_file_sync/config"
	"prospect_file_sync/database"
	"prospect_file_sync/util"
)

var targetDB *sqlx.DB
var regionPrefix = "cnpc_"

func SyncFiles(rc config.RegionConfig) {
	logger.Printf("%s start sync files...\n", rc.Name)

	// 1. init origin db connection
	originDB, err := database.ConnectDB(rc.DB)
	if err != nil {
		logger.Printf("%s originDB init error: %s\n", rc.Name, err.Error())
	}
	defer originDB.Close()

	// 2. queryFileLogsToSync
	fls := make([]FileLog, 0)
	rows, err := queryFileLogsToSync(originDB, rc.DB.LogTable)
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
			addFile(originDB, rc, fl)
		case "D":
			fmt.Println("DDDDDDDDDDD")
		case "U":
			fmt.Println("UUUUUUUUUUU")
		default:
			logger.Printf("%s DMLTYPE error：%s is not in ['I','D','U']\n", rc.Name, fl.DMLTYPE)
		}
	}

	logger.Printf("%s sync files end\n", rc.Name)
}

// 查询需要同步的文件列表
func queryFileLogsToSync(db *sqlx.DB, logTableName string) (*sqlx.Rows, error) {
	if len(logTableName) == 0 {
		return nil, errors.New("logTableName is null")
	}

	sql := fmt.Sprintf("SELECT * FROM \"%s\"", logTableName)
	udb := db.Unsafe()
	rows, err := udb.Queryx(sql)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

// action I : 同步insert文件和文件表记录 并删除log记录
func addFile(originDB *sqlx.DB, rc config.RegionConfig, fl FileLog) {
	originLogTableName := rc.DB.LogTable
	originTableName := rc.DB.FileTable
	targetTableName := cfg.Target.DB.FileTable

	// 1. 查询文件详情
	ft, err := queryFile(originDB, originTableName, fl)
	if err != nil {
		logger.Printf("%s queryFile error：%s\n", rc.Name, err.Error())
		return
	}

	// 2. 下载文件
	// 源头服务器文件下载地址 == BaseUrl + 截取RootDir之后的剩余path
	restPath := strings.Split(ft.CFLJ, rc.RootDir)[1]
	downloadUrl, _ := url.JoinPath(rc.BaseUrl, strings.ReplaceAll(restPath, "\\", "/"))
	filename := path.Base(strings.ReplaceAll(ft.CFLJ, "\\", "/"))

	// 目标服务器文件落盘地址 == RootDir + cnpc_dq + 井号第一个字 + 井号 + 文件名
	filepath := path.Join(cfg.Target.RootDir, regionPrefix+rc.Name, fl.JH[0:3], fl.JH, filename)

	err = util.DownloadFile(filepath, downloadUrl)
	if err != nil {
		logger.Printf("%s downloadFile error：%s\n", rc.Name, err.Error())
		return
	}

	// 3. 写目标库文件表
	err = insertFileTable(targetDB, ft, targetTableName)
	if err != nil {
		logger.Printf("%s insertFileTable error：%s\n", rc.Name, err.Error())

		// 删除刚落盘的文件
		err = util.DeleteFile(filepath)
		if err != nil {
			logger.Printf("%s DeleteFile error：%s\n", rc.Name, err.Error())
		}

		return
	}

	// 4. 删源头库log表
	err = deleteLogRecord(originDB, fl, originLogTableName)
	if err != nil {
		logger.Printf("%s deleteLogRecord error：%s\n", rc.Name, err.Error())

		// 删除刚落盘的文件
		err = util.DeleteFile(filepath)
		if err != nil {
			logger.Printf("%s DeleteFile error：%s\n", rc.Name, err.Error())
		}

		// 删除目标库刚insert的记录
		err = deleteFileRecord(targetDB, fl, targetTableName)
		if err != nil {
			logger.Printf("%s deleteFileRecord error：%s\n", rc.Name, err.Error())
		}

		return
	}
}

// 查询文件详情 FileTable
func queryFile(db *sqlx.DB, fileTableName string, fl FileLog) (FileTable, error) {
	if len(fileTableName) == 0 {
		return FileTable{}, errors.New("fileTableName is null")
	}

	var ft FileTable
	sql := fmt.Sprintf("SELECT * FROM \"%s\" WHERE DW =:DW and JH =:JH and WDMC =:WDMC", fileTableName)
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

// insert 文件表FileTable
func insertFileTable(db *sqlx.DB, ft FileTable, fileTableName string) error {
	sqlStr := fmt.Sprintf(`
		insert into "%s"(DW, JH, WDMC, CFLJ, WDLX, WDZY, SJLB, BXDW, BXRQ, BZ, LRR, LRRQ) 
		values (:DW, :JH, :WDMC, :CFLJ, :WDLX, :WDZY,:SJLB, :BXDW, :BXRQ, :BZ, :LRR, :LRRQ)
	`, fileTableName)

	_, err := db.NamedExec(sqlStr, &ft)
	if err != nil {
		return err
	}

	fmt.Printf("insert table[%s] file[%s] success\n", fileTableName, ft.WDMC)
	return nil
}

// delete 文件表FileTable
func deleteLogRecord(db *sqlx.DB, fl FileLog, tableName string) error {
	sql := fmt.Sprintf("DELETE FROM  \"%s\" WHERE SEQUENCE$$ = %s", tableName, fl.SEQUENCE)
	_, err := db.Exec(sql)
	if err != nil {
		return err
	}

	fmt.Printf("delete table[%s] file[%s] success\n", tableName, fl.WDMC)
	return nil
}

// delete 文件表FileTable
func deleteFileRecord(db *sqlx.DB, fl FileLog, tableName string) error {
	sql := fmt.Sprintf("DELETE FROM  \"%s\" WHERE DW =:DW and JH =:JH and WDMC =:WDMC", tableName)
	nstmt, err := db.PrepareNamed(sql)
	_, err = nstmt.Exec(fl)
	if err != nil {
		return err
	}

	fmt.Printf("delete table[%s] file[%s] success\n", tableName, fl.WDMC)
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
