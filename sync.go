package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
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
var xjToken = ""

func SyncFiles(rc config.RegionConfig) {
	logger.Printf("------------------ %s sync files.start ------------------\r\n", rc.Name)

	// 1. init origin db connection
	originDB, err := database.ConnectDB(rc.DB)
	if err != nil {
		logger.Printf("%s originDB init error: %s\r\n", rc.Name, err.Error())
		return
	}
	defer originDB.Close()

	// 如果是xj油田先登录获取token
	xjToken, err = loginXj(rc)
	if err != nil {
		logger.Printf("%s loginXj error:%s\r\n", rc.Name, err.Error())
		return
	}

	// 2. queryFileLogsToSync
	fls := make([]FileLog, 0)
	rows, err := queryFileLogsToSync(originDB, rc.DB.LogTable)
	if err != nil {
		logger.Printf("%s queryFileLogsToSync error:%s\r\n", rc.Name, err.Error())
		return
	}

	for rows.Next() {
		var fl FileLog
		if err := rows.StructScan(&fl); err != nil {
			logger.Printf("%s StructScan error:%s\r\n", rc.Name, err.Error())
		}

		fls = append(fls, fl)
	}
	rows.Close()

	// 3. foreach files
	logger.Printf("%d logs to sync\n", len(fls))
	for _, fl := range fls {
		logger.Printf("****** %s %s[%s] %s-%s sync ******\r\n", rc.Name, fl.SEQUENCE, fl.DMLTYPE, fl.JH, fl.WDMC)

		switch fl.DMLTYPE {
		case "I":
			addFile(originDB, rc, fl)
		case "D":
			deleteFile(originDB, rc, fl)
		case "U":
			updateFile(originDB, rc, fl)
		default:
			logger.Printf("%s DMLTYPE error:%s is not in ['I','D','U']\r\n", rc.Name, fl.DMLTYPE)
		}

		logger.Printf("****** sync end ******\r\n")
	}

	logger.Printf("------------------ %s sync files end ------------------\r\n", rc.Name)
}

// 查询需要同步的文件列表
func queryFileLogsToSync(db *sqlx.DB, logTableName string) (*sqlx.Rows, error) {
	if len(logTableName) == 0 {
		return nil, errors.New("logTableName is null")
	}

	sql := fmt.Sprintf("SELECT * FROM \"%s\" ORDER BY SEQUENCE$$", logTableName)
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

	// 1. 查询源头库文件详情
	ft, err := queryFile(originDB, originTableName, fl)
	if err != nil {
		logger.Printf("%s queryFile[addFile] error:%s\r\n", rc.Name, err.Error())
		return
	}

	// 2. 下载文件
	// downloadUrl := getFileDownloadUrl(ft, rc) // 源服务器文件下载地址
	// storePath := getFileStorePath(ft, rc, fl) // 目标服务器文件落盘地址
	// err = util.DownloadFile(storePath, downloadUrl)
	downloadUrl, storePath, err := downloadFile(ft, rc, fl)
	if err != nil {
		logger.Printf("%s downloadFile[addFile] error:%s\r\n", rc.Name, err.Error())
		return
	} else {
		logger.Printf("%s downloadFile[addFile]:%s\r\n", rc.Name, downloadUrl)
	}

	// 3. 写目标库FileTable表
	count, err := queryCount(targetDB, targetTableName, fl)
	if err != nil {
		logger.Printf("%s queryCount[addFile] error:%s\r\n", rc.Name, err.Error())
	}
	if count > 0 { // 删除目标库重复的旧记录
		err = deleteFileRecord(targetDB, fl, targetTableName)
		if err != nil {
			logger.Printf("%s deleteFileRecord[addFile] error:%s\r\n", rc.Name, err.Error())
			return
		}
	}
	// insert file table
	ft.CFLJ = getFileFTPPath(storePath) // 修改target库存储的文件路径 2.3使用FTP地址供勘探系统内页面使用
	err = insertFileRecord(targetDB, ft, targetTableName)
	if err != nil {
		logger.Printf("%s insertFileRecord[addFile] error:%s\r\n", rc.Name, err.Error())

		// 删除刚落盘的文件
		err = util.DeleteFile(storePath)
		if err != nil {
			logger.Printf("%s DeleteFile[addFile] error:%s\r\n", rc.Name, err.Error())
		}
		return
	}

	// 4. 删源头库log表
	err = deleteLogRecord(originDB, fl, originLogTableName)
	if err != nil {
		logger.Printf("%s deleteLogRecord[addFile] error:%s\r\n", rc.Name, err.Error())

		// 删除刚落盘的文件
		err = util.DeleteFile(storePath)
		if err != nil {
			logger.Printf("%s DeleteFile[addFile] error:%s\r\n", rc.Name, err.Error())
		}

		// 删除目标库刚insert的记录
		err = deleteFileRecord(targetDB, fl, targetTableName)
		if err != nil {
			logger.Printf("%s deleteFileRecord[addFile] error:%s\r\n", rc.Name, err.Error())
		}

		return
	}
}

// action U : 同步update文件和文件表记录 并删除log记录
func updateFile(originDB *sqlx.DB, rc config.RegionConfig, fl FileLog) {
	originLogTableName := rc.DB.LogTable
	originTableName := rc.DB.FileTable
	targetTableName := cfg.Target.DB.FileTable

	// 1. 查询目标库文件详情
	ftt, err := queryFile(targetDB, targetTableName, fl)
	if err != nil {
		logger.Printf("%s queryFile[deleteFile] error:%s\r\n", rc.Name, err.Error())
		logger.Println("U转I")
		addFile(originDB, rc, fl)
		return
	}

	// 2. 删除目标服务器落盘的文件
	oldPath := ftpToStorePath(ftt.CFLJ)
	err = util.DeleteFile(oldPath)
	if err != nil {
		logger.Printf("%s DeleteFile[deleteFile] error:%s\r\n", rc.Name, err.Error())
	} else {
		logger.Printf("%s DeleteFile[deleteFile]:%s\r\n", rc.Name, oldPath)
	}

	// 3. 删除目标库insert的记录
	err = deleteFileRecord(targetDB, fl, targetTableName)
	if err != nil {
		logger.Printf("%s deleteFileRecord[deleteFile] error:%s\r\n", rc.Name, err.Error())
	}

	// 4. 查询源头库文件详情
	ft, err := queryFile(originDB, originTableName, fl)
	if err != nil {
		logger.Printf("%s queryFile[addFile] error:%s\r\n", rc.Name, err.Error())
		return
	}

	// 5. 下载文件
	// downloadUrl := getFileDownloadUrl(ft, rc) // 源服务器文件下载地址
	// storePath := getFileStorePath(ft, rc, fl) // 目标服务器文件落盘地址
	// err = util.DownloadFile(storePath, downloadUrl)
	downloadUrl, storePath, err := downloadFile(ft, rc, fl)
	if err != nil {
		logger.Printf("%s downloadFile[addFile] error:%s\r\n", rc.Name, err.Error())
		return
	} else {
		logger.Printf("%s downloadFile[addFile]:%s\r\n", rc.Name, downloadUrl)
	}

	// 6. 写目标库FileTable表
	count, err := queryCount(targetDB, targetTableName, fl)
	if err != nil {
		logger.Printf("%s queryCount[addFile] error:%s\r\n", rc.Name, err.Error())
	}
	if count > 0 { // 删除目标库重复的旧记录
		err = deleteFileRecord(targetDB, fl, targetTableName)
		if err != nil {
			logger.Printf("%s deleteFileRecord[addFile] error:%s\r\n", rc.Name, err.Error())
			return
		}
	}
	// insert file table
	ft.CFLJ = getFileFTPPath(storePath) // 修改target库存储的文件路径 2.3使用FTP地址供勘探系统内页面使用
	err = insertFileRecord(targetDB, ft, targetTableName)
	if err != nil {
		logger.Printf("%s insertFileRecord[addFile] error:%s\r\n", rc.Name, err.Error())

		// 删除刚落盘的文件
		err = util.DeleteFile(storePath)
		if err != nil {
			logger.Printf("%s DeleteFile[addFile] error:%s\r\n", rc.Name, err.Error())
		}
		return
	}

	// 7. 删源头库log表
	err = deleteLogRecord(originDB, fl, originLogTableName)
	if err != nil {
		logger.Printf("%s deleteLogRecord[addFile] error:%s\r\n", rc.Name, err.Error())

		// 删除刚落盘的文件
		err = util.DeleteFile(storePath)
		if err != nil {
			logger.Printf("%s DeleteFile[addFile] error:%s\r\n", rc.Name, err.Error())
		}

		// 删除目标库刚insert的记录
		err = deleteFileRecord(targetDB, fl, targetTableName)
		if err != nil {
			logger.Printf("%s deleteFileRecord[addFile] error:%s\r\n", rc.Name, err.Error())
		}

		return
	}
}

// action D : 同步delete文件 并删除log记录
func deleteFile(originDB *sqlx.DB, rc config.RegionConfig, fl FileLog) {
	originLogTableName := rc.DB.LogTable
	targetTableName := cfg.Target.DB.FileTable

	// 1. 查询目标库文件详情
	ft, err := queryFile(targetDB, targetTableName, fl)
	if err != nil {
		logger.Printf("%s queryFile[deleteFile] error:%s\r\n", rc.Name, err.Error())
		return
	}

	// 2. 删除目标服务器落盘的文件
	storePath := ftpToStorePath(ft.CFLJ)
	err = util.DeleteFile(storePath)
	if err != nil {
		logger.Printf("%s DeleteFile[deleteFile] error:%s\r\n", rc.Name, err.Error())
	} else {
		logger.Printf("%s DeleteFile[deleteFile]:%s\r\n", rc.Name, storePath)
	}

	// 3. 删除目标库insert的记录
	err = deleteFileRecord(targetDB, fl, targetTableName)
	if err != nil {
		logger.Printf("%s deleteFileRecord[deleteFile] error:%s\r\n", rc.Name, err.Error())
	}

	// 4. 删源头库log表
	err = deleteLogRecord(originDB, fl, originLogTableName)
	if err != nil {
		logger.Printf("%s deleteLogRecord[deleteFile] error:%s\r\n", rc.Name, err.Error())
	}
}

// 查询文件详情 FileTable
func queryFile(db *sqlx.DB, fileTableName string, fl FileLog) (FileTable, error) {
	if len(fileTableName) == 0 {
		return FileTable{}, errors.New("fileTableName is null")
	}

	var ft FileTable
	sql := fmt.Sprintf("SELECT DW,JH,WDMC,CFLJ,WDLX,WDZY,SJLB,BXDW,BXRQ,BZ FROM \"%s\" WHERE DW =:DW and JH =:JH and WDMC =:WDMC", fileTableName)
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

// 查询文件数量 FileTable
func queryCount(db *sqlx.DB, fileTableName string, fl FileLog) (int, error) {
	if len(fileTableName) == 0 {
		return 0, errors.New("fileTableName is null")
	}

	var count int
	sql := fmt.Sprintf("SELECT COUNT(*) FROM \"%s\" WHERE DW =:DW and JH =:JH and WDMC =:WDMC", fileTableName)

	nstmt, err := db.PrepareNamed(sql)
	err = nstmt.Get(&count, fl)
	if err != nil {
		return 0, err
	}

	return count, nil
}

// insert 文件表FileTable
func insertFileRecord(db *sqlx.DB, ft FileTable, fileTableName string) error {
	sqlStr := fmt.Sprintf(`
		insert into "%s"(DW, JH, WDMC, CFLJ, WDLX, WDZY, SJLB, BXDW, BXRQ, BZ) 
		values (:DW, :JH, :WDMC, :CFLJ, :WDLX, :WDZY,:SJLB, :BXDW, :BXRQ, :BZ)
	`, fileTableName)

	_, err := db.NamedExec(sqlStr, &ft)
	if err != nil {
		return err
	}

	logger.Printf("insert target record %s\r\n", ft.WDMC)
	return nil
}

// delete 文件表FileTable
func deleteLogRecord(db *sqlx.DB, fl FileLog, tableName string) error {
	sql := fmt.Sprintf("DELETE FROM  \"%s\" WHERE SEQUENCE$$ = %s", tableName, fl.SEQUENCE)
	_, err := db.Exec(sql)
	if err != nil {
		return err
	}

	logger.Printf("delete origin log %s(%s)\r\n", fl.SEQUENCE, fl.WDMC)
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
	logger.Printf("delete target record %s\r\n", fl.WDMC)
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

// 下载具体静态文件落盘 分新疆和其他
func downloadFile(ft FileTable, rc config.RegionConfig, fl FileLog) (string, string, error) {

	if rc.Name == "xj" {
		downloadUrl, err := getFileDownloadUrlXj(ft, rc)
		if err != nil {
			return "", "", err
		}

		storePath := getFileStorePath(ft, rc, fl) // 目标服务器文件落盘地址
		err = util.DownloadFile(storePath, downloadUrl)
		return downloadUrl, storePath, err
	} else {
		downloadUrl := getFileDownloadUrl(ft, rc) // 源服务器文件下载地址
		storePath := getFileStorePath(ft, rc, fl) // 目标服务器文件落盘地址
		err := util.DownloadFile(storePath, downloadUrl)
		return downloadUrl, storePath, err
	}

	return "", "", nil
}

// 拼接origin 文件下载地址
func getFileDownloadUrl(ft FileTable, rc config.RegionConfig) string {
	// 源头服务器文件下载地址 == BaseUrl + 截取RootDir之后的剩余path
	restPath := ""
	strlist := strings.Split(ft.CFLJ, rc.RootDir)
	if len(strlist) == 2 {
		restPath = strlist[1]
		u, err := url.JoinPath(rc.BaseUrl, strings.ReplaceAll(restPath, "\\", "/"))
		if err != nil {
			logger.Println("getFileDownloadUrl error: " + err.Error())
		}
		return u
	}

	return ""
}

// 拼接origin 文件下载地址 新疆油田特殊实现
func getFileDownloadUrlXj(ft FileTable, rc config.RegionConfig) (string, error) {
	// 解析新疆文件url
	u, err := url.Parse(ft.CFLJ)
	if err != nil {
		logger.Println("getFileDownloadUrlXj error: " + err.Error())
		return "", err
	}
	if u.Scheme != "http" {
		err = errors.New("新疆非http文件不做处理")
		return "", err
	}

	pathParts := strings.Split(u.Path, "/")
	bucketName := pathParts[1]
	u.Path = strings.Join(pathParts[2:], "/")

	bucketUrl := "https://" + bucketName + "." + u.String()

	// 组装下载文件的url
	params := url.Values{}
	params.Set("url", bucketUrl)
	params.Set("access_token", xjToken)
	u2, err := url.ParseRequestURI(rc.FileDownloadUrl)
	if err != nil {
		fmt.Println("URL解析错误:", err)
		return "", err
	}
	u2.RawQuery = params.Encode()

	return u2.String(), nil
}

// 新疆登录接口func 返回token 和 error
func loginXj(rc config.RegionConfig) (string, error) {
	if rc.Name != "xj" {
		return "", nil
	}

	data := url.Values{}
	data.Set("grant_type", rc.GrantType)
	data.Set("client_id", rc.ClientId)
	data.Set("client_secret", rc.ClientSecret)

	response, err := http.Post(rc.LoginUrl, "application/x-www-form-urlencoded", bytes.NewBufferString(data.Encode()))
	if err != nil {
		fmt.Println("请求错误:", err)
		return "", nil
	}
	defer response.Body.Close()

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println("读取响应错误:", err)
		return "", err
	}

	respData := new(LoginRespXJ)
	err = json.Unmarshal(body, respData)
	if err != nil {
		return "", err
	}

	return respData.AccessToken, nil
}

// 拼接target 文件落盘地址
func getFileStorePath(ft FileTable, originRC config.RegionConfig, fl FileLog) string {
	filename := path.Base(strings.ReplaceAll(ft.CFLJ, "\\", "/"))
	// 目标服务器文件落盘地址 == RootDir + cnpc_dq + 井号第一个字 + 井号 + 文件名
	p := path.Join(cfg.Target.RootDir, regionPrefix+originRC.Name, fl.JH[0:3], fl.JH, filename)
	return p
}

// 拼接target 文件入库地址 ftp地址
func getFileFTPPath(storePath string) string {
	restPath := ""
	strlist := strings.Split(storePath, cfg.Target.RootDir)
	if len(strlist) == 2 {
		restPath = strlist[1]
		p := cfg.Target.FtpPrefix + strings.ReplaceAll(restPath, "\\", "/")
		return p
	}

	return ""
}

// 拼接target 文件入库地址(ftp地址)  转 文件落盘地址
func ftpToStorePath(ftpPath string) string {
	restPath := ""
	strlist := strings.Split(ftpPath, cfg.Target.FtpPrefix)
	if len(strlist) == 2 {
		restPath = strlist[1]
		p := cfg.Target.RootDir + strings.ReplaceAll(restPath, "/", "\\")
		return p
	}

	return ""
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

type LoginRespXJ struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
	Scope       string `json:"scope"`
}
