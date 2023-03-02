package database

import (
	"fmt"

	_ "github.com/godror/godror"
	"github.com/jmoiron/sqlx"
	"prospect_file_sync/config"
)

func ConnectDB(cfg config.DB) (*sqlx.DB, error) {
	dsn := fmt.Sprintf(`user="%s" password="%s" connectString="%s:%d/%s"`, cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.ServiceName)
	var err error
	DB, err := sqlx.Open("godror", dsn)
	if err != nil {
		return nil, err
	}
	// defer DB.Close()
	err = DB.Ping()
	if err != nil {
		return nil, err
	}
	fmt.Println(cfg.Host + "-db链接成功")

	return DB, nil
}
