package athena

import (
	"strconv"
	"fmt"

	"github.com/aws/aws-sdk-go/service/athena"
)

//AthenaConfig for config
type Config struct {
	Region         string
	OutputLocation string
	PollFrequency  string
	AccessID       string
	SecretKey      string
	SessionToken   string
	Role           string
	MaxInterval    int
	MaxTimeout     int
}

//AthenaRequestParam for request
type RequestParam struct {
	SQL        string
	QueryID    string
	DataSource string
	DataBase   string
	NetworkID  int64
	QueryOpt   string
}

//AthenaResponseData for response
type ResponseData struct {
	Columns     []*athena.ColumnInfo
	Rows        []*athena.Row
	QueryID     string
	QueryStatus string
}

//BuildAthenaConfig for athena engine
func BuildAthenaConfig(conf map[string]string) *Config {
	if len(conf) == 0 {
		fmt.Errorf("BuildDBConfig error: input is nil.")
		return nil
	}

	maxIv, _ := strconv.Atoi(conf["maxInterval"])
	maxTo, _ := strconv.Atoi(conf["maxTimeout"])
	return &Config{
		OutputLocation: conf["output_location"],
		PollFrequency:  conf["poll_frequency"],
		MaxInterval:    maxIv,
		MaxTimeout:     maxTo,
		AccessID:       conf["access_id"],
		SecretKey:      conf["secret_key"],
		SessionToken:   conf["session_token"],
		Role:           conf["role"],
		Region:         conf["region"],
	}
}
