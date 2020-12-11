package athena

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/athena/athenaiface"
)

const (
	QueryOptStart  = "startQuery"
	QueryOptStatus = "queryStatus"
	QueryOptResult = "queryResult"
)

type AthenaEngine struct {
	athena         athenaiface.AthenaAPI
	db             string
	OutputLocation string
	MaxInterval    int
	MaxTimeout     int

	pollFrequency time.Duration
	//engine.BaseEngine
}

func GetInstance(config *Config) (*AthenaEngine, error) {
	if config == nil {
		return nil, fmt.Errorf("The config is nil")
	}
	c := &AthenaEngine{
		OutputLocation: config.OutputLocation,
		MaxInterval:    config.MaxInterval,
		MaxTimeout:     config.MaxTimeout,
	}
	err := c.setupAthenaSession(config)
	if err != nil {
		return nil, err
	}
	return c, nil
}

//Exec for athena query
func (c *AthenaEngine) Exec(param *RequestParam) (*ResponseData, error) {

	if param == nil {
		return nil, nil
	}

	switch param.QueryOpt {
	case QueryOptStart:
		queryID, err := c.ExecuteQuery(param)
		if err != nil {
			return nil, err
		}
		return &ResponseData{
			QueryID: queryID,
		}, nil

	case QueryOptStatus:
		status, err := c.CheckStatusByQueryID(param.QueryID)
		if err != nil {
			return nil, err
		}
		return &ResponseData{
			QueryID:     param.QueryID,
			QueryStatus: status,
		}, nil

	case QueryOptResult:
		return c.QueryResult(param)
	}
	return nil, nil
}

//For athena connect, it'll only setup the connection config
func (c *AthenaEngine) Connect() {}

//SetupAthenaSession to set up the session from the config
func (c *AthenaEngine) setupAthenaSession(config *Config) error {
	if config == nil {
		return fmt.Errorf("The config is missing")
	}
	if config.Role != "" {
		c.athena = c.getAthenaWithRole(config.Role)
		return nil
	}
	if config.Region != "" {
		c.athena = c.getAthenaWithRegion(config.Region)
		return nil
	}
	return fmt.Errorf("The Athena Config is insufficient")
}

func (c *AthenaEngine) getAthenaWithRole(role string) *athena.Athena {
	session, cfg := NewSessionWithRole(role)
	return athena.New(session, cfg)
}

func (c *AthenaEngine) getAthenaWithRegion(region string) *athena.Athena {
	session, _ := NewSessionWithRegion(region)
	return athena.New(session)
}

//AthenaQuery is the interface to operate the query
type AthenaQuery interface {
	Exec(param *RequestParam) (*ResponseData, error)
	ExecuteQuery(*RequestParam) (queryID string, err error)
	CheckStatusByQueryID(string) (status string, err error)
	QueryResult(*RequestParam) (*ResponseData, error)
}

// func (c *AthenaEngine) Config(conf viper.Viper) {
// 	logger.Infof("Athena Config is loading ... ")
// 	if c.confMap == nil {
// 		c.confMap = make(map[string]*Config)
// 	}
// 	dbs := config.AthenaConfig(conf)
// 	for ds := range dbs {
// 		dbconfig := conf.GetStringMapString(fmt.Sprintf("athena.%s", ds))
// 		c.confMap[ds] = db.BuildAthenaConfig(dbconfig)
// 		logger.Infof("Athena-%s config is loaded: %v", ds, c.confMap[ds])
// 	}
// }

//ExecuteQuery to execute the athena query
func (c *AthenaEngine) ExecuteQuery(qi *RequestParam) (queryID string, err error) {
	fmt.Printf("[Executing Athena Query] %s", qi)
	queryInput := &athena.StartQueryExecutionInput{
		QueryString:           aws.String(qi.SQL),
		QueryExecutionContext: &athena.QueryExecutionContext{Database: aws.String(qi.DataBase)},
		ResultConfiguration:   &athena.ResultConfiguration{OutputLocation: aws.String(c.OutputLocation)},
	}
	output, err := c.athena.StartQueryExecution(queryInput)
	if err != nil {
		fmt.Errorf("Athena Query Error: %s", err.Error())
		return "", err
	}
	fmt.Printf("[Finished Athena Query] QueryID: %s", aws.StringValue(output.QueryExecutionId))
	return aws.StringValue(output.QueryExecutionId), nil
}

//CheckStatusByQueryID to check the query status
func (c *AthenaEngine) CheckStatusByQueryID(queryID string) (status string, err error) {
	input := &athena.GetQueryExecutionInput{QueryExecutionId: aws.String(queryID)}
	output, err := c.athena.GetQueryExecution(input)
	if err != nil {
		return "", err
	}
	status = aws.StringValue(output.QueryExecution.Status.State)
	c.PrintQueryStatus(output.QueryExecution)
	return status, nil
}

//PrintQueryStatus to print the query status
func (c *AthenaEngine) PrintQueryStatus(qe *athena.QueryExecution) string {
	if qe == nil || qe.Status == nil {
		return "[Athena Query Excution Status] Nil Query Status"
	}

	status, queryID, duration, endTime := qe.Status, aws.StringValue(qe.QueryExecutionId), 0.0, time.Now()

	if status.CompletionDateTime != nil {
		endTime = *status.CompletionDateTime
	}

	if aws.StringValue(status.State) == athena.QueryExecutionStateSucceeded && status.SubmissionDateTime != nil {
		st := *status.SubmissionDateTime
		duration = endTime.Sub(st).Seconds()
	}

	return fmt.Sprintf("[Athena Query Excution Status] query_id=%s ,query_state=%s, duration=%f", queryID, aws.StringValue(status.State), duration)
}

//GetQueryResultByQueryID to get rows by queryID
func (c *AthenaEngine) GetQueryResultByQueryID(queryID string) ([]*athena.ColumnInfo, []*athena.Row, error) {
	cols, rows := []*athena.ColumnInfo{}, []*athena.Row{}

	input := athena.GetQueryResultsInput{QueryExecutionId: aws.String(queryID)}
	out, err := c.athena.GetQueryResults(&input)
	if err != nil {
		return nil, nil, err
	}

	cols, rows = out.ResultSet.ResultSetMetadata.ColumnInfo, out.ResultSet.Rows

	return cols, rows, nil
}

func (c *AthenaEngine) QueryResult(qi *RequestParam) (*ResponseData, error) {
	queryID, err := c.ExecuteQuery(qi)
	if err != nil {
		return nil, err
	}

	if err := c.waitQueryToFinish(queryID); err != nil {
		return nil, err
	}

	// return adapter.NewRows(adapter.RowsConfig{
	// 	Athena:  c.athena,
	// 	QueryID: queryID,
	// 	// todo add check for ddl queries to not skip header(#10)
	// 	SkipHeader: true,
	// })

	cols, rows, err := c.getResultByQueryID(queryID)
	if err != nil {
		return nil, err
	}
	return &ResponseData{
		QueryID:     queryID,
		Columns:     cols,
		Rows:        rows,
		QueryStatus: athena.QueryExecutionStateSucceeded,
	}, nil
}

func (c *AthenaEngine) waitQueryToFinish(queryID string) error {
	if c.athena == nil {
		return fmt.Errorf("The query.AthenaQuery is nil")
	}
	runtime, isRunning := 0, true
	for isRunning {
		s, e := c.CheckStatusByQueryID(queryID)
		if e != nil {
			return e
		}
		switch s {
		case athena.QueryExecutionStateFailed:
			return fmt.Errorf("The Athena Query %s is failed", queryID)
		case athena.QueryExecutionStateCancelled:
			return fmt.Errorf("The Athena Query %s is cancelled", queryID)
		case athena.QueryExecutionStateSucceeded:
			isRunning = false
		case athena.QueryExecutionStateRunning:
			fmt.Printf("running")
			runtime += c.MaxInterval
			if runtime > c.MaxTimeout {
				return fmt.Errorf("The Athena Query %s is timeout", queryID)
			}
			time.Sleep(time.Duration(3) * time.Second)
		}
	}
	return nil
}

func (c *AthenaEngine) getResultByQueryID(queryID string) ([]*athena.ColumnInfo, []*athena.Row, error) {
	cols, rows := []*athena.ColumnInfo{}, []*athena.Row{}

	input := athena.GetQueryResultsInput{QueryExecutionId: aws.String(queryID)}
	out, err := c.athena.GetQueryResults(&input)
	if err != nil {
		return nil, nil, err
	}

	cols, rows = out.ResultSet.ResultSetMetadata.ColumnInfo, out.ResultSet.Rows

	return cols, rows, nil
}
