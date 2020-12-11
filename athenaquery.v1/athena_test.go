package athena

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/athena/athenaiface"
)

//MockAthenaClient to mock athena client
type MockAthenaClient struct {
	athenaiface.AthenaAPI
}

type MockAthenaClientFail struct {
	athenaiface.AthenaAPI
}

type MockAthenaClientWait struct {
	athenaiface.AthenaAPI
	status string
}

//StartQueryExecution ..
func (m *MockAthenaClient) StartQueryExecution(*athena.StartQueryExecutionInput) (*athena.StartQueryExecutionOutput, error) {
	queryID := "12345-12345"
	return &athena.StartQueryExecutionOutput{QueryExecutionId: &queryID}, nil
}
func (m *MockAthenaClientFail) StartQueryExecution(*athena.StartQueryExecutionInput) (*athena.StartQueryExecutionOutput, error) {
	return nil, fmt.Errorf("StartQueryExecution mock error")
}

//GetQueryExecution ..
func (m *MockAthenaClientFail) GetQueryExecution(*athena.GetQueryExecutionInput) (*athena.GetQueryExecutionOutput, error) {
	return nil, fmt.Errorf("GetQueryExecution mock error")
}

//GetQueryExecution ..
func (m *MockAthenaClient) GetQueryExecution(*athena.GetQueryExecutionInput) (*athena.GetQueryExecutionOutput, error) {
	return &athena.GetQueryExecutionOutput{QueryExecution: &athena.QueryExecution{Status: &athena.QueryExecutionStatus{State: aws.String(athena.QueryExecutionStateSucceeded)}}}, nil
}

func (m *MockAthenaClientWait) GetQueryExecution(*athena.GetQueryExecutionInput) (*athena.GetQueryExecutionOutput, error) {
	return &athena.GetQueryExecutionOutput{QueryExecution: &athena.QueryExecution{Status: &athena.QueryExecutionStatus{State: aws.String(m.status)}}}, nil
}

func (m *MockAthenaClient) GetQueryResults(*athena.GetQueryResultsInput) (*athena.GetQueryResultsOutput, error) {
	return &athena.GetQueryResultsOutput{
		ResultSet: &athena.ResultSet{
			ResultSetMetadata: &athena.ResultSetMetadata{
				ColumnInfo: []*athena.ColumnInfo{
					&athena.ColumnInfo{
						Name:       aws.String("max_job_id"),
						SchemaName: aws.String("job_id"),
						TableName:  aws.String("viewership"),
						Type:       aws.String("string"),
					},
				},
			},
			Rows: []*athena.Row{
				&athena.Row{
					Data: []*athena.Datum{
						&athena.Datum{VarCharValue: aws.String("20200825")},
					},
				},
			},
		}}, nil
}
func (m *MockAthenaClientFail) GetQueryResults(*athena.GetQueryResultsInput) (*athena.GetQueryResultsOutput, error) {
	return nil, fmt.Errorf("GetQueryResults mock error")
}

var mockAthenaClient = &MockAthenaClient{}
var mockAthenaClientFail = &MockAthenaClientFail{}

func TestAthenaEngine_Exec(t *testing.T) {
	type args struct {
		param *RequestParam
	}
	tests := []struct {
		name           string
		args           args
		want           *ResponseData
		wantErr        bool
		wantFailClient bool
	}{
		{name: "testStartQuery",
			args: args{
				param: &RequestParam{
					SQL:      "SELECT * FROM viewership",
					QueryOpt: "startQuery",
					DataBase: "viewership",
				},
			},
			want:           &ResponseData{QueryID: "12345-12345"},
			wantFailClient: false,
		},
		{name: "testCheckStatusByQueryID",
			args: args{
				param: &RequestParam{
					QueryID:  "12345-12345",
					QueryOpt: "queryStatus",
					DataBase: "viewership",
				},
			},
			want:           &ResponseData{QueryID: "12345-12345", QueryStatus: "SUCCEEDED"},
			wantFailClient: false,
		},
		{name: "testQueryResult",
			args: args{
				param: &RequestParam{
					QueryID:  "SELECT * FROM viewership",
					QueryOpt: "queryResult",
					DataBase: "index",
				},
			},
			want: &ResponseData{QueryID: "12345-12345", QueryStatus: "SUCCEEDED", Columns: []*athena.ColumnInfo{
				&athena.ColumnInfo{
					Name:       aws.String("max_job_id"),
					SchemaName: aws.String("job_id"),
					TableName:  aws.String("viewership"),
					Type:       aws.String("string"),
				},
			},
				Rows: []*athena.Row{
					&athena.Row{
						Data: []*athena.Datum{
							&athena.Datum{VarCharValue: aws.String("20200825")},
						},
					},
				},
			},
			wantFailClient: false,
		},
		{name: "testStartQuery-fail",
			args: args{
				param: &RequestParam{
					SQL:      "SELECT * FROM viewership",
					QueryOpt: "startQuery",
					DataBase: "viewership",
				},
			},
			wantFailClient: true,
			wantErr:        true,
		},
		{name: "testCheckStatusByQueryID-fail",
			args: args{
				param: &RequestParam{
					QueryID:  "12345-12345",
					QueryOpt: "queryStatus",
					DataBase: "viewership",
				},
			},
			wantFailClient: true,
			wantErr:        true,
		},
		{name: "testQueryResult-fail",
			args: args{
				param: &RequestParam{
					QueryID:  "SELECT * FROM viewership",
					QueryOpt: "queryResult",
					DataBase: "index",
				},
			},
			wantFailClient: true,
			wantErr:        true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &AthenaEngine{
				athena: mockAthenaClient,
			}

			if tt.wantFailClient {
				c = &AthenaEngine{
					athena: mockAthenaClientFail,
				}
			}

			got, err := c.Exec(tt.args.param)
			if (err != nil) != tt.wantErr {
				t.Errorf("AthenaEngine.Exec() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("AthenaEngine.Exec() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetInstance(t *testing.T) {
	type args struct {
		config *Config
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{name: "t-1", args: args{config: &Config{
			Region:         "us-east-1",
			OutputLocation: "s3:xxx",
			MaxInterval:    3,
			MaxTimeout:     15,
		}}, wantErr: false,
		},
		{name: "t-2", args: args{config: nil}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, e := GetInstance(tt.args.config)
			if (e != nil) != tt.wantErr {
				t.Errorf("GetInstance() = %v", got)
			}
		})
	}
}

func TestAthenaEngine_Connect(t *testing.T) {
	type fields struct {
		athena         athenaiface.AthenaAPI
		db             string
		OutputLocation string
		MaxInterval    int
		MaxTimeout     int
		pollFrequency  time.Duration
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{name: "", fields: fields{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &AthenaEngine{
				athena:         tt.fields.athena,
				db:             tt.fields.db,
				OutputLocation: tt.fields.OutputLocation,
				MaxInterval:    tt.fields.MaxInterval,
				MaxTimeout:     tt.fields.MaxTimeout,
				pollFrequency:  tt.fields.pollFrequency,
			}
			c.Connect()
		})
	}
}

func TestAthenaEngine_setupAthenaSession(t *testing.T) {
	type fields struct {
		athena         athenaiface.AthenaAPI
		db             string
		OutputLocation string
		MaxInterval    int
		MaxTimeout     int
		pollFrequency  time.Duration
	}
	type args struct {
		config *Config
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{name: "t-1", args: args{}, wantErr: true},
		{name: "t-2", args: args{config: &Config{Region: "us-east-1"}}, wantErr: false},
		{name: "t-3", args: args{config: &Config{Role: "role-test"}}, wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &AthenaEngine{
				athena:         tt.fields.athena,
				db:             tt.fields.db,
				OutputLocation: tt.fields.OutputLocation,
				MaxInterval:    tt.fields.MaxInterval,
				MaxTimeout:     tt.fields.MaxTimeout,
				pollFrequency:  tt.fields.pollFrequency,
			}
			if err := c.setupAthenaSession(tt.args.config); (err != nil) != tt.wantErr {
				t.Errorf("AthenaEngine.setupAthenaSession() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestAthenaEngine_getAthenaWithRole(t *testing.T) {
	type fields struct {
		athena         athenaiface.AthenaAPI
		db             string
		OutputLocation string
		MaxInterval    int
		MaxTimeout     int
		pollFrequency  time.Duration
	}
	type args struct {
		role string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *athena.Athena
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &AthenaEngine{
				athena:         tt.fields.athena,
				db:             tt.fields.db,
				OutputLocation: tt.fields.OutputLocation,
				MaxInterval:    tt.fields.MaxInterval,
				MaxTimeout:     tt.fields.MaxTimeout,
				pollFrequency:  tt.fields.pollFrequency,
			}
			if got := c.getAthenaWithRole(tt.args.role); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("AthenaEngine.getAthenaWithRole() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAthenaEngine_getAthenaWithRegion(t *testing.T) {
	type fields struct {
		athena         athenaiface.AthenaAPI
		db             string
		OutputLocation string
		MaxInterval    int
		MaxTimeout     int
		pollFrequency  time.Duration
	}
	type args struct {
		region string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *athena.Athena
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &AthenaEngine{
				athena:         tt.fields.athena,
				db:             tt.fields.db,
				OutputLocation: tt.fields.OutputLocation,
				MaxInterval:    tt.fields.MaxInterval,
				MaxTimeout:     tt.fields.MaxTimeout,
				pollFrequency:  tt.fields.pollFrequency,
			}
			if got := c.getAthenaWithRegion(tt.args.region); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("AthenaEngine.getAthenaWithRegion() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAthenaEngine_ExecuteQuery(t *testing.T) {
	type fields struct {
		athena         athenaiface.AthenaAPI
		db             string
		OutputLocation string
		MaxInterval    int
		MaxTimeout     int
		pollFrequency  time.Duration
	}
	type args struct {
		qi *RequestParam
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		wantQueryID string
		wantErr     bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &AthenaEngine{
				athena:         tt.fields.athena,
				db:             tt.fields.db,
				OutputLocation: tt.fields.OutputLocation,
				MaxInterval:    tt.fields.MaxInterval,
				MaxTimeout:     tt.fields.MaxTimeout,
				pollFrequency:  tt.fields.pollFrequency,
			}
			gotQueryID, err := c.ExecuteQuery(tt.args.qi)
			if (err != nil) != tt.wantErr {
				t.Errorf("AthenaEngine.ExecuteQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotQueryID != tt.wantQueryID {
				t.Errorf("AthenaEngine.ExecuteQuery() = %v, want %v", gotQueryID, tt.wantQueryID)
			}
		})
	}
}

func TestAthenaEngine_CheckStatusByQueryID(t *testing.T) {
	type args struct {
		queryID string
	}
	tests := []struct {
		name       string
		args       args
		wantStatus string
		wantErr    bool
	}{
		{name: "t-1", wantStatus: "SUCCEEDED"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &AthenaEngine{
				athena: mockAthenaClient,
			}
			gotStatus, err := c.CheckStatusByQueryID(tt.args.queryID)
			if (err != nil) != tt.wantErr {
				t.Errorf("AthenaEngine.CheckStatusByQueryID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotStatus != tt.wantStatus {
				t.Errorf("AthenaEngine.CheckStatusByQueryID() = %v, want %v", gotStatus, tt.wantStatus)
			}
		})
	}
}

func TestAthenaEngine_PrintQueryStatus(t *testing.T) {
	startDate, endDate := time.Date(2020, 1, 1, 1, 0, 0, 0, time.UTC), time.Date(2020, 1, 2, 1, 0, 0, 0, time.UTC)
	type args struct {
		qe *athena.QueryExecution
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{name: "t-1", args: args{qe: &athena.QueryExecution{}}, want: "[Athena Query Excution Status] Nil Query Status"},
		{name: "t-2", args: args{qe: &athena.QueryExecution{
			Status: &athena.QueryExecutionStatus{
				State:              aws.String(athena.QueryExecutionStateSucceeded),
				SubmissionDateTime: &startDate,
				CompletionDateTime: &endDate,
			},
		}}, want: "[Athena Query Excution Status] query_id= ,query_state=SUCCEEDED, duration=86400.000000"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &AthenaEngine{
				athena: mockAthenaClient,
			}
			if got := c.PrintQueryStatus(tt.args.qe); got != tt.want {
				t.Errorf("AthenaEngine.PrintQueryStatus() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAthenaEngine_GetQueryResultByQueryID(t *testing.T) {
	type args struct {
		queryID string
	}
	tests := []struct {
		name           string
		args           args
		want           []*athena.ColumnInfo
		want1          []*athena.Row
		wantErr        bool
		wantFailClient bool
	}{
		{name: "t1", args: args{queryID: "1234-1234"},
			want: []*athena.ColumnInfo{
				&athena.ColumnInfo{
					Name:       aws.String("max_job_id"),
					SchemaName: aws.String("job_id"),
					TableName:  aws.String("viewership"),
					Type:       aws.String("string"),
				},
			},
			want1: []*athena.Row{
				&athena.Row{
					Data: []*athena.Datum{
						&athena.Datum{VarCharValue: aws.String("20200825")},
					},
				},
			},
		},
		{name: "t1", args: args{queryID: "1234-1234"},
			wantFailClient: true,
			wantErr:        true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &AthenaEngine{
				athena: mockAthenaClient,
			}

			if tt.wantFailClient {
				c = &AthenaEngine{
					athena: mockAthenaClientFail,
				}
			}
			got, got1, err := c.GetQueryResultByQueryID(tt.args.queryID)
			if (err != nil) != tt.wantErr {
				t.Errorf("AthenaEngine.GetQueryResultByQueryID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("AthenaEngine.GetQueryResultByQueryID() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("AthenaEngine.GetQueryResultByQueryID() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestAthenaEngine_QueryResult(t *testing.T) {
	type fields struct {
		athena         athenaiface.AthenaAPI
		db             string
		OutputLocation string
		MaxInterval    int
		MaxTimeout     int
		pollFrequency  time.Duration
	}
	type args struct {
		qi *RequestParam
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *ResponseData
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &AthenaEngine{
				athena:         tt.fields.athena,
				db:             tt.fields.db,
				OutputLocation: tt.fields.OutputLocation,
				MaxInterval:    tt.fields.MaxInterval,
				MaxTimeout:     tt.fields.MaxTimeout,
				pollFrequency:  tt.fields.pollFrequency,
			}
			got, err := c.QueryResult(tt.args.qi)
			if (err != nil) != tt.wantErr {
				t.Errorf("AthenaEngine.QueryResult() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("AthenaEngine.QueryResult() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAthenaEngine_waitQueryToFinish(t *testing.T) {
	type args struct {
		queryID string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{name: "t1", args: args{queryID: "1234-1234"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &AthenaEngine{
				athena:      &MockAthenaClientWait{status: "RUNNING"},
				MaxTimeout:  1,
				MaxInterval: 1,
			}

			if err := c.waitQueryToFinish(tt.args.queryID); (err != nil) != tt.wantErr {
				t.Errorf("AthenaEngine.waitQueryToFinish() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestAthenaEngine_getResultByQueryID(t *testing.T) {
	type fields struct {
		athena         athenaiface.AthenaAPI
		db             string
		OutputLocation string
		MaxInterval    int
		MaxTimeout     int
		pollFrequency  time.Duration
	}
	type args struct {
		queryID string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []*athena.ColumnInfo
		want1   []*athena.Row
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &AthenaEngine{
				athena:         tt.fields.athena,
				db:             tt.fields.db,
				OutputLocation: tt.fields.OutputLocation,
				MaxInterval:    tt.fields.MaxInterval,
				MaxTimeout:     tt.fields.MaxTimeout,
				pollFrequency:  tt.fields.pollFrequency,
			}
			got, got1, err := c.getResultByQueryID(tt.args.queryID)
			if (err != nil) != tt.wantErr {
				t.Errorf("AthenaEngine.getResultByQueryID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("AthenaEngine.getResultByQueryID() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("AthenaEngine.getResultByQueryID() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestBuildAthenaConfig(t *testing.T) {
	type args struct {
		conf map[string]string
	}
	tests := []struct {
		name string
		args args
		want *Config
	}{
		{name: "t-1", args: args{
			conf: map[string]string{
				"maxInterval":     "1",
				"maxTimeout":      "1",
				"output_location": "test",
				"region":          "testRegion",
				"role":            "testRole",
			},
		}, want: &Config{
			Region:         "testRegion",
			Role:           "testRole",
			MaxTimeout:     1,
			MaxInterval:    1,
			OutputLocation: "test",
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := BuildAthenaConfig(tt.args.conf); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("BuildAthenaConfig() = %v, want %v", got, tt.want)
			}
		})
	}
}
