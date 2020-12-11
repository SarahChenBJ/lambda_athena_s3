package athena

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
)

func TestNewSessionWithKeys(t *testing.T) {
	type args struct {
		region          string
		accessKey       string
		secretAccessKey string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{name: "t-1", args: args{region: "us-east-1", accessKey: "key", secretAccessKey: "secret"}, wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewSessionWithKeys(tt.args.region, tt.args.accessKey, tt.args.secretAccessKey)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewSessionWithKeys() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestNewSession(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "t-1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewSession(); got == nil {
				t.Errorf("NewSession() = %v should not be nil", got)
			}
		})
	}
}

func TestNewSessionWithRole(t *testing.T) {
	type args struct {
		role string
	}
	tests := []struct {
		name  string
		args  args
		want  *AwsSession
		want1 *aws.Config
	}{
		{name: "t-1", args: args{role: "arn:xx:xx"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := NewSessionWithRole(tt.args.role)
			if got == nil {
				t.Errorf("NewSessionWithRole() got = %v", got)
			}
			if got1 == nil {
				t.Errorf("NewSessionWithRole() got1 = %v", got1)
			}
		})
	}
}

func TestNewSessionWithRegion(t *testing.T) {
	type args struct {
		region string
	}
	tests := []struct {
		name    string
		args    args
		want    *AwsSession
		wantErr bool
	}{
		{name: "t-1", args: args{region: "us-east-1"}, wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewSessionWithRegion(tt.args.region)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewSessionWithRegion() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

		})
	}
}
