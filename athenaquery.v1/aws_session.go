package athena

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
)

type AwsSession struct {
	*session.Session
}

//NewSessionWithKeys for aws opt
func NewSessionWithKeys(region, accessKey, secretAccessKey string) (*AwsSession, error) {
	fmt.Printf("[New AWS Session with keys] region=%s, accessKey=%s, secretAccessKey=%s", region, accessKey, secretAccessKey)
	config := aws.NewConfig().WithRegion(region).
		WithCredentials(credentials.NewStaticCredentials(accessKey, secretAccessKey, ""))
	sess, err := session.NewSession(config)
	if err != nil {
		return nil, err
	}
	return &AwsSession{Session: sess}, nil
}

//NewSession for aws opt
func NewSession() *AwsSession {
	sess := session.Must(session.NewSession())
	return &AwsSession{Session: sess}
}

//NewSessionWithRole for aws opt
func NewSessionWithRole(role string) (*AwsSession, *aws.Config) {
	fmt.Printf("[New AWS Session with Role1] role=%s", role)
	sess := NewSession()
	creds := stscreds.NewCredentials(sess, role)
	return sess, &aws.Config{Credentials: creds}
}

//NewSessionWithRegion for aws opt
func NewSessionWithRegion(region string) (*AwsSession, error) {
	config := aws.NewConfig().WithRegion(region)
	sess, err := session.NewSession(config)
	if err != nil {
		return nil, err
	}
	return &AwsSession{Session: sess}, nil
}
