package aws

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/pipedrive/uncouch/config"
	"github.com/pipedrive/uncouch/logger"
	"go.uber.org/zap"
)

var (
	log  *zap.Logger
	slog *zap.SugaredLogger
	sess *session.Session
	err error
)

func init() {
	log, slog = logger.GetLogger()

	// The session the S3 Uploader will use.
	sess, err = session.NewSessionWithOptions(session.Options{
		Config:  aws.Config{Region: aws.String(config.AWS_REGION)},
		Profile: config.AWS_PROFILE,
	})
	if err != nil {
		slog.Error(err)
		return
	}
}