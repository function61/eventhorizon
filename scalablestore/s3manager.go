package scalablestore

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/function61/eventhorizon/config"
	"io"
)

type ScalableStoreGetResponse struct {
	Body io.ReadCloser
}

type S3Manager struct {
	bucketName string
	s3Client   *s3.S3
}

func NewS3Manager() *S3Manager {
	awsSession, err := session.NewSession()
	if err != nil {
		panic(err)
	}

	s3Client := s3.New(awsSession, aws.NewConfig().WithRegion("us-east-1"))

	s := &S3Manager{config.S3_BUCKET, s3Client}

	return s
}

func (s *S3Manager) Put(key string, body io.ReadSeeker) error {
	_, err := s.s3Client.PutObject(&s3.PutObjectInput{
		Bucket: &s.bucketName,
		Key:    &key,
		Body:   body,
	})

	return err
}

func (s *S3Manager) Get(key string) (*ScalableStoreGetResponse, error) {
	response, err := s.s3Client.GetObject(&s3.GetObjectInput{
		Bucket: &s.bucketName,
		Key:    &key,
	})

	if err != nil { // FIXME: assuming 404, not any other error like network error..
		return nil, err
	}

	return &ScalableStoreGetResponse{
		Body: response.Body,
	}, nil
}
