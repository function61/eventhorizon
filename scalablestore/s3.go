package scalablestore

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/function61/eventhorizon/config"
	"io"
	"strings"
	"time"
)

type ScalableStoreGetResponse struct {
	Body io.ReadCloser
}

type S3Manager struct {
	bucketName string
	s3Client   *s3.S3
}

func NewS3Manager(confCtx *config.Context) *S3Manager {
	awsSession, err := session.NewSession()
	if err != nil {
		panic(err)
	}

	url := confCtx.ScalableStoreUrl()

	secretAccessKey, provided := url.User.Password()
	if !provided {
		panic("Secret access key not provided")
	}

	// cannot represent '/' char in secret access key,
	// it is represented by '_' (not used in secret access keys)
	secretAccessKey = strings.Replace(secretAccessKey, "_", "/", -1)

	manualCredential := credentials.NewStaticCredentials(
		url.User.Username(), // AWS_ACCESS_KEY_ID
		secretAccessKey,     // AWS_SECRET_ACCESS_KEY
		"")

	s3Client := s3.New(awsSession, aws.NewConfig().WithCredentials(manualCredential).WithRegion(url.Host))

	bucketName := url.Path[1:] // '/bucket-name' => 'bucket-name'

	s := &S3Manager{bucketName, s3Client}

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

// this can panic() on context deadline
func (s *S3Manager) Get(key string) (*ScalableStoreGetResponse, error) {
	request, response := s.s3Client.GetObjectRequest(&s3.GetObjectInput{
		Bucket: &s.bucketName,
		Key:    &key,
	})

	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	// cancel is advised to be used always, even on successes
	// TODO: doesn't seem to work here, probably Send() returns before the whole request is read, because
	//       we got unexpected EOF with cancel(). that's good because it means S3 client supports streaming
	// defer cancel()

	// monkey patch the request to use our context
	request.HTTPRequest = request.HTTPRequest.WithContext(ctx)

	if err := request.Send(); err != nil { // FIXME: assuming 404, not any other error like network error..
		return nil, err
	}

	return &ScalableStoreGetResponse{
		Body: response.Body,
	}, nil
}
