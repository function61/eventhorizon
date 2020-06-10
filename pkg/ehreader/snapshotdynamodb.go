package ehreader

import (
	"context"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/function61/eventhorizon/pkg/dynamoutils"
	"github.com/function61/eventhorizon/pkg/ehclient"
)

type DynamoSnapshotItem struct {
	Stream  string `json:"s"` // stream + context form the composite key
	Context string `json:"c"` // different software can read the same stream,
	Version int64  `json:"v"` // we conditionally put updates into DynamoDB as not to overwrite advanced state
	Data    []byte `json:"d"` // actual snapshot data
}

type dynamoSnapshotStorage struct {
	context            string // unique string for your software
	dynamo             *dynamodb.DynamoDB
	snapshotsTableName *string
}

func NewDynamoDbSnapshotStore(opts ehclient.DynamoDbOptions, context string) (SnapshotStore, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}

	staticCreds := credentials.NewStaticCredentials(
		opts.AccessKeyId,
		opts.AccessKeySecret,
		"")

	dynamo := dynamodb.New(
		sess,
		aws.NewConfig().WithCredentials(staticCreds).WithRegion(opts.RegionId))

	return &dynamoSnapshotStorage{
		context:            context,
		dynamo:             dynamo,
		snapshotsTableName: &opts.TableName,
	}, nil
}

func (d *dynamoSnapshotStorage) LoadSnapshot(ctx context.Context, lastKnown ehclient.Cursor) (*Snapshot, error) {
	getResponse, err := d.dynamo.GetItemWithContext(ctx, &dynamodb.GetItemInput{
		Key: dynamoutils.Record{
			"s": dynamoutils.String(lastKnown.Stream()),
			"c": dynamoutils.String(d.context),
		},
		TableName: d.snapshotsTableName,
	})
	if err != nil {
		return nil, err
	}

	// genius, for items that don't exist the only way to know is to check that there are
	// no attributes.........
	if len(getResponse.Item) == 0 {
		return nil, os.ErrNotExist
	}

	dynamoSnapshot := DynamoSnapshotItem{}
	if err := dynamoutils.Unmarshal(getResponse.Item, &dynamoSnapshot); err != nil {
		return nil, err
	}

	cursorInSnapshot := ehclient.At(dynamoSnapshot.Stream, dynamoSnapshot.Version)

	return NewSnapshot(cursorInSnapshot, dynamoSnapshot.Data), nil
}

func (d *dynamoSnapshotStorage) StoreSnapshot(ctx context.Context, snap Snapshot) error {
	dynamoSnapshot, err := dynamoutils.Marshal(DynamoSnapshotItem{
		Stream:  snap.Cursor.Stream(),
		Context: d.context,
		Version: snap.Cursor.Version(),
		Data:    snap.Data,
	})
	if err != nil {
		return err
	}

	_, err = d.dynamo.PutItemWithContext(ctx, &dynamodb.PutItemInput{
		TableName:           d.snapshotsTableName,
		Item:                dynamoSnapshot,
		ConditionExpression: aws.String("attribute_not_exists(v) OR v < :versionToPut"),
		ExpressionAttributeValues: dynamoutils.Record{
			":versionToPut": dynamoutils.Number(int(snap.Cursor.Version())),
		},
	})
	if err != nil {
		if err, ok := err.(awserr.Error); ok && err.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
			return nil // not an error per se, since there was a newer version in DynamoDB
		} else {
			return err
		}
	}

	return nil
}
