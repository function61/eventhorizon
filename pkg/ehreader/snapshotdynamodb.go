package ehreader

import (
	"context"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehserver/ehdynamodb"
	"github.com/function61/gokit/aws/dynamoutils"
)

type DynamoSnapshotItem struct {
	Stream  string `json:"s"` // stream + context form the composite key
	Context string `json:"c"` // different software can read the same stream,
	Version int64  `json:"v"` // we conditionally put updates into DynamoDB as not to overwrite advanced state
	Data    []byte `json:"d"` // actual snapshot data
}

type dynamoSnapshotStorage struct {
	dynamo             *dynamodb.DynamoDB
	snapshotsTableName *string
}

func NewDynamoDbSnapshotStore(opts ehdynamodb.DynamoDbOptions) (eh.SnapshotStore, error) {
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
		dynamo:             dynamo,
		snapshotsTableName: &opts.TableName,
	}, nil
}

func (d *dynamoSnapshotStorage) ReadSnapshot(
	ctx context.Context,
	stream eh.StreamName,
	snapshotContext string,
) (*eh.Snapshot, error) {
	getResponse, err := d.dynamo.GetItemWithContext(ctx, &dynamodb.GetItemInput{
		Key: dynamoutils.Record{
			"s": dynamoutils.String(stream.String()),
			"c": dynamoutils.String(snapshotContext),
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

	streamName, err := eh.DeserializeStreamName(dynamoSnapshot.Stream)
	if err != nil {
		return nil, err
	}

	cursorInSnapshot := streamName.At(dynamoSnapshot.Version)

	return eh.NewSnapshot(cursorInSnapshot, dynamoSnapshot.Data, snapshotContext), nil
}

func (d *dynamoSnapshotStorage) WriteSnapshot(ctx context.Context, snap eh.Snapshot) error {
	dynamoSnapshot, err := dynamoutils.Marshal(DynamoSnapshotItem{
		Stream:  snap.Cursor.Stream().String(),
		Context: snap.Context,
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

func (d *dynamoSnapshotStorage) DeleteSnapshot(
	ctx context.Context,
	stream eh.StreamName,
	snapshotContext string,
) error {
	_, err := d.dynamo.DeleteItemWithContext(ctx, &dynamodb.DeleteItemInput{
		TableName:           d.snapshotsTableName,
		ConditionExpression: aws.String("attribute_exists(s)"), // without this we don't get error if item does not exist
		Key: dynamoutils.Record{
			"s": dynamoutils.String(stream.String()),
			"c": dynamoutils.String(snapshotContext),
		},
	})
	if err != nil {
		if err, ok := err.(awserr.Error); ok && err.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
			return os.ErrNotExist
		} else {
			return err
		}
	}

	return nil
}
