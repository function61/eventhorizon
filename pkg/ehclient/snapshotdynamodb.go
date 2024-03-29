package ehclient

import (
	"context"
	"os"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehserver/ehdynamodb"
	"github.com/function61/gokit/app/aws/dynamoutils"
)

type SnapshotReference struct {
	Stream      eh.StreamName
	Perspective eh.SnapshotPerspective
}

type SnapshotVersion struct {
	Snapshot SnapshotReference
	Version  int64
}

type DynamoSnapshotItem struct {
	Stream      string `json:"s"` // stream + perspective form the composite key
	Perspective string `json:"c"` // different software can have different perspective for the same stream
	Version     int64  `json:"v"` // we conditionally put updates into DynamoDB as not to overwrite advanced state
	RawData     []byte `json:"d"` // actual snapshot data, probably encrypted
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
		opts.AccessKeyToken)

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
	input eh.ReadSnapshotInput,
) (*eh.ReadSnapshotOutput, error) {
	getResponse, err := d.dynamo.GetItemWithContext(ctx, &dynamodb.GetItemInput{
		Key: dynamoutils.Record{
			"s": dynamoutils.String(input.Stream.String()),
			"c": dynamoutils.String(input.Perspective.String()),
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

	return &eh.ReadSnapshotOutput{
		Snapshot: &eh.PersistedSnapshot{
			Cursor:      streamName.At(dynamoSnapshot.Version),
			RawData:     dynamoSnapshot.RawData,
			Perspective: eh.ParseSnapshotPerspective(dynamoSnapshot.Perspective),
		},
	}, nil
}

func (d *dynamoSnapshotStorage) WriteSnapshot(ctx context.Context, snap eh.PersistedSnapshot) error {
	dynamoSnapshot, err := dynamoutils.Marshal(DynamoSnapshotItem{
		Stream:      snap.Cursor.Stream().String(),
		Perspective: snap.Perspective.String(),
		Version:     snap.Cursor.Version(),
		RawData:     snap.RawData,
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
	perspective eh.SnapshotPerspective,
) error {
	_, err := d.dynamo.DeleteItemWithContext(ctx, &dynamodb.DeleteItemInput{
		TableName:           d.snapshotsTableName,
		ConditionExpression: aws.String("attribute_exists(s)"), // without this we don't get error if item does not exist
		Key: dynamoutils.Record{
			"s": dynamoutils.String(stream.String()),
			"c": dynamoutils.String(perspective.String()),
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

// mainly useful for debugging
func (d *dynamoSnapshotStorage) ListSnapshotsForStream(ctx context.Context, stream eh.StreamName) ([]SnapshotVersion, error) {
	contextNames, err := d.dynamo.QueryWithContext(ctx, &dynamodb.QueryInput{
		TableName:              d.snapshotsTableName,
		KeyConditionExpression: aws.String("s = :s"),
		ExpressionAttributeValues: dynamoutils.Record{
			":s": dynamoutils.String(stream.String()),
		},
		ProjectionExpression: aws.String("c,v"), // only fetch context and version (= don't bother fetching data)
	})
	if err != nil {
		return nil, err
	}

	names := []SnapshotVersion{}

	for _, item := range contextNames.Items {
		ver, err := strconv.ParseInt(*item["v"].N, 10, 64) // DynamoDB network APIs send numbers as string
		if err != nil {
			return nil, err
		}

		names = append(names, SnapshotVersion{
			Snapshot: SnapshotReference{
				Stream:      stream,
				Perspective: eh.ParseSnapshotPerspective(*item["c"].S),
			},
			Version: ver,
		})
	}

	return names, nil
}
