// Event Horizon client
package ehclient

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/function61/eventhorizon/pkg/dynamoutils"
	"github.com/function61/eventhorizon/pkg/ehevent"
	"path"
	"time"
)

// somewhat the same design as: https://stackoverflow.com/questions/55763006/dynamodb-event-store-on-aws

type Client struct {
	dynamo          *dynamodb.DynamoDB
	eventsTableName *string
}

// interface assertion
var _ ReaderWriter = (*Client)(nil)

func New(env Environment) *Client {
	sess, err := session.NewSession()
	if err != nil {
		panic(err)
	}

	dynamo := dynamodb.New(
		sess,
		aws.NewConfig().WithRegion(env.regionId))

	return &Client{dynamo, aws.String(env.events)}
}

// "lastKnown" is exclusive (i.e. the record pointed by it will not be returned)
func (e *Client) Read(ctx context.Context, lastKnown Cursor) (*ReadResult, error) {
	resp, err := e.dynamo.QueryWithContext(ctx, &dynamodb.QueryInput{
		TableName:              e.eventsTableName,
		Limit:                  aws.Int64(100), // I don't see other max in docs except 1 MB result set size
		KeyConditionExpression: aws.String("s = :s AND v > :v"),
		ExpressionAttributeValues: dynamoutils.Record{
			":s": dynamoutils.String(lastKnown.stream),
			":v": dynamoutils.Number(int(lastKnown.version)),
		},
	})
	if err != nil {
		return nil, err
	}

	lastVersion := lastKnown.version

	entries := []LogEntry{}

	// these are in chronological order
	for _, item := range resp.Items {
		entry := &LogEntry{}
		if err := dynamoutils.Unmarshal(item, entry); err != nil {
			return nil, err
		}

		lastVersion = entry.Version

		entries = append(entries, *entry)
	}

	// "If LastEvaluatedKey is not empty, it does not necessarily mean that there
	// is more data in the result set. The only way to know when you have reached
	// the end of the result set is when LastEvaluatedKey is empty."
	moreData := len(resp.LastEvaluatedKey) != 0

	lastEntryCursor := At(lastKnown.stream, lastVersion)

	return &ReadResult{entries, lastEntryCursor, moreData}, nil
}

func (e *Client) Append(ctx context.Context, stream string, events []string) error {
	// this can fail, so retry a few times
	for i := 0; i < 3; i++ {
		at, err := e.resolveStreamPosition(ctx, stream)
		if err != nil {
			return err
		}

		if err := e.AppendAt(ctx, *at, events); err != nil {
			if _, isAboutConcurrency := err.(*concurrencyError); isAboutConcurrency {
				continue
			} else {
				return err // some other error - don't even retry
			}
		}

		return nil
	}

	return fmt.Errorf("Append: retry times exceeded, stream=%s", stream)
}

// NOTE: be very sure that stream exists, since it is not validated
// NOTE: be sure that you don't set version into the future, since that will leave a gap
func (e *Client) AppendAt(ctx context.Context, after Cursor, events []string) error {
	dynamoEntry, err := dynamoutils.Marshal(LogEntry{
		Stream:  after.stream,
		Version: after.Next().version,
		Events:  events,
	})
	if err != nil {
		return err
	}

	_, err = e.dynamo.PutItemWithContext(ctx, &dynamodb.PutItemInput{
		TableName: e.eventsTableName,
		Item:      dynamoEntry,
		// http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html
		ConditionExpression: aws.String("attribute_not_exists(s) AND attribute_not_exists(v)"),
	})
	if err != nil {
		if err, ok := err.(awserr.Error); ok && err.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
			return &concurrencyError{err}
		} else {
			return err
		}
	}

	return nil
}

func (e *Client) CreateStream(ctx context.Context, parent string, name string) error {
	if parent == "" {
		return errors.New("parent stream name cannot be empty")
	}
	if name == "" {
		return errors.New("stream name cannot be empty")
	}

	streamPath := path.Join(parent, name)

	parentAt, err := e.resolveStreamPosition(ctx, parent)
	if err != nil {
		return err
	}

	now := time.Now()

	itemInParent, err := e.entryAsTxPut(metaEntry(NewChildStreamCreated(streamPath, ehevent.MetaSystemUser(now)), parentAt.Next()))
	if err != nil {
		return err
	}

	itemInChild, err := e.entryAsTxPut(streamCreationEntry(streamPath, parent, now))
	if err != nil {
		return err
	}

	_, err = e.dynamo.TransactWriteItemsWithContext(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []*dynamodb.TransactWriteItem{
			itemInParent,
			itemInChild,
		},
	})
	if err != nil {
		// TODO: retry if TransactionCanceledException
		return err
	}

	return nil
}

func (e *Client) resolveStreamPosition(
	ctx context.Context,
	stream string,
) (*Cursor, error) {
	// grab the most recent entry
	mostRecent, err := e.dynamo.QueryWithContext(ctx, &dynamodb.QueryInput{
		TableName:              e.eventsTableName,
		KeyConditionExpression: aws.String("s = :s"),
		ExpressionAttributeValues: dynamoutils.Record{
			":s": dynamoutils.String(stream),
		},
		Limit:            aws.Int64(1),
		ScanIndexForward: aws.Bool(false),
	})
	if err != nil {
		return nil, err
	}

	// existing stream should never be empty (b/c it always has the first "created" entry)
	if len(mostRecent.Items) == 0 {
		return nil, fmt.Errorf("resolveStreamPosition: '%s' does not seem to exist", stream)
	}

	en := &LogEntry{}
	if err := dynamoutils.Unmarshal(mostRecent.Items[0], en); err != nil {
		return nil, err
	}

	return &Cursor{stream, en.Version}, nil
}

func (e *Client) entryAsTxPut(item LogEntry) (*dynamodb.TransactWriteItem, error) {
	itemDynamo, err := dynamoutils.Marshal(item)
	if err != nil {
		return nil, err
	}

	return &dynamodb.TransactWriteItem{
		Put: &dynamodb.Put{
			TableName:           e.eventsTableName,
			Item:                itemDynamo,
			ConditionExpression: aws.String("attribute_not_exists(s) AND attribute_not_exists(v)"),
		},
	}, nil
}

func streamCreationEntry(stream string, parent string, now time.Time) LogEntry {
	return metaEntry(
		NewStreamStarted(parent, ehevent.MetaSystemUser(now)),
		At(stream, 0))
}

func metaEntry(metaEvent ehevent.Event, pos Cursor) LogEntry {
	return LogEntry{
		Stream:    pos.stream,
		Version:   pos.version,
		MetaEvent: aws.String(ehevent.Serialize(metaEvent)),
	}
}
