// Event log storage in AWS DynamoDB
package ehdynamodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/function61/eventhorizon/pkg/eh"
	"github.com/function61/eventhorizon/pkg/ehevent"
	"github.com/function61/eventhorizon/pkg/envelopeenc"
	"github.com/function61/gokit/aws/dynamoutils"
)

// somewhat the same design as: https://stackoverflow.com/questions/55763006/dynamodb-event-store-on-aws

// Raw entry from DynamoDB
// - can contain 0-n events. commit all events in a single transaction.
// - might contain a single meta event
// - why most common attribute names shortened? DynamoDB charges for each byte in item attribute names..
// - we have JSON marshalling defined but please consider it DynamoDB internal implementation
type LogEntryRaw struct {
	Stream      string `json:"s"` // stream + version form the composite key
	Version     int64  `json:"v"`
	KindAndData []byte `json:"d"` // first byte is eh.LogDataKind, the rest is data (combined to save space)
}

type DynamoDbOptions struct {
	AccessKeyId     string // AWS_ACCESS_KEY_ID
	AccessKeySecret string // AWS_SECRET_ACCESS_KEY
	AccessKeyToken  string // AWS_SESSION_TOKEN (only needed in Lambda)
	RegionId        string
	TableName       string
}

type Client struct {
	dynamo          *dynamodb.DynamoDB
	eventsTableName *string
}

// interface assertion
var _ eh.ReaderWriter = (*Client)(nil)

func New(opts DynamoDbOptions) *Client {
	sess, err := session.NewSession()
	if err != nil {
		panic(err)
	}

	staticCreds := credentials.NewStaticCredentials(
		opts.AccessKeyId,
		opts.AccessKeySecret,
		opts.AccessKeyToken)

	dynamo := dynamodb.New(
		sess,
		aws.NewConfig().WithCredentials(staticCreds).WithRegion(opts.RegionId))

	return &Client{dynamo, &opts.TableName}
}

// "lastKnown" is exclusive (i.e. the record pointed by it will not be returned)
func (e *Client) Read(ctx context.Context, lastKnown eh.Cursor) (*eh.ReadResult, error) {
	resp, err := e.dynamo.QueryWithContext(ctx, &dynamodb.QueryInput{
		TableName:              e.eventsTableName,
		Limit:                  aws.Int64(100), // I don't see other max in docs except 1 MB result set size
		KeyConditionExpression: aws.String("s = :s AND v > :v"),
		ExpressionAttributeValues: dynamoutils.Record{
			":s": dynamoutils.String(lastKnown.Stream().String()),
			":v": dynamoutils.Number(int(lastKnown.Version())),
		},
	})
	if err != nil {
		return nil, err
	}

	// each stream always has at least StreamStarted event, so if we start from beginning
	// and don't get any entries at all, it means that stream doesn't exist
	if lastKnown.Version() < 0 && len(resp.Items) == 0 {
		return nil, fmt.Errorf("Read: non-existent stream: %s", lastKnown.Stream().String())
	}

	lastVersion := lastKnown.Version()

	entries := []eh.LogEntry{}

	// these are in chronological order
	for _, item := range resp.Items {
		entryRaw := LogEntryRaw{}
		if err := dynamoutils.Unmarshal(item, &entryRaw); err != nil {
			return nil, err
		}

		lastVersion = entryRaw.Version

		entries = append(entries, unmarshalLogEntryRaw(entryRaw))
	}

	// "If LastEvaluatedKey is not empty, it does not necessarily mean that there
	// is more data in the result set. The only way to know when you have reached
	// the end of the result set is when LastEvaluatedKey is empty."
	moreData := len(resp.LastEvaluatedKey) != 0

	lastEntryCursor := lastKnown.Stream().At(lastVersion)

	return &eh.ReadResult{entries, lastEntryCursor, moreData}, nil
}

func (e *Client) Append(ctx context.Context, stream eh.StreamName, data eh.LogData) (*eh.AppendResult, error) {
	// this can fail, so retry a few times
	for i := 0; i < 3; i++ {
		at, err := e.resolveStreamPosition(ctx, stream)
		if err != nil {
			return nil, err
		}

		result, err := e.AppendAfter(ctx, *at, data)
		if err != nil {
			// I think this is a false positive lint message:
			//     "when isAboutConcurrency is true, err can't be nil"
			//nolint:gosimple
			if _, isAboutConcurrency := err.(*eh.ErrOptimisticLockingFailed); isAboutConcurrency {
				continue
			} else {
				return nil, err // some other error - don't even retry
			}
		}

		return result, nil
	}

	return nil, fmt.Errorf("Append: retry times exceeded, stream=%s", stream)
}

// NOTE: be very sure that stream exists, since it is not validated (only happens if malicious Cursor provided)
// NOTE: be sure that you don't set version into the future, since that will leave a gap
// NOTE: returned error is *ErrOptimisticLockingFailed if stream had writes
func (e *Client) AppendAfter(ctx context.Context, after eh.Cursor, data eh.LogData) (*eh.AppendResult, error) {
	resultingCursor := after.Next()

	if resultingCursor.Version() == 0 {
		// usually an indication of trying to append to a stream that either doesn't exist,
		// or its state has not been examined - which is conflicting since AppendAfter() by
		// definition is state-aware.
		return nil, errors.New("AppendAfter: refusing @0, since stream should start with StreamStarted")
	}

	logEntryInDynamo, err := dynamoutils.Marshal(mkLogEntryRaw(resultingCursor, data))
	if err != nil {
		return nil, err
	}

	_, err = e.dynamo.PutItemWithContext(ctx, &dynamodb.PutItemInput{
		TableName: e.eventsTableName,
		Item:      logEntryInDynamo,
		// http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html
		ConditionExpression: aws.String("attribute_not_exists(s) AND attribute_not_exists(v)"),
	})
	if err != nil {
		if err, ok := err.(awserr.Error); ok && err.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
			return nil, eh.NewErrOptimisticLockingFailed(err)
		} else {
			return nil, err
		}
	}

	return &eh.AppendResult{
		Cursor: resultingCursor,
	}, nil
}

func (e *Client) CreateStream(
	ctx context.Context,
	stream eh.StreamName,
	dekEnvelope envelopeenc.Envelope,
	initialData *eh.LogData,
) (*eh.AppendResult, error) {
	parent := stream.Parent()
	if parent == nil {
		return nil, errors.New("cannot create root stream")
	}

	parentAt, err := e.resolveStreamPosition(ctx, *parent)
	if err != nil {
		return nil, err
	}

	now := time.Now()

	itemInParent, err := e.entryAsTxPut(metaEntry(eh.NewStreamChildStreamCreated(stream.String(), ehevent.MetaSystemUser(now)), parentAt.Next()))
	if err != nil {
		return nil, err
	}

	itemInChild, err := e.entryAsTxPut(streamCreationEntry(stream, dekEnvelope, now))
	if err != nil {
		return nil, err
	}

	items := []*dynamodb.TransactWriteItem{
		itemInParent,
		itemInChild,
	}

	if initialData != nil {
		itemInitialEvents, err := e.entryAsTxPut(mkLogEntryRaw(stream.At(1), *initialData))
		if err != nil {
			return nil, err
		}

		items = append(items, itemInitialEvents)
	}

	resultingCursor := func() eh.Cursor {
		if initialData != nil {
			return stream.At(1)
		} else {
			return stream.At(0)
		}
	}()

	_, err = e.dynamo.TransactWriteItemsWithContext(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: items,
	})
	if err != nil {
		// TODO: retry if TransactionCanceledException
		return nil, err
	}

	return &eh.AppendResult{
		Cursor: resultingCursor,
	}, nil
}

func (e *Client) resolveStreamPosition(
	ctx context.Context,
	stream eh.StreamName,
) (*eh.Cursor, error) {
	// grab the most recent entry
	mostRecent, err := e.dynamo.QueryWithContext(ctx, &dynamodb.QueryInput{
		TableName:              e.eventsTableName,
		KeyConditionExpression: aws.String("s = :s"),
		ExpressionAttributeValues: dynamoutils.Record{
			":s": dynamoutils.String(stream.String()),
		},
		Limit:            aws.Int64(1),
		ScanIndexForward: aws.Bool(false),
	})
	if err != nil {
		return nil, err
	}

	// existing stream should never be empty (b/c it always has the first "created" entry)
	if len(mostRecent.Items) == 0 {
		return nil, fmt.Errorf("resolveStreamPosition: '%s' does not seem to exist", stream.String())
	}

	en := &LogEntryRaw{}
	if err := dynamoutils.Unmarshal(mostRecent.Items[0], en); err != nil {
		return nil, err
	}

	cur := stream.At(en.Version)
	return &cur, nil
}

func (e *Client) entryAsTxPut(item LogEntryRaw) (*dynamodb.TransactWriteItem, error) {
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

func streamCreationEntry(stream eh.StreamName, dekEnvelope envelopeenc.Envelope, now time.Time) LogEntryRaw {
	return metaEntry(
		eh.NewStreamStarted(dekEnvelope, ehevent.MetaSystemUser(now)),
		stream.At(0))
}

func metaEntry(metaEvent ehevent.Event, pos eh.Cursor) LogEntryRaw {
	return mkLogEntryRaw(pos, *eh.LogDataMeta(metaEvent))
}

func mkLogEntryRaw(cursor eh.Cursor, data eh.LogData) LogEntryRaw {
	return LogEntryRaw{
		Stream:      cursor.Stream().String(),
		Version:     cursor.Version(),
		KindAndData: append([]byte{byte(data.Kind)}, data.Raw...),
	}
}

func unmarshalLogEntryRaw(entry LogEntryRaw) eh.LogEntry {
	stream, err := eh.DeserializeStreamName(entry.Stream)
	if err != nil {
		panic(err) // shouldn't happen, because data saved in DB is validated
	}

	return eh.LogEntry{
		Cursor: stream.At(entry.Version),
		Data: eh.LogData{
			Kind: eh.LogDataKind(entry.KindAndData[0]),
			Raw:  entry.KindAndData[1:],
		},
	}
}
