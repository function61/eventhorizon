// DynamoDB utils for hiding complexity
package dynamoutils

// TODO: put these in gokit?

import (
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"strconv"
)

// this shorthand type does not exist in AWS' library, even though this is rather common
type Record map[string]*dynamodb.AttributeValue

func EventImageToRecord(eventRecord map[string]events.DynamoDBAttributeValue) Record {
	ret := Record{}

	for key, valWrapper := range eventRecord {
		switch valWrapper.DataType() {
		case events.DataTypeString:
			ret[key] = String(valWrapper.String())
		case events.DataTypeNumber:
			ret[key] = NumberFromString(valWrapper.String())
		default:
			panic("unsupported datatype")
		}
	}

	return ret
}

func String(value string) *dynamodb.AttributeValue {
	return &dynamodb.AttributeValue{
		S: aws.String(value),
	}
}

func Number(input int) *dynamodb.AttributeValue {
	return NumberFromString(strconv.Itoa(input))
}

func NumberFromString(input string) *dynamodb.AttributeValue {
	return &dynamodb.AttributeValue{
		N: aws.String(input),
	}
}

func Unmarshal(record Record, to interface{}) error {
	return dynamodbattribute.UnmarshalMap(record, to)
}

func Marshal(obj interface{}) (Record, error) {
	return dynamodbattribute.MarshalMap(obj)
}
