package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/jasonmichels/cf-utils/models"
)

func main() {
	lambda.Start(HandleRequest)
}

func HandleRequest(ctx context.Context) error {
	// Retrieve URL and API Key from environment variables
	apiUrl := os.Getenv("API_URL")
	apiKey := os.Getenv("API_KEY")

	claims, err := fetchClaims(ctx, apiUrl, apiKey)
	if err != nil {
		log.Printf("Error fetching claims: %v\n", err)
		return err
	}

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Printf("Error loading AWS SDK config: %v\n", err)
		return err
	}

	dynamoClient := dynamodb.NewFromConfig(cfg)
	sqsClient := sqs.NewFromConfig(cfg)

	for _, claim := range claims {
		shouldPublish, err := shouldPublishClaim(ctx, dynamoClient, claim)
		if err != nil {
			log.Printf("Error processing claim ID %s: %v\n", claim.ClaimID, err)
			continue
		}

		if shouldPublish {
			sendClaimToSQS(ctx, sqsClient, claim)
		}

	}

	return nil
}

func fetchClaims(ctx context.Context, apiUrl, apiKey string) ([]models.Claim, error) {
	client := &http.Client{Timeout: time.Second * 30} // 30-second timeout
	req, err := http.NewRequest("GET", apiUrl, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("x-api-key", apiKey)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var claims []models.Claim
	if err := json.NewDecoder(resp.Body).Decode(&claims); err != nil {
		return nil, err
	}

	return claims, nil
}

func shouldPublishClaim(ctx context.Context, dynamoClient *dynamodb.Client, claim models.Claim) (bool, error) {
	// Define the table name and the claim ID you're querying for
	tableName := os.Getenv("EMAILS_DYNAMODB_TABLE_NAME")
	claimId := claim.ClaimID

	// Query DynamoDB for records matching the claimId
	resp, err := dynamoClient.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("claimId = :claimId"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":claimId": &types.AttributeValueMemberS{Value: claimId},
		},
	})
	if err != nil {
		return false, err
	}

	// Determine action based on the number of records found
	now := time.Now()
	switch len(resp.Items) {
	case 0:
		return true, nil
	case 1:
		// One email has been sent, check if it's been a week
		dateSentStr, exists := resp.Items[0]["dateSent"].(*types.AttributeValueMemberS)
		if !exists {
			return false, fmt.Errorf("dateSent attribute missing or not a string for claimId: %s", claimId)
		}
		dateSent, err := time.Parse(time.RFC3339, dateSentStr.Value)
		if err != nil {
			return false, fmt.Errorf("error parsing dateSent for claimId: %s: %v", claimId, err)
		}
		if now.Sub(dateSent).Hours() >= 168 { // 168 hours in a week
			return true, nil
		}
		// Less than a week since the first email, do not send
		return false, nil
	default:
		// Two emails have already been sent, do not send again
		return false, nil
	}
}

// This function sends the claim to SQS if the conditions are met.
func sendClaimToSQS(ctx context.Context, sqsClient *sqs.Client, claim models.Claim) {
	// Your existing logic to send a message to SQS
	msg, err := json.Marshal(claim)
	if err != nil {
		log.Printf("Error marshalling claim with ID %s: %v\n", claim.ClaimID, err)
		return
	}
	_, err = sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(os.Getenv("SQS_QUEUE_URL")),
		MessageBody: aws.String(string(msg)),
	})

	if err != nil {
		log.Printf("Error sending claim with ID %s to SQS: %v\n", claim.ClaimID, err)
		return
	} else {
		log.Printf("Successfully sent claim to SQS - ID: %s, Number: %s\n", claim.ClaimID, claim.ClaimNumber)
		return
	}
}
