package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
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

	// Step 1: Make API call with header
	client := &http.Client{Timeout: time.Second * 30} // 30-second timeout
	req, err := http.NewRequest("GET", apiUrl, nil)
	if err != nil {
		return err
	}
	req.Header.Set("x-api-key", apiKey)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var claims []models.Claim
	if err := json.NewDecoder(resp.Body).Decode(&claims); err != nil {
		return err
	}

	// Step 3: Send to SQS
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return err
	}
	sqsClient := sqs.NewFromConfig(cfg)

	for _, claim := range claims {
		msg, err := json.Marshal(claim)
		if err != nil {
			continue // log the error
		}
		_, err = sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:    aws.String(os.Getenv("SQS_QUEUE_URL")),
			MessageBody: aws.String(string(msg)),
		})
		if err != nil {
			continue // log the error
		}
	}

	return nil
}
