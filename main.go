package main

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/khalifa-is/datademon"
	"log"
	"path/filepath"
	"time"
)

func main() {
	defer timeFxn(time.Now(), "main fxn")

	cfg, err := getAwsConfig()
	if err != nil {
		log.Fatalf("Unable to load SDK config, %v", err)
	}

	dynamoClient := dynamodb.NewFromConfig(cfg)

	configJson, err := getJsonConfig()
	if err != nil {
		log.Fatal(err)
	}

	// -2021-08-01-part1_6.zip

	chDataLink := configJson["chDataLink"].(string)
	zipFilePath := filepath.Join("data", configJson["zipFileName"].(string))

	_, err = datademon.DownloadZipFile(chDataLink, zipFilePath)
	if err != nil {
		log.Fatal(err)
	}

	files, err := datademon.Unzip(zipFilePath, "data")
	if err != nil {
		log.Fatal(err)
	}

	records, err := datademon.ReadCsvFile(files[0])
	if err != nil {
		log.Fatal(err)
	}

	var errors []map[string]interface{}
	datademon.ParseCsv(records, func(index int, record []string) bool {
		if record[0] == "CompanyName" {
			return false
		}

		company := buildCompany(record)

		av, err := attributevalue.MarshalMap(company)
		if err != nil {
			errors = append(errors, map[string]interface{}{
				"error":   err,
				"company": company,
			})
		}

		companyInput := &dynamodb.PutItemInput{
			Item:      av,
			TableName: aws.String("Companies"),
		}

		_, err = dynamoClient.PutItem(context.TODO(), companyInput)
		if err != nil {
			errors = append(errors, map[string]interface{}{
				"error":   err,
				"company": company,
			})
		}
		if index == 5 {
			return true
		}

		return false
	})

	cleanupDataDirectory()
}
