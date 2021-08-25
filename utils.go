package main

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/spf13/viper"
	"io/ioutil"
	"log"
	"os"
	"path"
	"time"
)

func timeFxn(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %s", name, elapsed)
}

func cleanupDataDirectory() {
	dir, err := ioutil.ReadDir("data")
	if err != nil {
		log.Fatal(err)
	}
	for _, d := range dir {
		err := os.RemoveAll(path.Join([]string{"data", d.Name()}...))
		if err != nil {
			log.Fatalf("Clean up failed: %v", err)
		}
	}
}

func getEnvVar(key string) string {
	viper.SetConfigFile(".env")

	err := viper.ReadInConfig()

	if err != nil {
		log.Fatalf("Error while reading config file %s", err)

	}

	value, ok := viper.Get(key).(string)

	if !ok {
		log.Fatal("Invalid type assertion")
	}

	return value
}

func getAwsConfig() (cfg aws.Config, err error) {
	ctx := context.TODO()
	if os.Args[len(os.Args)-1] == "-d" {
		return config.LoadDefaultConfig(ctx, config.WithSharedConfigProfile(getEnvVar("AWS_PROFILE")))
	}
	return config.LoadDefaultConfig(ctx)
}

func getJsonConfig() (map[string]interface{}, error) {
	jsonFile, err := os.Open(getEnvVar("CONFIG_JSON"))
	if err != nil {
		return nil, err
	}

	defer func(jsonFile *os.File) {
		err = jsonFile.Close()
	}(jsonFile)

	if err != nil {
		return nil, err
	}

	byteValue, _ := ioutil.ReadAll(jsonFile)

	var configJson map[string]interface{}
	err = json.Unmarshal(byteValue, &configJson)

	if err != nil {
		return nil, err
	}

	return configJson, nil
}

func parseCsvCallback(index int, record []string) (bool, error) {
	terminate := false
	if index == 5 {
		terminate = true
	}

	company := buildCompany(record)

	av, err := attributevalue.MarshalMap(company)
	if err != nil {
		return terminate, err
	}

	companyInput := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String("Companies"),
	}

	_, err = dynamoClient.PutItem(context.TODO(), companyInput)
	if err != nil {
		return terminate, nil
	}

	if index == 5 {
		return terminate, nil
	}

	return terminate, nil
}
