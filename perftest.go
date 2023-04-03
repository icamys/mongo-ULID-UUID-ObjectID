package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	mongooptions "go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func main() {
	client, cleanup := mustConnectToDb()
	defer cleanup()

	results := runTests(client)

	fmt.Println(results)
}

type testResults struct{}

func (r *testResults) String() string {
	return ""
}

func runTests(client *mongo.Client) *testResults {
	results := new(testResults)

	return results
}

func mustConnectToDb() (*mongo.Client, func()) {
	const timeout = 1 * time.Second
	ctx := context.Background()
	uri := os.Getenv("MONGO_URI")

	connCtx, connCancel := context.WithTimeout(ctx, timeout)
	defer connCancel()
	connOptions := mongooptions.Client().ApplyURI(uri)

	client, err := mongo.Connect(connCtx, connOptions)
	if err != nil {
		panic(fmt.Errorf("connection failed: %w", err))
	}

	pingCtx, pingCancel := context.WithTimeout(ctx, timeout)
	defer pingCancel()
	if err = client.Ping(pingCtx, readpref.Primary()); err != nil {
		panic(fmt.Errorf("connection failed: %w", err))
	}

	cleanup := func() {
		if err = client.Disconnect(ctx); err != nil {
			panic(err)
		}
	}

	return client, cleanup
}
