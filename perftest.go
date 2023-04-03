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
	coll, cleanup := mustConnect()
	defer cleanup()

	tester := Tester{
		Coll: coll,
	}

	results, err := tester.Run()
	if err != nil {
		panic(err)
	}

	PrintTable(results)
}

func mustConnect() (*mongo.Collection, func()) {
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
	const dbName = "perftest"
	const collectionName = "perftest"

	db := client.Database(dbName)
	collection := db.Collection(collectionName)

	return collection, cleanup
}
