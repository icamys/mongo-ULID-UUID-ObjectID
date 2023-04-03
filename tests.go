package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type mongoDocumentULID struct {
	ID ulid.ULID `bson:"_id"`
}
type mongoDocumentObjectID struct {
	ID primitive.ObjectID `bson:"_id"`
}

type TesterResults struct {
	InsertsBatched1M1K  *InsertBatchesTestResult
	InsertsBatched1M5K  *InsertBatchesTestResult
	InsertsBatched1M10K *InsertBatchesTestResult
	Insert1M            *InsertTestResult
}

func PrintTable(r *TesterResults) {
	var header = []string{"", "ObjectId", "ULID", "% perf diff"}
	var data [][]string

	data = append(data,
		[]string{
			"1M inserts batched, batch size = 1k",
			r.InsertsBatched1M1K.ObjectIDDuration.Round(1 * time.Millisecond).String(),
			r.InsertsBatched1M1K.ULIDDuration.Round(1 * time.Millisecond).String(),
			fmt.Sprintf("%.2f%%", calcTimeDiffPercent(
				r.InsertsBatched1M1K.ObjectIDDuration, r.InsertsBatched1M1K.ULIDDuration)),
		},
		[]string{
			"1M inserts batched, batch size = 5k",
			r.InsertsBatched1M5K.ObjectIDDuration.Round(1 * time.Millisecond).String(),
			r.InsertsBatched1M5K.ULIDDuration.Round(1 * time.Millisecond).String(),
			fmt.Sprintf("%.2f%%", calcTimeDiffPercent(
				r.InsertsBatched1M5K.ObjectIDDuration, r.InsertsBatched1M5K.ULIDDuration)),
		},
		[]string{
			"1M inserts batched, batch size = 10k",
			r.InsertsBatched1M10K.ObjectIDDuration.Round(1 * time.Millisecond).String(),
			r.InsertsBatched1M10K.ULIDDuration.Round(1 * time.Millisecond).String(),
			fmt.Sprintf("%.2f%%", calcTimeDiffPercent(
				r.InsertsBatched1M10K.ObjectIDDuration, r.InsertsBatched1M10K.ULIDDuration)),
		},
		[]string{
			"1M inserts",
			r.Insert1M.ObjectIDDuration.Round(1 * time.Millisecond).String(),
			r.Insert1M.ULIDDuration.Round(1 * time.Millisecond).String(),
			fmt.Sprintf("%.2f%%", calcTimeDiffPercent(
				r.Insert1M.ObjectIDDuration, r.Insert1M.ULIDDuration)),
		},
	)

	fmt.Println(
		"|", fmt.Sprintf("%-45s", header[0]),
		"|", fmt.Sprintf("%-10s", header[1]),
		"|", fmt.Sprintf("%-22s", header[2]),
		"|", fmt.Sprintf("%-12s", header[3]),
		"|")
	fmt.Println(
		"|", strings.Repeat("-", 45),
		"|", strings.Repeat("-", 10),
		"|", strings.Repeat("-", 22),
		"|", strings.Repeat("-", 12),
		"|")

	for _, row := range data {
		fmt.Println(
			"|", fmt.Sprintf("%-45s", row[0]),
			"|", fmt.Sprintf("%-10s", row[1]),
			"|", fmt.Sprintf("%-22s", row[2]),
			"|", fmt.Sprintf("%-12s", row[3]),
			"|")
	}
}

func calcTimeDiffPercent(v1, v2 time.Duration) float64 {
	var k float64 = 1
	if v2 > v1 {
		k = -1
	}

	return k * (float64(v2.Microseconds()) - float64(v1.Microseconds())) * 100 / float64(v1.Microseconds())
}

type Tester struct {
	Coll *mongo.Collection
}

func (t *Tester) Run() (*TesterResults, error) {
	var err error
	results := new(TesterResults)

	const (
		OneMillion   = 1000000
		OneThousand  = 1000
		FiveThousand = 5000
		TenThousand  = 10000
	)

	results.InsertsBatched1M1K, err = t.testInsertsBatched(OneMillion, OneThousand)
	if err != nil {
		return nil, fmt.Errorf("failed to run insert batches test: %w", err)
	}

	results.InsertsBatched1M5K, err = t.testInsertsBatched(OneMillion, FiveThousand)
	if err != nil {
		return nil, fmt.Errorf("failed to run insert batches test: %w", err)
	}

	results.InsertsBatched1M10K, err = t.testInsertsBatched(OneMillion, TenThousand)
	if err != nil {
		return nil, fmt.Errorf("failed to run insert batches test: %w", err)
	}

	results.Insert1M, err = t.testInserts(OneMillion)
	if err != nil {
		return nil, fmt.Errorf("failed to run inserts test: %w", err)
	}

	return results, nil
}

type InsertBatchesTestResult struct {
	ULIDDuration     time.Duration
	ObjectIDDuration time.Duration
}

func (t *Tester) testInsertsBatched(totalDocs, batchSize int) (*InsertBatchesTestResult, error) {
	var start time.Time

	result := new(InsertBatchesTestResult)

	{
		start = time.Now()
		if err := t.insertDocumentsInBatches(batchSize, generateDocsUlid(totalDocs)); err != nil {
			return nil, fmt.Errorf("error on insert documents in batches test run: %w", err)
		}
		result.ULIDDuration = time.Now().Sub(start)
		if err := t.cleanCollection(); err != nil {
			return nil, fmt.Errorf("collection cleanup error: %w", err)
		}
	}

	{
		start = time.Now()
		if err := t.insertDocumentsInBatches(batchSize, generateDocsObjectID(totalDocs)); err != nil {
			return nil, fmt.Errorf("error on insert documents in batches test run: %w", err)
		}
		result.ObjectIDDuration = time.Now().Sub(start)
		if err := t.cleanCollection(); err != nil {
			return nil, fmt.Errorf("collection cleanup error: %w", err)
		}
	}

	return result, nil
}

type InsertTestResult struct {
	ULIDDuration     time.Duration
	ObjectIDDuration time.Duration
}

func (t *Tester) testInserts(totalDocs int) (*InsertTestResult, error) {
	var start time.Time

	result := new(InsertTestResult)

	{
		start = time.Now()
		if err := t.insertDocuments(generateDocsUlid(totalDocs)); err != nil {
			return nil, fmt.Errorf("error on insert documents test run: %w", err)
		}
		result.ULIDDuration = time.Now().Sub(start)
		if err := t.cleanCollection(); err != nil {
			return nil, fmt.Errorf("collection cleanup error: %w", err)
		}
	}

	{
		start = time.Now()
		if err := t.insertDocuments(generateDocsObjectID(totalDocs)); err != nil {
			return nil, fmt.Errorf("error on insert documents test run: %w", err)
		}
		result.ObjectIDDuration = time.Now().Sub(start)
		if err := t.cleanCollection(); err != nil {
			return nil, fmt.Errorf("collection cleanup error: %w", err)
		}
	}

	return result, nil
}

func (t *Tester) insertDocumentsInBatches(batchSize int, docs []interface{}) error {

	totalDocs := len(docs)

	var end int
	for start := 0; start < totalDocs; start += batchSize {
		if (start + batchSize) > totalDocs {
			end = totalDocs
		} else {
			end = start + batchSize
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := t.Coll.InsertMany(ctx, docs[start:end])
		cancel()
		if err != nil {
			return fmt.Errorf("error inserting documents in batch: %w", err)
		}
	}
	return nil
}

func (t *Tester) insertDocuments(docs []interface{}) error {
	totalDocs := len(docs)

	for i := 0; i < totalDocs; i += 1 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := t.Coll.InsertOne(ctx, docs[i])
		cancel()
		if err != nil {
			return fmt.Errorf("error inserting document: %w", err)
		}
	}
	return nil
}

func (t *Tester) cleanCollection() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := t.Coll.DeleteMany(ctx, bson.M{}); err != nil {
		return fmt.Errorf("failed tp delete docs: %w", err)
	}
	return nil
}

func generateDocsUlid(n int) []interface{} {
	result := make([]interface{}, n)
	for i := 0; i < n; i++ {
		result[i] = mongoDocumentULID{
			ID: ulid.Make(),
		}
	}
	return result
}

func generateDocsObjectID(n int) []interface{} {
	result := make([]interface{}, n)
	for i := 0; i < n; i++ {
		result[i] = mongoDocumentObjectID{
			ID: primitive.NewObjectID(),
		}
	}
	return result
}
