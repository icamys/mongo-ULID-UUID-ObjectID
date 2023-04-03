package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
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
	InsertsBatched1M1K        *InsertBatchesTestResult
	InsertsBatched1M5K        *InsertBatchesTestResult
	InsertsBatched1M10K       *InsertBatchesTestResult
	Insert1M                  *InsertTestResult
	InsertsBatchedPres10M10K  *InsertBatchesWithPresentTestResult
	InsertsBatchedPres10M100K *InsertBatchesWithPresentTestResult
}

func PrintTable(r *TesterResults) {
	var header = []string{"Test case", "ObjectId", "ULID", "% perf diff"}
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
		[]string{
			"10M inserts batched, 10M documents already present, batch size = 10k",
			r.InsertsBatchedPres10M10K.ObjectIDDuration.Round(1 * time.Millisecond).String(),
			r.InsertsBatchedPres10M10K.ULIDDuration.Round(1 * time.Millisecond).String(),
			fmt.Sprintf("%.2f%%", calcTimeDiffPercent(
				r.InsertsBatchedPres10M10K.ObjectIDDuration, r.InsertsBatchedPres10M10K.ULIDDuration)),
		},
		[]string{
			"10M inserts batched, 10M documents already present, batch size = 100k",
			r.InsertsBatchedPres10M100K.ObjectIDDuration.Round(1 * time.Millisecond).String(),
			r.InsertsBatchedPres10M100K.ULIDDuration.Round(1 * time.Millisecond).String(),
			fmt.Sprintf("%.2f%%", calcTimeDiffPercent(
				r.InsertsBatchedPres10M100K.ObjectIDDuration, r.InsertsBatchedPres10M100K.ULIDDuration)),
		},
	)

	fmt.Println(
		"|", fmt.Sprintf("%-70s", header[0]),
		"|", fmt.Sprintf("%-10s", header[1]),
		"|", fmt.Sprintf("%-22s", header[2]),
		"|", fmt.Sprintf("%-12s", header[3]),
		"|")
	fmt.Println(
		"|", strings.Repeat("-", 70),
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
		OneMillion      = 1000000
		TenMillion      = 10 * OneMillion
		OneThousand     = 1000
		FiveThousand    = 5 * OneThousand
		TenThousand     = 10 * OneThousand
		HundredThousand = 100 * OneThousand
	)

	results.InsertsBatched1M1K, err = t.testInsertBatches(OneMillion, OneThousand)
	if err != nil {
		return nil, fmt.Errorf("failed to run insert batches test: %w", err)
	}

	results.InsertsBatched1M5K, err = t.testInsertBatches(OneMillion, FiveThousand)
	if err != nil {
		return nil, fmt.Errorf("failed to run insert batches test: %w", err)
	}

	results.InsertsBatched1M10K, err = t.testInsertBatches(OneMillion, TenThousand)
	if err != nil {
		return nil, fmt.Errorf("failed to run insert batches test: %w", err)
	}

	results.Insert1M, err = t.testInserts(OneMillion)
	if err != nil {
		return nil, fmt.Errorf("failed to run inserts test: %w", err)
	}

	results.InsertsBatchedPres10M10K, err = t.testInsertBatchesWithPresent(TenMillion, TenMillion, TenThousand)
	if err != nil {
		return nil, fmt.Errorf("failed to run insert batches with present test: %w", err)
	}

	results.InsertsBatchedPres10M100K, err = t.testInsertBatchesWithPresent(TenMillion, TenMillion, HundredThousand)
	if err != nil {
		return nil, fmt.Errorf("failed to run insert batches with present test: %w", err)
	}

	return results, nil
}

type InsertBatchesTestResult struct {
	ULIDDuration     time.Duration
	ObjectIDDuration time.Duration
}

func (t *Tester) testInsertBatches(totalDocs, batchSize int) (*InsertBatchesTestResult, error) {
	var start time.Time

	result := new(InsertBatchesTestResult)

	{
		start = time.Now()
		if err := t.insertDocumentsInBatches(batchSize, generateDocsUlid(totalDocs)); err != nil {
			return nil, fmt.Errorf("error on insert documents in batches test run: %w", err)
		}
		result.ULIDDuration = time.Now().Sub(start)
		if err := t.dropCollection(); err != nil {
			return nil, fmt.Errorf("collection cleanup error: %w", err)
		}
	}

	{
		start = time.Now()
		if err := t.insertDocumentsInBatches(batchSize, generateDocsObjectID(totalDocs)); err != nil {
			return nil, fmt.Errorf("error on insert documents in batches test run: %w", err)
		}
		result.ObjectIDDuration = time.Now().Sub(start)
		if err := t.dropCollection(); err != nil {
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
		if err := t.dropCollection(); err != nil {
			return nil, fmt.Errorf("collection cleanup error: %w", err)
		}
	}

	{
		start = time.Now()
		if err := t.insertDocuments(generateDocsObjectID(totalDocs)); err != nil {
			return nil, fmt.Errorf("error on insert documents test run: %w", err)
		}
		result.ObjectIDDuration = time.Now().Sub(start)
		if err := t.dropCollection(); err != nil {
			return nil, fmt.Errorf("collection cleanup error: %w", err)
		}
	}

	return result, nil
}

type InsertBatchesWithPresentTestResult struct {
	ULIDDuration     time.Duration
	ObjectIDDuration time.Duration
}

func (t *Tester) testInsertBatchesWithPresent(insertCount, presentCount, batchSize int) (*InsertBatchesWithPresentTestResult, error) {
	var start time.Time

	result := new(InsertBatchesWithPresentTestResult)

	const prepareBatchSize = 100000

	{
		if err := t.insertDocumentsInBatches(prepareBatchSize, generateDocsUlid(presentCount)); err != nil {
			return nil, fmt.Errorf("error on insert documents in batches: %w", err)
		}
		start = time.Now()
		if err := t.insertDocumentsInBatches(batchSize, generateDocsUlid(insertCount)); err != nil {
			return nil, fmt.Errorf("error on insert documents in batches test run: %w", err)
		}
		result.ULIDDuration = time.Now().Sub(start)
		if err := t.dropCollection(); err != nil {
			return nil, fmt.Errorf("collection cleanup error: %w", err)
		}
	}

	{
		if err := t.insertDocumentsInBatches(prepareBatchSize, generateDocsObjectID(presentCount)); err != nil {
			return nil, fmt.Errorf("error on insert documents in batches: %w", err)
		}
		start = time.Now()
		if err := t.insertDocumentsInBatches(batchSize, generateDocsObjectID(insertCount)); err != nil {
			return nil, fmt.Errorf("error on insert documents in batches test run: %w", err)
		}
		result.ObjectIDDuration = time.Now().Sub(start)
		if err := t.dropCollection(); err != nil {
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

func (t *Tester) dropCollection() error {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	if err := t.Coll.Drop(ctx); err != nil {
		return fmt.Errorf("failed to drop collection: %w", err)
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
