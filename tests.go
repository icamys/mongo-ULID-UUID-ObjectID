package main

import (
	"context"
	"fmt"
	"math/rand"
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
	InsertsBatched1M1K        *InsertBatchesTestResult
	InsertsBatched1M5K        *InsertBatchesTestResult
	InsertsBatched1M10K       *InsertBatchesTestResult
	Insert1M                  *InsertTestResult
	InsertsBatchedPres10M10K  *InsertBatchesWithPresentTestResult
	InsertsBatchedPres10M100K *InsertBatchesWithPresentTestResult
}

func PrintTable(r *TesterResults) {
	var header = []string{"Test case", "ObjectId", "ULID", "% diff"}
	var data [][4]string

	data = append(data,
		[4]string{
			"1M inserts batched, batch size = 1k",
			r.InsertsBatched1M1K.ObjectIDDuration.Round(1 * time.Millisecond).String(),
			r.InsertsBatched1M1K.ULIDDuration.Round(1 * time.Millisecond).String(),
			fmt.Sprintf("%.2f%%", calcDiffPercent(
				r.InsertsBatched1M1K.ObjectIDDuration.Microseconds(),
				r.InsertsBatched1M1K.ULIDDuration.Microseconds(),
			)),
		},
		[4]string{
			"1M inserts batched, batch size = 5k",
			r.InsertsBatched1M5K.ObjectIDDuration.Round(1 * time.Millisecond).String(),
			r.InsertsBatched1M5K.ULIDDuration.Round(1 * time.Millisecond).String(),
			fmt.Sprintf("%.2f%%", calcDiffPercent(
				r.InsertsBatched1M5K.ObjectIDDuration.Microseconds(),
				r.InsertsBatched1M5K.ULIDDuration.Microseconds(),
			)),
		},
		[4]string{
			"1M inserts batched, batch size = 10k",
			r.InsertsBatched1M10K.ObjectIDDuration.Round(1 * time.Millisecond).String(),
			r.InsertsBatched1M10K.ULIDDuration.Round(1 * time.Millisecond).String(),
			fmt.Sprintf("%.2f%%", calcDiffPercent(
				r.InsertsBatched1M10K.ObjectIDDuration.Microseconds(),
				r.InsertsBatched1M10K.ULIDDuration.Microseconds(),
			)),
		},
		[4]string{
			"1M inserts",
			r.Insert1M.ObjectIDDuration.Round(1 * time.Millisecond).String(),
			r.Insert1M.ULIDDuration.Round(1 * time.Millisecond).String(),
			fmt.Sprintf("%.2f%%", calcDiffPercent(
				r.Insert1M.ObjectIDDuration.Microseconds(),
				r.Insert1M.ULIDDuration.Microseconds(),
			)),
		},
		[4]string{
			"10M inserts batched, 10M documents already present, batch size = 10k",
			r.InsertsBatchedPres10M10K.ObjectIDInsertDuration.Round(1 * time.Millisecond).String(),
			r.InsertsBatchedPres10M10K.ULIDInsertDuration.Round(1 * time.Millisecond).String(),
			fmt.Sprintf("%.2f%%", calcDiffPercent(
				r.InsertsBatchedPres10M10K.ObjectIDInsertDuration.Microseconds(),
				r.InsertsBatchedPres10M10K.ULIDInsertDuration.Microseconds(),
			)),
		},
		[4]string{
			"10M inserts batched, 10M documents already present, batch size = 100k",
			r.InsertsBatchedPres10M100K.ObjectIDInsertDuration.Round(1 * time.Millisecond).String(),
			r.InsertsBatchedPres10M100K.ULIDInsertDuration.Round(1 * time.Millisecond).String(),
			fmt.Sprintf("%.2f%%", calcDiffPercent(
				r.InsertsBatchedPres10M100K.ObjectIDInsertDuration.Microseconds(),
				r.InsertsBatchedPres10M100K.ULIDInsertDuration.Microseconds(),
			)),
		},
		[4]string{
			"Index size with 20M docs in bytes",
			byteCountIEC(r.InsertsBatchedPres10M100K.ObjectIDIdxSize),
			byteCountIEC(r.InsertsBatchedPres10M100K.ULIDIdxSize),
			fmt.Sprintf("%.2f%%", calcDiffPercent(
				r.InsertsBatchedPres10M100K.ObjectIDIdxSize, r.InsertsBatchedPres10M100K.ULIDIdxSize)),
		},
		[4]string{
			"Get by ID from 20M docs, avg duration",
			r.InsertsBatchedPres10M100K.ObjectIDGetDuration.Round(1 * time.Microsecond).String(),
			r.InsertsBatchedPres10M100K.ULIDGetDuration.Round(1 * time.Microsecond).String(),
			fmt.Sprintf("%.2f%%", calcDiffPercent(
				r.InsertsBatchedPres10M100K.ObjectIDGetDuration.Microseconds(),
				r.InsertsBatchedPres10M100K.ULIDGetDuration.Microseconds(),
			)),
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
			"|", fmt.Sprintf("%-70s", row[0]),
			"|", fmt.Sprintf("%-10s", row[1]),
			"|", fmt.Sprintf("%-22s", row[2]),
			"|", fmt.Sprintf("%-12s", row[3]),
			"|")
	}
}

func calcDiffPercent(baseline, newVal int64) float64 {
	var k float64 = 1
	if newVal > baseline {
		k = -1
	}

	return k * (float64(newVal) - float64(baseline)) * 100 / float64(baseline)
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
	ULIDInsertDuration     time.Duration
	ULIDIdxSize            int64
	ULIDGetDuration        time.Duration
	ObjectIDInsertDuration time.Duration
	ObjectIDIdxSize        int64
	ObjectIDGetDuration    time.Duration
}

func (t *Tester) testInsertBatchesWithPresent(insertCount, presentCount, batchSize int) (*InsertBatchesWithPresentTestResult, error) {
	var start time.Time

	result := new(InsertBatchesWithPresentTestResult)

	const prepareBatchSize = 100000
	const getProbes = 100

	{
		// provisioning with fixtures
		fixtures := generateDocsUlid(presentCount)
		if err := t.insertDocumentsInBatches(prepareBatchSize, fixtures); err != nil {
			return nil, fmt.Errorf("error on insert documents in batches: %w", err)
		}

		// inserting batches
		start = time.Now()
		if err := t.insertDocumentsInBatches(batchSize, generateDocsUlid(insertCount)); err != nil {
			return nil, fmt.Errorf("error on insert documents in batches test run: %w", err)
		}
		result.ULIDInsertDuration = time.Now().Sub(start)

		// getting random docs
		getIDs := pickRandomULID(fixtures, getProbes)
		start = time.Now()
		for _, id := range getIDs {
			if err := t.getDocumentByID(id); err != nil {
				return nil, fmt.Errorf("error on getting document by id: %w", err)
			}
		}
		result.ULIDGetDuration = time.Now().Sub(start) / getProbes

		// getting the index size
		if idxSize, err := t.getDefaultIDIndexSize(); err != nil {
			return nil, fmt.Errorf("failed to get default id index size: %w", err)
		} else {
			result.ULIDIdxSize = idxSize
		}

		if err := t.dropCollection(); err != nil {
			return nil, fmt.Errorf("collection cleanup error: %w", err)
		}
	}

	{
		// provisioning with fixtures
		fixtures := generateDocsObjectID(presentCount)
		if err := t.insertDocumentsInBatches(prepareBatchSize, fixtures); err != nil {
			return nil, fmt.Errorf("error on insert documents in batches: %w", err)
		}

		// inserting batches
		start = time.Now()
		if err := t.insertDocumentsInBatches(batchSize, generateDocsObjectID(insertCount)); err != nil {
			return nil, fmt.Errorf("error on insert documents in batches test run: %w", err)
		}
		result.ObjectIDInsertDuration = time.Now().Sub(start)

		// getting random docs
		getIDs := pickRandomObjectID(fixtures, getProbes)
		start = time.Now()
		for _, id := range getIDs {
			if err := t.getDocumentByID(id); err != nil {
				return nil, fmt.Errorf("error on getting document by id: %w", err)
			}
		}
		result.ObjectIDGetDuration = time.Now().Sub(start) / getProbes

		// getting the index size
		if idxSize, err := t.getDefaultIDIndexSize(); err != nil {
			return nil, fmt.Errorf("failed to get default id index size: %w", err)
		} else {
			result.ObjectIDIdxSize = idxSize
		}

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

func (t *Tester) getDocumentByID(id interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err := t.Coll.Find(ctx, bson.M{"_id": id})
	cancel()
	if err != nil {
		return fmt.Errorf("error getting document: %w", err)
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

func (t *Tester) getDefaultIDIndexSize() (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res := t.Coll.Database().RunCommand(ctx, bson.M{"collStats": "perftest"})

	var document bson.M
	if err := res.Decode(&document); err != nil {
		return 0, fmt.Errorf("%w", err)
	}

	idxSizes, ok := document["indexSizes"]
	if !ok {
		return 0, fmt.Errorf("indexSizes key not found")
	}

	size, ok := idxSizes.(bson.M)["_id_"]
	if !ok {
		return 0, fmt.Errorf("_id_ key not found")
	}

	return int64(size.(int32)), nil
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

func byteCountIEC(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB",
		float64(b)/float64(div), "KMGTPE"[exp])
}

func pickRandomULID(docs []interface{}, n int) []ulid.ULID {
	docsLen := len(docs)
	var result = make([]ulid.ULID, n)
	for i := 0; i < n; i++ {
		d := docs[rand.Intn(docsLen)]
		result[i] = d.(mongoDocumentULID).ID
	}
	return result
}

func pickRandomObjectID(docs []interface{}, n int) []primitive.ObjectID {
	docsLen := len(docs)
	var result = make([]primitive.ObjectID, n)
	for i := 0; i < n; i++ {
		d := docs[rand.Intn(docsLen)]
		result[i] = d.(mongoDocumentObjectID).ID
	}
	return result
}
