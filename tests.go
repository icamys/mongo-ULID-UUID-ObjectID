package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/oklog/ulid/v2"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

func init() {
	uuid.EnableRandPool()
}

type mongoDocumentULID struct {
	ID ulid.ULID `bson:"_id"`
}
type mongoDocumentUUID struct {
	ID uuid.UUID `bson:"_id"`
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
	UUIDDuration     time.Duration
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
		if err := t.insertDocumentsInBatches(batchSize, generateDocsUUID(totalDocs)); err != nil {
			return nil, fmt.Errorf("error on insert documents in batches test run: %w", err)
		}
		result.UUIDDuration = time.Now().Sub(start)
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
	UUIDDuration     time.Duration
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
		if err := t.insertDocuments(generateDocsUUID(totalDocs)); err != nil {
			return nil, fmt.Errorf("error on insert documents test run: %w", err)
		}
		result.UUIDDuration = time.Now().Sub(start)
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
	ObjectIDInsertDuration time.Duration
	ObjectIDIdxSize        int64
	ObjectIDGetDuration    time.Duration

	ULIDInsertDuration time.Duration
	ULIDIdxSize        int64
	ULIDGetDuration    time.Duration

	UUIDInsertDuration time.Duration
	UUIDIdxSize        int64
	UUIDGetDuration    time.Duration
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
		fixtures := generateDocsUUID(presentCount)
		if err := t.insertDocumentsInBatches(prepareBatchSize, fixtures); err != nil {
			return nil, fmt.Errorf("error on insert documents in batches: %w", err)
		}

		// inserting batches
		start = time.Now()
		if err := t.insertDocumentsInBatches(batchSize, generateDocsUUID(insertCount)); err != nil {
			return nil, fmt.Errorf("error on insert documents in batches test run: %w", err)
		}
		result.UUIDInsertDuration = time.Now().Sub(start)

		// getting random docs
		getIDs := pickRandomUUID(fixtures, getProbes)
		start = time.Now()
		for _, id := range getIDs {
			if err := t.getDocumentByID(id); err != nil {
				return nil, fmt.Errorf("error on getting document by id: %w", err)
			}
		}
		result.UUIDGetDuration = time.Now().Sub(start) / getProbes

		// getting the index size
		if idxSize, err := t.getDefaultIDIndexSize(); err != nil {
			return nil, fmt.Errorf("failed to get default id index size: %w", err)
		} else {
			result.UUIDIdxSize = idxSize
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

func generateDocsUUID(n int) []interface{} {
	result := make([]interface{}, n)
	for i := 0; i < n; i++ {
		result[i] = mongoDocumentUUID{
			ID: uuid.New(),
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

func pickRandomUUID(docs []interface{}, n int) []uuid.UUID {
	docsLen := len(docs)
	var result = make([]uuid.UUID, n)
	for i := 0; i < n; i++ {
		d := docs[rand.Intn(docsLen)]
		result[i] = d.(mongoDocumentUUID).ID
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

func calcDiffPercent(baseline, newVal int64) float64 {
	var k float64 = 1
	if newVal > baseline {
		k = -1
	}

	return k * (float64(newVal) - float64(baseline)) * 100 / float64(baseline)
}
