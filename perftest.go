package main

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/google/uuid"
	"github.com/oklog/ulid/v2"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"go.mongodb.org/mongo-driver/bson/bsontype"
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

	start := time.Now()
	results, err := tester.Run()
	if err != nil {
		panic(err)
	}
	testDuration := time.Now().Sub(start)

	printer := new(TablePrinter)
	printer.Print(results)

	fmt.Printf("\nTotal execution time: %s\n", testDuration.Round(time.Millisecond).String())
}

func mustConnect() (*mongo.Collection, func()) {
	const timeout = 1 * time.Second
	ctx := context.Background()
	uri := os.Getenv("MONGO_URI")

	registry := bson.NewRegistryBuilder().
		RegisterTypeEncoder(ulidType, bsoncodec.ValueEncoderFunc(ULIDEncodeValue)).
		RegisterTypeDecoder(ulidType, bsoncodec.ValueDecoderFunc(ULIDDecodeValue)).
		RegisterTypeEncoder(uuidType, bsoncodec.ValueEncoderFunc(UUIDEncodeValue)).
		RegisterTypeDecoder(uuidType, bsoncodec.ValueDecoderFunc(UUIDDecodeValue)).
		Build()

	connCtx, connCancel := context.WithTimeout(ctx, timeout)
	defer connCancel()
	connOptions := mongooptions.Client().ApplyURI(uri).SetRegistry(registry)

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

var uuidType = reflect.TypeOf(uuid.UUID{})
var ulidType = reflect.TypeOf(ulid.ULID{})

func ULIDEncodeValue(_ bsoncodec.EncodeContext, vw bsonrw.ValueWriter, val reflect.Value) error {
	if !val.IsValid() || val.Type() != ulidType {
		return bsoncodec.ValueEncoderError{Name: "ULIDEncodeValue", Types: []reflect.Type{ulidType}, Received: val}
	}
	b, ok := val.Interface().(ulid.ULID)
	if !ok {
		return fmt.Errorf("failed to convert interface of type %s to %s",
			reflect.TypeOf(val.Interface()).String(), reflect.TypeOf(b))
	}

	if err := vw.WriteBinaryWithSubtype(b[:], bsontype.BinaryUUID); err != nil {
		return fmt.Errorf("failed to write binary: %w", err)
	}
	return nil
}

func ULIDDecodeValue(_ bsoncodec.DecodeContext, vr bsonrw.ValueReader, val reflect.Value) error {
	if !val.CanSet() || val.Type() != ulidType {
		return bsoncodec.ValueDecoderError{Name: "ULIDDecodeValue", Types: []reflect.Type{ulidType}, Received: val}
	}

	var data []byte
	var subtype byte
	var err error

	//nolint:exhaustive // the rest of types are covered by the `default` branch
	switch vrType := vr.Type(); vrType {
	case bsontype.Binary:
		data, subtype, err = vr.ReadBinary()
		if subtype != bsontype.BinaryUUID {
			return fmt.Errorf("unsupported binary subtype %v for ULID", subtype)
		}
	case bsontype.Null:
		err = vr.ReadNull()
	case bsontype.Undefined:
		err = vr.ReadUndefined()
	default:
		return fmt.Errorf("cannot decode %v into a ULID", vrType)
	}

	if err != nil {
		return fmt.Errorf("failed to read ULID value: %w", err)
	}
	val.Set(reflect.ValueOf(ulid.ULID(data)))
	return nil
}

func UUIDEncodeValue(_ bsoncodec.EncodeContext, vw bsonrw.ValueWriter, val reflect.Value) error {
	if !val.IsValid() || val.Type() != uuidType {
		return bsoncodec.ValueEncoderError{Name: "UUIDEncodeValue", Types: []reflect.Type{uuidType}, Received: val}
	}
	b, ok := val.Interface().(uuid.UUID)
	if !ok {
		return fmt.Errorf("failed to convert interface of type %s to %s",
			reflect.TypeOf(val.Interface()).String(), reflect.TypeOf(b))
	}

	if err := vw.WriteBinaryWithSubtype(b[:], bsontype.BinaryUUID); err != nil {
		return fmt.Errorf("failed to write binary: %w", err)
	}
	return nil
}

func UUIDDecodeValue(_ bsoncodec.DecodeContext, vr bsonrw.ValueReader, val reflect.Value) error {
	if !val.CanSet() || val.Type() != uuidType {
		return bsoncodec.ValueDecoderError{Name: "UUIDDecodeValue", Types: []reflect.Type{uuidType}, Received: val}
	}

	var data []byte
	var subtype byte
	var err error

	//nolint:exhaustive // the rest of types are covered by the `default` branch
	switch vrType := vr.Type(); vrType {
	case bsontype.Binary:
		data, subtype, err = vr.ReadBinary()
		if subtype != bsontype.BinaryUUID {
			return fmt.Errorf("unsupported binary subtype %v for UUID", subtype)
		}
	case bsontype.Null:
		err = vr.ReadNull()
	case bsontype.Undefined:
		err = vr.ReadUndefined()
	default:
		return fmt.Errorf("cannot decode %v into a UUID", vrType)
	}

	if err != nil {
		return fmt.Errorf("failed to read UUID value: %w", err)
	}
	uuidBytes, err := uuid.FromBytes(data)
	if err != nil {
		return fmt.Errorf("failed to read UUID from bytes: %w", err)
	}
	val.Set(reflect.ValueOf(uuidBytes))
	return nil
}
