package main

import (
	"fmt"
	"strings"
	"time"
)

type TablePrinter struct{}

func (p *TablePrinter) Print(r *TesterResults) {
	var header = []string{"Test case", "ObjectId", "ULID", "UUID", "% diff ULID", "% diff UUID"}
	var data = [][]string{
		append([]string{"1M inserts batched, batch size = 1k"}, p.makeRowDataInsertBatches(r.InsertsBatched1M1K)...),
		append([]string{"1M inserts batched, batch size = 5k"}, p.makeRowDataInsertBatches(r.InsertsBatched1M5K)...),
		append([]string{"1M inserts batched, batch size = 10k"}, p.makeRowDataInsertBatches(r.InsertsBatched1M10K)...),
		append([]string{"1M inserts"}, p.makeRowDataInserts(r.Insert1M)...),
		append(
			[]string{"10M inserts batched, 10M documents already present, batch size = 10k"},
			p.makeRowDataInsertBatchesPresInsert(r.InsertsBatchedPres10M10K)...,
		),
		append(
			[]string{"10M inserts batched, 10M documents already present, batch size = 100k"},
			p.makeRowDataInsertBatchesPresInsert(r.InsertsBatchedPres10M10K)...,
		),
		append(
			[]string{"Index size with 20M docs in bytes"},
			p.makeRowDataInsertBatchesPresIdxSize(r.InsertsBatchedPres10M10K)...,
		),
		append(
			[]string{"Get by ID from 20M docs, avg duration"},
			p.makeRowDataInsertBatchesPresGetByID(r.InsertsBatchedPres10M10K)...,
		),
	}

	p.printSep()
	p.printHeader(header)
	p.printSep()
	p.printData(data)
	p.printSep()
}

func (p *TablePrinter) printHeader(r []string) {
	fmt.Println(
		"|", fmt.Sprintf("%-70s", r[0]),
		"|", fmt.Sprintf("%-10s", r[1]),
		"|", fmt.Sprintf("%-12s", r[2]),
		"|", fmt.Sprintf("%-12s", r[3]),
		"|", fmt.Sprintf("%-12s", r[4]),
		"|", fmt.Sprintf("%-12s", r[5]),
		"|")
}

func (p *TablePrinter) printSep() {
	fmt.Println(
		"|", strings.Repeat("-", 70),
		"|", strings.Repeat("-", 10),
		"|", strings.Repeat("-", 12),
		"|", strings.Repeat("-", 12),
		"|", strings.Repeat("-", 12),
		"|", strings.Repeat("-", 12),
		"|")
}

func (p *TablePrinter) printData(data [][]string) {
	for _, row := range data {
		fmt.Println(
			"|", fmt.Sprintf("%-70s", row[0]),
			"|", fmt.Sprintf("%-10s", row[1]),
			"|", fmt.Sprintf("%-12s", row[2]),
			"|", fmt.Sprintf("%-12s", row[3]),
			"|", fmt.Sprintf("%-12s", row[4]),
			"|", fmt.Sprintf("%-12s", row[5]),
			"|")
	}
}

func (p *TablePrinter) makeRowDataInsertBatches(r *InsertBatchesTestResult) []string {
	if r == nil {
		return nil
	}
	return []string{
		r.ObjectIDDuration.Round(1 * time.Millisecond).String(),
		r.ULIDDuration.Round(1 * time.Millisecond).String(),
		r.UUIDDuration.Round(1 * time.Millisecond).String(),
		fmt.Sprintf("%.2f%%", calcDiffPercent(
			r.ObjectIDDuration.Microseconds(),
			r.ULIDDuration.Microseconds(),
		)),
		fmt.Sprintf("%.2f%%", calcDiffPercent(
			r.ObjectIDDuration.Microseconds(),
			r.UUIDDuration.Microseconds(),
		)),
	}
}

func (p *TablePrinter) makeRowDataInsertBatchesPresInsert(r *InsertBatchesWithPresentTestResult) []string {
	if r == nil {
		return nil
	}
	return []string{
		r.ObjectIDInsertDuration.Round(1 * time.Millisecond).String(),
		r.ULIDInsertDuration.Round(1 * time.Millisecond).String(),
		r.UUIDInsertDuration.Round(1 * time.Millisecond).String(),
		fmt.Sprintf("%.2f%%", calcDiffPercent(
			r.ObjectIDInsertDuration.Microseconds(),
			r.ULIDInsertDuration.Microseconds(),
		)),
		fmt.Sprintf("%.2f%%", calcDiffPercent(
			r.ObjectIDInsertDuration.Microseconds(),
			r.UUIDInsertDuration.Microseconds(),
		)),
	}
}

func (p *TablePrinter) makeRowDataInsertBatchesPresIdxSize(r *InsertBatchesWithPresentTestResult) []string {
	if r == nil {
		return nil
	}
	return []string{
		byteCountIEC(r.ObjectIDIdxSize),
		byteCountIEC(r.ULIDIdxSize),
		byteCountIEC(r.UUIDIdxSize),
		fmt.Sprintf("%.2f%%", calcDiffPercent(r.ObjectIDIdxSize, r.ULIDIdxSize)),
		fmt.Sprintf("%.2f%%", calcDiffPercent(r.ObjectIDIdxSize, r.UUIDIdxSize)),
	}
}

func (p *TablePrinter) makeRowDataInsertBatchesPresGetByID(r *InsertBatchesWithPresentTestResult) []string {
	if r == nil {
		return nil
	}
	return []string{
		r.ObjectIDGetDuration.Round(1 * time.Microsecond).String(),
		r.ULIDGetDuration.Round(1 * time.Microsecond).String(),
		r.UUIDGetDuration.Round(1 * time.Microsecond).String(),
		fmt.Sprintf("%.2f%%", calcDiffPercent(
			r.ObjectIDGetDuration.Microseconds(), r.ULIDGetDuration.Microseconds(),
		)),
		fmt.Sprintf("%.2f%%", calcDiffPercent(
			r.ObjectIDGetDuration.Microseconds(), r.UUIDGetDuration.Microseconds(),
		)),
	}
}

func (p *TablePrinter) makeRowDataInserts(r *InsertTestResult) []string {
	if r == nil {
		return nil
	}
	return []string{
		r.ObjectIDDuration.Round(1 * time.Millisecond).String(),
		r.ULIDDuration.Round(1 * time.Millisecond).String(),
		r.UUIDDuration.Round(1 * time.Millisecond).String(),
		fmt.Sprintf("%.2f%%", calcDiffPercent(
			r.ObjectIDDuration.Microseconds(),
			r.ULIDDuration.Microseconds(),
		)),
		fmt.Sprintf("%.2f%%", calcDiffPercent(
			r.ObjectIDDuration.Microseconds(),
			r.UUIDDuration.Microseconds(),
		)),
	}
}
