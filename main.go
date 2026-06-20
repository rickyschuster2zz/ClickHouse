package main

import (
	"crypto/sha256"
	"fmt"
)

// Block represents a data block with a deduplication token (hash).
type Block struct {
	Hash string
	Data string
}

// ReplicatedTable simulates a ReplicatedMergeTree table.
type ReplicatedTable struct {
	Name              string
	ProcessedHashes   map[string]bool
	Data              []string
	InsertDeduplicate bool
}

func NewReplicatedTable(name string) *ReplicatedTable {
	return &ReplicatedTable{
		Name:              name,
		ProcessedHashes:   make(map[string]bool),
		Data:              []string{},
		InsertDeduplicate: true,
	}
}

// Insert inserts a block into the table, performing deduplication if enabled.
func (t *ReplicatedTable) Insert(block Block) bool {
	if t.InsertDeduplicate {
		if t.ProcessedHashes[block.Hash] {
			// Block is a duplicate, skip insertion
			return false
		}
		t.ProcessedHashes[block.Hash] = true
	}
	t.Data = append(t.Data, block.Data)
	return true
}

// MaterializedView simulates a Materialized View forwarding data from Source to Target.
type MaterializedView struct {
	Source *ReplicatedTable
	Target *ReplicatedTable
}

// InsertWithMV simulates the insertion pipeline with Materialized View.
func (mv *MaterializedView) InsertWithMV(block Block) {
	// 1. Insert into source table
	inserted := mv.Source.Insert(block)

	// 2. Propagate the deduplication token to the target table.
	// We generate a deterministic derivative token for the target table.
	targetHash := mv.deriveTargetHash(block.Hash)

	targetBlock := Block{
		Hash: targetHash,
		Data: block.Data,
	}

	if inserted {
		mv.Target.Insert(targetBlock)
	} else {
		// Even if the source table deduplicated the block, we still attempt to insert
		// into the target table with the derived hash. The target table's own deduplication
		// mechanism will ensure it is not duplicated if it was already processed.
		mv.Target.Insert(targetBlock)
	}
}

// deriveTargetHash generates a deterministic hash for the target table based on the source hash.
func (mv *MaterializedView) deriveTargetHash(sourceHash string) string {
	h := sha256.New()
	h.Write([]byte(sourceHash + "_" + mv.Target.Name))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func main() {
	fmt.Println("Simulating Materialized View Deduplication...")

	// Initialize tables
	sourceTable := NewReplicatedTable("source_table")
	targetTable := NewReplicatedTable("target_table")
	mv := &MaterializedView{Source: sourceTable, Target: targetTable}

	// 1. Normal Insert
	block1 := Block{Hash: "block_hash_1", Data: "row1"}
	mv.InsertWithMV(block1)

	// 2. Simulate Network Partition / Retry
	// The client retries inserting block1 because it didn't get an ack.
	mv.InsertWithMV(block1)

	// Verify deduplication
	fmt.Printf("Source Table Rows: %v (Expected: 1)\n", len(sourceTable.Data))
	fmt.Printf("Target Table Rows: %v (Expected: 1)\n", len(targetTable.Data))

	if len(sourceTable.Data) == 1 && len(targetTable.Data) == 1 {
		fmt.Println("Deduplication successful! No duplicate rows in source or target tables.")
	} else {
		fmt.Println("Deduplication failed! Duplicate rows detected.")
	}
}