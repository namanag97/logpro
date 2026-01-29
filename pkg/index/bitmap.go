// Package index provides bitmap indexes for fast attribute lookups on event logs.
package index

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
)

// AttributeIndex provides bitmap indexes for event log attributes.
// For each column, it maps distinct values to roaring bitmaps of row positions.
// This enables O(1) value lookups and efficient set operations (AND/OR/NOT)
// for multi-attribute filtering.
type AttributeIndex struct {
	mu sync.RWMutex

	// columns maps column_name -> value -> bitmap of row positions
	columns map[string]map[string]*roaring.Bitmap

	// rowCount tracks total rows indexed
	rowCount uint32
}

// NewAttributeIndex creates an empty attribute index.
func NewAttributeIndex() *AttributeIndex {
	return &AttributeIndex{
		columns: make(map[string]map[string]*roaring.Bitmap),
	}
}

// IndexBatch adds all rows from an Arrow RecordBatch to the index.
// rowOffset is the global row offset for multi-batch files.
func (idx *AttributeIndex) IndexBatch(batch arrow.Record, rowOffset uint32) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	nRows := int(batch.NumRows())
	schema := batch.Schema()

	for colIdx := 0; colIdx < int(batch.NumCols()); colIdx++ {
		field := schema.Field(colIdx)
		// Skip nested columns (LIST, STRUCT) — they are not indexed as flat values.
		if field.Type.ID() == arrow.LIST || field.Type.ID() == arrow.STRUCT {
			continue
		}

		fieldName := field.Name
		col := batch.Column(colIdx)

		if idx.columns[fieldName] == nil {
			idx.columns[fieldName] = make(map[string]*roaring.Bitmap)
		}
		valMap := idx.columns[fieldName]

		for rowIdx := 0; rowIdx < nRows; rowIdx++ {
			if col.IsNull(rowIdx) {
				continue
			}

			value := extractStringValue(col, rowIdx)
			bm, ok := valMap[value]
			if !ok {
				bm = roaring.New()
				valMap[value] = bm
			}
			bm.Add(rowOffset + uint32(rowIdx))
		}
	}

	idx.rowCount = rowOffset + uint32(nRows)
	return nil
}

// Lookup returns the bitmap of row positions where column == value.
func (idx *AttributeIndex) Lookup(column, value string) *roaring.Bitmap {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if valMap, ok := idx.columns[column]; ok {
		if bm, ok := valMap[value]; ok {
			return bm.Clone()
		}
	}
	return roaring.New()
}

// LookupAnd returns row positions matching ALL conditions (column=value pairs).
func (idx *AttributeIndex) LookupAnd(conditions map[string]string) *roaring.Bitmap {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var result *roaring.Bitmap
	for col, val := range conditions {
		bm := idx.lookupUnsafe(col, val)
		if result == nil {
			result = bm.Clone()
		} else {
			result.And(bm)
		}
	}
	if result == nil {
		return roaring.New()
	}
	return result
}

// LookupOr returns row positions matching ANY condition.
func (idx *AttributeIndex) LookupOr(conditions map[string]string) *roaring.Bitmap {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	result := roaring.New()
	for col, val := range conditions {
		bm := idx.lookupUnsafe(col, val)
		result.Or(bm)
	}
	return result
}

// Columns returns the list of indexed column names.
func (idx *AttributeIndex) Columns() []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	cols := make([]string, 0, len(idx.columns))
	for col := range idx.columns {
		cols = append(cols, col)
	}
	return cols
}

// Cardinality returns the number of distinct values for a column.
func (idx *AttributeIndex) Cardinality(column string) int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if valMap, ok := idx.columns[column]; ok {
		return len(valMap)
	}
	return 0
}

// DistinctValues returns all distinct values for a column.
func (idx *AttributeIndex) DistinctValues(column string) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	valMap, ok := idx.columns[column]
	if !ok {
		return nil
	}

	values := make([]string, 0, len(valMap))
	for v := range valMap {
		values = append(values, v)
	}
	return values
}

// RowCount returns the total number of indexed rows.
func (idx *AttributeIndex) RowCount() uint32 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.rowCount
}

// WriteTo serializes the entire index to a writer.
// Format: [rowCount:u32][numCols:u32]([nameLen:u32][name][numVals:u32]([valLen:u32][val][bitmap])...)...
func (idx *AttributeIndex) WriteTo(w io.Writer) (int64, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var total int64

	if err := binary.Write(w, binary.LittleEndian, idx.rowCount); err != nil {
		return total, err
	}
	total += 4

	numCols := uint32(len(idx.columns))
	if err := binary.Write(w, binary.LittleEndian, numCols); err != nil {
		return total, err
	}
	total += 4

	for colName, valMap := range idx.columns {
		n, err := writeString(w, colName)
		if err != nil {
			return total, err
		}
		total += int64(n)

		numVals := uint32(len(valMap))
		if err := binary.Write(w, binary.LittleEndian, numVals); err != nil {
			return total, err
		}
		total += 4

		for val, bm := range valMap {
			n, err := writeString(w, val)
			if err != nil {
				return total, err
			}
			total += int64(n)

			// Serialize the bitmap using its native WriteTo
			nn, err := bm.WriteTo(w)
			if err != nil {
				return total, fmt.Errorf("serialize bitmap for %s=%s: %w", colName, val, err)
			}
			total += nn
		}
	}

	return total, nil
}

// ReadFrom deserializes an index from a reader.
func (idx *AttributeIndex) ReadFrom(r io.Reader) (int64, error) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	var total int64

	if err := binary.Read(r, binary.LittleEndian, &idx.rowCount); err != nil {
		return total, err
	}
	total += 4

	var numCols uint32
	if err := binary.Read(r, binary.LittleEndian, &numCols); err != nil {
		return total, err
	}
	total += 4

	idx.columns = make(map[string]map[string]*roaring.Bitmap, numCols)

	for i := uint32(0); i < numCols; i++ {
		colName, n, err := readString(r)
		if err != nil {
			return total, err
		}
		total += int64(n)

		var numVals uint32
		if err := binary.Read(r, binary.LittleEndian, &numVals); err != nil {
			return total, err
		}
		total += 4

		valMap := make(map[string]*roaring.Bitmap, numVals)

		for j := uint32(0); j < numVals; j++ {
			val, n, err := readString(r)
			if err != nil {
				return total, err
			}
			total += int64(n)

			bm := roaring.New()
			nn, err := bm.ReadFrom(r)
			if err != nil {
				return total, fmt.Errorf("deserialize bitmap for %s=%s: %w", colName, val, err)
			}
			total += nn

			valMap[val] = bm
		}

		idx.columns[colName] = valMap
	}

	return total, nil
}

// lookupUnsafe performs a lookup without locking (caller must hold lock).
func (idx *AttributeIndex) lookupUnsafe(column, value string) *roaring.Bitmap {
	if valMap, ok := idx.columns[column]; ok {
		if bm, ok := valMap[value]; ok {
			return bm
		}
	}
	return roaring.New()
}

// extractStringValue converts an Arrow array value to a string for indexing.
// Handles dictionary-encoded columns by resolving through the dictionary.
func extractStringValue(col arrow.Array, row int) string {
	switch c := col.(type) {
	case *array.String:
		return c.Value(row)
	case *array.Int64:
		return fmt.Sprintf("%d", c.Value(row))
	case *array.Float64:
		return fmt.Sprintf("%g", c.Value(row))
	case *array.Boolean:
		if c.Value(row) {
			return "true"
		}
		return "false"
	case *array.Timestamp:
		return fmt.Sprintf("%d", c.Value(row))
	case *array.Dictionary:
		// Dictionary-aware: resolve through the dictionary to the decoded value.
		dict := c.Dictionary()
		dictIdx := c.GetValueIndex(row)
		return extractStringValue(dict, dictIdx)
	default:
		return ""
	}
}

func writeString(w io.Writer, s string) (int, error) {
	total := 0
	sLen := uint32(len(s))
	if err := binary.Write(w, binary.LittleEndian, sLen); err != nil {
		return total, err
	}
	total += 4
	n, err := w.Write([]byte(s))
	total += n
	return total, err
}

func readString(r io.Reader) (string, int, error) {
	total := 0
	var sLen uint32
	if err := binary.Read(r, binary.LittleEndian, &sLen); err != nil {
		return "", total, err
	}
	total += 4
	buf := make([]byte, sLen)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", total, err
	}
	total += int(sLen)
	return string(buf), total, nil
}
