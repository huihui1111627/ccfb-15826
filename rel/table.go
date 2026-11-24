package rel

// BlockStats holds per-column min/max summaries of one block, used for
// block-level min/max pruning on the scan side.
type BlockStats struct {
	HasMin bool
	HasMax bool
	Min    Value
	Max    Value
	// HasNull is true when the column contains at least one NULL in the block.
	HasNull bool
}

// Block is an I/O / pruning unit of a table, mirroring an on-disk chunk.
type Block struct {
	Rows  []Row
	Stats []BlockStats // one entry per column
}

// Table is a collection of blocks plus a column schema.
type Table struct {
	Name    string
	Columns []string
	Kinds   []Kind
	Blocks  []*Block
	Rows    int
}

// ColIndex resolves a column name to its position.
func (t *Table) ColIndex(name string) int {
	for i, c := range t.Columns {
		if c == name {
			return i
		}
	}
	return -1
}

// BuildBlocks materializes rows into blocks of the given size and computes
// per-block column min/max summaries.
func BuildBlocks(name string, columns []string, kinds []Kind, rows []Row, blockSize int) *Table {
	if blockSize <= 0 {
		blockSize = len(rows)
		if blockSize == 0 {
			blockSize = 1
		}
	}
	t := &Table{Name: name, Columns: columns, Kinds: kinds, Rows: len(rows)}
	for start := 0; start < len(rows); start += blockSize {
		end := start + blockSize
		if end > len(rows) {
			end = len(rows)
		}
		b := &Block{Rows: rows[start:end], Stats: make([]BlockStats, len(columns))}
		for col := range columns {
			st := &b.Stats[col]
			for _, r := range b.Rows {
				v := r[col]
				if v.IsNull() {
					st.HasNull = true
					continue
				}
				if !st.HasMin || Compare(v, st.Min) < 0 {
					st.HasMin = true
					st.Min = v
				}
				if !st.HasMax || Compare(v, st.Max) > 0 {
					st.HasMax = true
					st.Max = v
				}
			}
		}
		t.Blocks = append(t.Blocks, b)
	}
	return t
}

// RangeOverlap reports whether a block column could contain any value in
// [lo, hi]. lo/hi are non-null. hasNull controls whether NULLs in the block
// matter and is handled separately by the scan via NullPasses.
func RangeOverlap(st BlockStats, lo, hi Value) bool {
	if !st.HasMin {
		// Block column is entirely NULL: only NULL rows could pass, handled
		// by the null path; no non-null value can match.
		return false
	}
	if Compare(st.Max, lo) < 0 {
		return false
	}
	if Compare(st.Min, hi) > 0 {
		return false
	}
	return true
}
