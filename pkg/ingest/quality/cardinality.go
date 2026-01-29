package quality

import (
	"hash/fnv"
	"math"
)

// CardinalityEstimator estimates unique values using HyperLogLog.
type CardinalityEstimator struct {
	registers []uint8
	precision uint8
}

// NewCardinalityEstimator creates an estimator.
func NewCardinalityEstimator() *CardinalityEstimator {
	precision := uint8(14) // 2^14 = 16384 registers
	return &CardinalityEstimator{
		registers: make([]uint8, 1<<precision),
		precision: precision,
	}
}

// Add adds a value to the estimator.
func (e *CardinalityEstimator) Add(value string) {
	h := fnv.New64a()
	h.Write([]byte(value))
	hash := h.Sum64()

	// Get register index
	idx := hash >> (64 - e.precision)

	// Count leading zeros
	w := hash<<e.precision | 1<<(e.precision-1)
	lz := uint8(countLeadingZeros(w)) + 1

	if lz > e.registers[idx] {
		e.registers[idx] = lz
	}
}

// Estimate returns the cardinality estimate.
func (e *CardinalityEstimator) Estimate() uint64 {
	m := float64(len(e.registers))

	// Calculate harmonic mean
	sum := 0.0
	zeros := 0
	for _, val := range e.registers {
		sum += math.Pow(2, -float64(val))
		if val == 0 {
			zeros++
		}
	}

	alpha := 0.7213 / (1 + 1.079/m)
	estimate := alpha * m * m / sum

	// Small range correction
	if estimate <= 2.5*m && zeros > 0 {
		estimate = m * math.Log(m/float64(zeros))
	}

	return uint64(estimate)
}

// Merge combines two estimators.
func (e *CardinalityEstimator) Merge(other *CardinalityEstimator) {
	for i := range e.registers {
		if other.registers[i] > e.registers[i] {
			e.registers[i] = other.registers[i]
		}
	}
}

func countLeadingZeros(x uint64) int {
	if x == 0 {
		return 64
	}
	n := 0
	if x&0xFFFFFFFF00000000 == 0 {
		n += 32
		x <<= 32
	}
	if x&0xFFFF000000000000 == 0 {
		n += 16
		x <<= 16
	}
	if x&0xFF00000000000000 == 0 {
		n += 8
		x <<= 8
	}
	if x&0xF000000000000000 == 0 {
		n += 4
		x <<= 4
	}
	if x&0xC000000000000000 == 0 {
		n += 2
		x <<= 2
	}
	if x&0x8000000000000000 == 0 {
		n++
	}
	return n
}
