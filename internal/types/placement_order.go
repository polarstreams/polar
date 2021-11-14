package types

import "math"

// OrdinalsPlacementOrder gets a slice of ordinals in the placement order.
//
// e.g. {0, 1, 2} for 3-broker cluster and {0, 3, 1, 4, 2, 5} for a 6-broker cluster.
func OrdinalsPlacementOrder(size int) []uint32 {
	if size == 3 {
		return []uint32{0, 1, 2}
	}
	ringSize6 := []uint32{0, 3, 1, 4, 2, 5}
	if size == 6 {
		return ringSize6
	}

	// Rings are 3*2^n (6, 12, 24, ...)
	exponent := math.Log2(float64(size / 3))
	lastExponent := exponent - 1
	lastPow2 := math.Pow(2, lastExponent)

	// To calculate the odd numbers
	lastStartIndex := 3 * math.Pow(2, lastExponent)

	result := make([]uint32, size)

	for i := 0; i < size; i++ {
		if i%2 == 1 {
			result[i] = uint32(lastStartIndex + math.Floor(float64(i)/2))
			continue
		}

		if i%int(lastPow2) == 0 {
			// The position of ring size 6
			result[i] = ringSize6[int(math.Floor(float64(i)/lastPow2))]
			continue
		}

		for j := 1.0; j <= exponent-2; j++ {
			e := exponent - j
			step := int(math.Pow(2, e))
			if i%step == int(math.Pow(2, e-1)) {
				startSeries := 3 * math.Pow(2, j)
				result[i] = uint32(startSeries) + uint32(i/step)
			}
		}
	}

	return result
}
