//go:build integration
// +build integration

package integration

type ConsumerPollResponseJson struct {
	Topic       string           `json:"topics"`
	Token       string           `json:"token"`
	RangeIndex  int              `json:"rangeIndex"`
	StartOffset string           `json:"startOffset"`
	Values      []map[string]any `json:"values"`
}
