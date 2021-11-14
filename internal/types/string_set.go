package types

type StringSet map[string]bool

func (s *StringSet) Add(values ...string) {
	set := *s
	for _, v := range values {
		set[v] = true
	}
}

func (s *StringSet) ToSlice() []string {
	set := *s
	result := make([]string, 0, len(set))
	for t := range set {
		result = append(result, t)
	}

	return result
}
