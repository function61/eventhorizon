package stringslicediff

type Result struct {
	Added   []string
	Removed []string
}

func Diff(base []string, other []string) Result {
	result := newResult()

	m_base := make(map[string]bool)
	m_other := make(map[string]bool)

	for _, val := range base {
		m_base[val] = true
	}

	for _, val := range other {
		if _, existsInBase := m_base[val]; !existsInBase {
			result.Added = append(result.Added, val)
		}
		m_other[val] = true
	}

	for _, val := range base {
		if _, existsInOther := m_other[val]; !existsInOther {
			result.Removed = append(result.Removed, val)
		}
	}

	return result
}

func newResult() Result {
	return Result{[]string{}, []string{}}
}
