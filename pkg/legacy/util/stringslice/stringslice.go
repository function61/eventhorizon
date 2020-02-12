package stringslice

func ItemIndex(item string, slice []string) int {
	for idx, val := range slice {
		if val == item {
			return idx
		}
	}

	return -1
}
