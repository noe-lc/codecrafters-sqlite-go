package main

var serialTypes = map[int][2]int{
	2: [2]int{1, 1},
}

func SerialTypeData(serialInt uint64) (uint64, bool) {
	switch serialInt {
	case 0:
		return 0, true
	case 1, 2, 3, 4:
		return serialInt, true
	case 5:
		return 6, true
	case 6, 7:
		return 8, true
	case 8:
		return 0, true
	case 9:
		return 1, true
	case 10, 11:
		return 0, false
	default:
		if serialInt >= 12 && serialInt%2 == 0 {
			return (serialInt - 12) / 2, true
		}
		if serialInt >= 13 && serialInt%2 != 0 {
			return (serialInt - 13) / 2, true
		}
	}

	return 0, false
}
