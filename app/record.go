package main

var serialTypes = map[int][2]int{
	2: [2]int{1, 1},
}

func SerialTypeData(serialType uint64) (string, uint64) {
	switch serialType {
	case 0:
		return "nil", 0
	case 1, 2, 3, 4:
		return "int", serialType
	default:
		return "", 0
	}
}
