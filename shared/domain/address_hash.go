package domain

// 내부 공통 함수: 하위 16비트 정도 안전하게 읽어서 마스킹
func (a Address) lowBits() uint16 {
	// 마지막 2바이트만 읽으면 충분 (최대 10비트만 필요)
	return uint16(a[18])<<8 | uint16(a[19])
}

// HashAddress8: 3비트 (0~7)
func (a Address) HashAddress8() uint8 {
	return uint8(a.lowBits() & 0x7) // 0b111
}

// HashAddress64: 6비트 (0~63)
func (a Address) HashAddress64() uint8 {
	return uint8(a.lowBits() & 0x3F) // 0b111111
}

// HashAddress128: 7비트 (0~127)
func (a Address) HashAddress128() uint8 {
	return uint8(a.lowBits() & 0x7F) // 0b1111111
}

// HashAddress256: 8비트 (0~255)
func (a Address) HashAddress256() uint8 {
	return uint8(a.lowBits() & 0xFF) // 0b11111111
}

// HashAddress512: 9비트 (0~511)
func (a Address) HashAddress512() uint16 {
	return a.lowBits() & 0x1FF // 0b111111111
}

// HashAddress1024: 10비트 (0~1023)
func (a Address) HashAddress1024() uint16 {
	return a.lowBits() & 0x3FF // 0b1111111111
}
