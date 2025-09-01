package app

import (
	"sync"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/domain"
)

// RopeSizeCache는 로프 ID -> 사이즈 매핑을 LRU 방식으로 캐싱
type RopeSizeCache struct {
	cache     map[domain.RopeID]uint32
	lruList   []domain.RopeID // 가장 오래된 것이 앞에 위치
	maxSize   int
	mutex     sync.RWMutex
	db        *BadgerRopeDB // DB에서 실제 값 조회용
}

// NewRopeSizeCache 새 로프 사이즈 캐시 생성
func NewRopeSizeCache(maxSize int, db *BadgerRopeDB) *RopeSizeCache {
	return &RopeSizeCache{
		cache:   make(map[domain.RopeID]uint32),
		lruList: make([]domain.RopeID, 0),
		maxSize: maxSize,
		db:      db,
	}
}

// Get 로프 사이즈 조회 (캐시에 없으면 DB에서 로드)
func (c *RopeSizeCache) Get(ropeID domain.RopeID) uint32 {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	
	// 캐시에 있으면 LRU 위치 업데이트하고 반환
	if size, exists := c.cache[ropeID]; exists {
		c.updateLRU(ropeID)
		return size
	}
	
	// 캐시에 없으면 DB에서 로드
	rm := c.db.getRopeMark(ropeID)
	size := rm.Size
	
	// 캐시에 추가
	c.addToCache(ropeID, size)
	
	return size
}

// Put 로프 사이즈 캐시에 직접 추가/업데이트 (DB 쓰기 후 호출)
func (c *RopeSizeCache) Put(ropeID domain.RopeID, size uint32) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	
	c.addToCache(ropeID, size)
}

// addToCache 내부적으로 캐시에 항목 추가 (mutex 보유 상태에서 호출)
func (c *RopeSizeCache) addToCache(ropeID domain.RopeID, size uint32) {
	// 이미 존재하면 업데이트하고 LRU 위치 조정
	if _, exists := c.cache[ropeID]; exists {
		c.cache[ropeID] = size
		c.updateLRU(ropeID)
		return
	}
	
	// 캐시가 가득 찬 경우 오래된 항목 1만개 제거
	if len(c.cache) >= c.maxSize {
		c.evictOldest(10000)
	}
	
	// 새 항목 추가
	c.cache[ropeID] = size
	c.lruList = append(c.lruList, ropeID)
}

// updateLRU LRU 리스트에서 항목을 맨 뒤로 이동
func (c *RopeSizeCache) updateLRU(ropeID domain.RopeID) {
	// 기존 위치 찾아서 제거
	for i, id := range c.lruList {
		if id == ropeID {
			c.lruList = append(c.lruList[:i], c.lruList[i+1:]...)
			break
		}
	}
	// 맨 뒤에 추가
	c.lruList = append(c.lruList, ropeID)
}

// evictOldest 가장 오래된 항목들 제거
func (c *RopeSizeCache) evictOldest(count int) {
	if count >= len(c.lruList) {
		// 모든 항목 제거
		c.cache = make(map[domain.RopeID]uint32)
		c.lruList = make([]domain.RopeID, 0)
		return
	}
	
	// count개만큼 앞에서부터 제거
	for i := 0; i < count; i++ {
		if len(c.lruList) > 0 {
			oldestID := c.lruList[0]
			delete(c.cache, oldestID)
			c.lruList = c.lruList[1:]
		}
	}
}

// GetStats 캐시 통계 반환
func (c *RopeSizeCache) GetStats() map[string]interface{} {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	
	return map[string]interface{}{
		"size":    len(c.cache),
		"maxSize": c.maxSize,
	}
}