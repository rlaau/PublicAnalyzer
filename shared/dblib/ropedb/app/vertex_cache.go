package app

import (
	"container/list"
	"sync"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/dblib/ropedb/domain"
	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

// VertexCache - 단순한 LRU 캐시 + 주기적 BatchWrite
type VertexCache struct {
	mu      sync.RWMutex
	items   map[string]*cacheItem // key: address string
	lruList *list.List            // LRU 순서
	maxSize int                   // 최대 캐시 크기

	// BadgerDB 연결
	db *BadgerRopeDB

	// 통계
	hits   int64
	misses int64
}

type cacheItem struct {
	key      string
	vertex   *domain.Vertex
	element  *list.Element
	lastUsed time.Time
}

// NewVertexCache - 캐시 생성
func NewVertexCache(db *BadgerRopeDB, maxSize int) *VertexCache {
	if maxSize <= 0 {
		maxSize = 50000 // 기본 5만개
	}

	return &VertexCache{
		items:   make(map[string]*cacheItem, maxSize),
		lruList: list.New(),
		maxSize: maxSize,
		db:      db,
	}
}

// Get - 버텍스 조회 (캐시 우선)
func (c *VertexCache) Get(addr shareddomain.Address) *domain.Vertex {
	key := addr.String()

	c.mu.RLock()
	item, exists := c.items[key]
	if exists {
		c.mu.RUnlock()

		// 캐시 히트 - LRU 업데이트
		c.mu.Lock()
		item.lastUsed = time.Now()
		c.lruList.MoveToFront(item.element)
		c.hits++
		c.mu.Unlock()

		return item.vertex // 동일 쓰레드에서 사용하므로 복사 불필요
	}
	c.mu.RUnlock()

	// 캐시 미스 - DB 로드
	vertex := c.db.getOrCreateVertexNoLock(addr)
	c.Put(addr, vertex)

	c.mu.Lock()
	c.misses++
	c.mu.Unlock()

	return vertex
}

// Put - 버텍스 저장 (캐시만)
func (c *VertexCache) Put(addr shareddomain.Address, vertex *domain.Vertex) {
	key := addr.String()

	c.mu.Lock()
	defer c.mu.Unlock()

	// 기존 아이템 업데이트
	if item, exists := c.items[key]; exists {
		item.vertex = vertex
		item.lastUsed = time.Now()
		c.lruList.MoveToFront(item.element)
		return
	}

	// 캐시 용량 초과시 LRU 제거
	if len(c.items) >= c.maxSize {
		c.evictLRU()
	}

	// 새 아이템 추가
	element := c.lruList.PushFront(key)
	c.items[key] = &cacheItem{
		key:      key,
		vertex:   vertex,
		element:  element,
		lastUsed: time.Now(),
	}
}

// FlushAll - 캐시의 모든 데이터를 DB에 BatchWrite
func (c *VertexCache) FlushAll() int {
	c.mu.RLock()
	if len(c.items) == 0 {
		c.mu.RUnlock()
		return 0
	}

	// 모든 버텍스 복사
	vertices := make([]*domain.Vertex, 0, len(c.items))
	for _, item := range c.items {
		vertices = append(vertices, item.vertex)
	}
	c.mu.RUnlock()

	// BatchWrite로 한번에 처리
	c.batchWriteVertices(vertices)

	return len(vertices)
}

// evictLRU - 가장 오래된 아이템 제거 후 즉시 DB 저장
func (c *VertexCache) evictLRU() {
	if c.lruList.Len() == 0 {
		return
	}

	lastElement := c.lruList.Back()
	key := lastElement.Value.(string)
	item := c.items[key]

	// 제거되는 버텍스는 즉시 DB에 저장
	c.db.putVertexNoLock(item.vertex)

	// 캐시에서 제거
	c.lruList.Remove(lastElement)
	delete(c.items, key)
}

// batchWriteVertices - 배치로 버텍스들을 DB에 저장
func (c *VertexCache) batchWriteVertices(vertices []*domain.Vertex) {

	err := c.db.writeVertices(vertices)

	if err != nil {
		// 배치 실패시 개별 저장으로 폴백
		for _, v := range vertices {
			c.db.putVertexNoLock(v)
		}
	}
}

// GetStats - 캐시 통계
func (c *VertexCache) GetStats() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	total := c.hits + c.misses
	hitRate := float64(0)
	if total > 0 {
		hitRate = float64(c.hits) / float64(total)
	}

	return map[string]interface{}{
		"size":    len(c.items),
		"maxSize": c.maxSize,
		"hits":    c.hits,
		"misses":  c.misses,
		"hitRate": hitRate,
	}
}

// Clear - 캐시 전체 비우기 (모든 데이터 DB 저장 후)
func (c *VertexCache) Clear() {
	c.FlushAll()

	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]*cacheItem)
	c.lruList.Init()
	c.hits = 0
	c.misses = 0
}

// Size - 현재 캐시 크기
func (c *VertexCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}
