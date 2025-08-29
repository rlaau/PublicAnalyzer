// file: internal/apool/rel/triplet/infra/contract_db.go
package infra

import (
	"encoding/json"
	"errors"
	"hash/fnv"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
)

const (
	// GC 체크 주기(연산 횟수 기준)
	GC_CHECK_INTERVAL = 1_000_000
	// Badger GC 팩터
	GC_FACTOR = 0.3

	// 샤드 개수(2의 거듭제곱 권장)
	defaultShardCount = 256
)

var (
	errIterationStopped = errors.New("iteration_stopped")
)

// ContractDB: BadgerDB 기반 Contract 저장소
// - 동시쓰기: Badger MVCC 재시도(최대 3회) + 샤드 뮤텍스(낙관적 락) fallback
// - 읽기: View 트랜잭션(락 없음)
// - 주기적 RunValueLogGC
type ContractDB struct {
	db           *badger.DB
	operationCnt uint64

	// GC 보호용
	gcCounter sync.RWMutex

	// 낙관적 락(샤드)
	shards     []sync.Mutex
	shardMask  uint32
	maxRetries int
}

// NewContractDB: 프로세싱 모드에 따라 경로 선택
func NewContractDB(processMode mode.ProcessingMode, rootPath string) (*ContractDB, error) {
	var root string
	if rootPath == "" {
		if processMode.IsTest() {
			root = computation.FindTestingStorageRootPath() + "/nod/co/cont"
		} else {
			root = computation.FindProductionStorageRootPath() + "/nod/co/cont"
		}
	} else {
		root = rootPath
	}

	opts := badger.DefaultOptions(root)
	// 내구성 우선(처리량 우선이면 false 고려)
	opts.SyncWrites = true
	opts.CompactL0OnClose = true
	opts.Logger = nil

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	shards := make([]sync.Mutex, defaultShardCount)

	return &ContractDB{
		db:           db,
		operationCnt: 0,
		shards:       shards,
		shardMask:    uint32(defaultShardCount - 1),
		maxRetries:   3,
	}, nil
}

// NewContractDBWithPath: 명시 경로 버전
func NewContractDBWithPath(path string) (*ContractDB, error) {
	opts := badger.DefaultOptions(path)
	opts.SyncWrites = true
	opts.CompactL0OnClose = true
	opts.Logger = nil

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	shards := make([]sync.Mutex, defaultShardCount)

	return &ContractDB{
		db:           db,
		operationCnt: 0,
		shards:       shards,
		shardMask:    uint32(defaultShardCount - 1),
		maxRetries:   3,
	}, nil
}

// Close DB 종료
func (cdb *ContractDB) Close() error { return cdb.db.Close() }

// 연산 카운터 증가 및 GC 트리거
func (cdb *ContractDB) incrementOperationCount() {
	count := atomic.AddUint64(&cdb.operationCnt, 1)
	if count%GC_CHECK_INTERVAL == 0 {
		go cdb.checkAndRunGC()
	}
}

// GC 조건 체크 후 실행
func (cdb *ContractDB) checkAndRunGC() {
	cdb.gcCounter.Lock()
	defer cdb.gcCounter.Unlock()

	// 통계 문자열(모니터링용으로 추후 활용 가능)
	_ = cdb.db.LevelsToString()

	// 필요 시에만 GC 수행(필요 없으면 ErrNoRewrite 또는 no cleanup 메시지)
	if err := cdb.db.RunValueLogGC(GC_FACTOR); err != nil &&
		err != badger.ErrNoRewrite &&
		err.Error() != "Value log GC attempt didn't result in any cleanup" {
		return
	}
}

// ForceGC 강제 GC
func (cdb *ContractDB) ForceGC() error {
	cdb.gcCounter.Lock()
	defer cdb.gcCounter.Unlock()
	return cdb.db.RunValueLogGC(GC_FACTOR)
}

// GetOperationCount 현재 연산 횟수
func (cdb *ContractDB) GetOperationCount() uint64 { return atomic.LoadUint64(&cdb.operationCnt) }

// 직렬화/역직렬화
func (cdb *ContractDB) serializeContract(contract domain.Contract) ([]byte, error) {
	return json.Marshal(contract)
}
func (cdb *ContractDB) deserializeContract(data []byte) (domain.Contract, error) {
	var contract domain.Contract
	err := json.Unmarshal(data, &contract)
	return contract, err
}

// 키 생성
func (cdb *ContractDB) addressToKey(addr domain.Address) []byte {
	// addr.String()이 일관된 hex를 반환한다고 가정
	return []byte("contract:" + addr.String())
}

// ---- MVCC 재시도 + 샤드락 fallback 유틸 ----

func (cdb *ContractDB) shardIndex(key []byte) uint32 {
	h := fnv.New32a()
	_, _ = h.Write(key)
	return h.Sum32() & cdb.shardMask
}

// 단일 키 작업: MVCC 3회 재시도 후 해당 샤드 락을 잡고 1회 더 시도
func (cdb *ContractDB) doWithRetriesAndShard(key []byte, fn func(txn *badger.Txn) error) error {
	var err error
	for i := 0; i < cdb.maxRetries; i++ {
		err = cdb.db.Update(func(txn *badger.Txn) error { return fn(txn) })
		if err == nil {
			return nil
		}
		if !errors.Is(err, badger.ErrConflict) {
			return err
		}
	}
	ix := cdb.shardIndex(key)
	cdb.shards[ix].Lock()
	defer cdb.shards[ix].Unlock()
	return cdb.db.Update(func(txn *badger.Txn) error { return fn(txn) })
}

// 다중 키 작업: MVCC 3회 재시도 후, 관련 샤드들을 정렬해 모두 락 잡고 1회 더 시도(데드락 방지)
func (cdb *ContractDB) doWithRetriesAndShards(keys [][]byte, fn func(txn *badger.Txn) error) error {
	var err error
	for i := 0; i < cdb.maxRetries; i++ {
		err = cdb.db.Update(func(txn *badger.Txn) error { return fn(txn) })
		if err == nil {
			return nil
		}
		if !errors.Is(err, badger.ErrConflict) {
			return err
		}
	}
	if len(keys) > 0 {
		uniq := make(map[uint32]struct{}, len(keys))
		order := make([]uint32, 0, len(keys))
		for _, k := range keys {
			ix := cdb.shardIndex(k)
			if _, ok := uniq[ix]; !ok {
				uniq[ix] = struct{}{}
				order = append(order, ix)
			}
		}
		sort.Slice(order, func(i, j int) bool { return order[i] < order[j] })
		for _, ix := range order {
			cdb.shards[ix].Lock()
		}
		defer func() {
			for i := len(order) - 1; i >= 0; i-- {
				cdb.shards[order[i]].Unlock()
			}
		}()
	}
	return cdb.db.Update(func(txn *badger.Txn) error { return fn(txn) })
}

// ---- CRUD ----

// Put: 단일 upsert
func (cdb *ContractDB) Put(contract domain.Contract) error {
	cdb.incrementOperationCount()

	key := cdb.addressToKey(contract.Address)
	data, err := cdb.serializeContract(contract)
	if err != nil {
		return err
	}
	return cdb.doWithRetriesAndShard(key, func(txn *badger.Txn) error {
		return txn.Set(key, data)
	})
}

// Get: 단일 조회
func (cdb *ContractDB) Get(addr domain.Address) (domain.Contract, error) {
	cdb.incrementOperationCount()

	var contract domain.Contract
	key := cdb.addressToKey(addr)
	err := cdb.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var e error
			contract, e = cdb.deserializeContract(val)
			return e
		})
	})
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return contract, errors.New("contract not found")
		}
		return contract, err
	}
	return contract, nil
}

// IsContain: 존재 여부
func (cdb *ContractDB) IsContain(addr domain.Address) bool {
	cdb.incrementOperationCount()

	key := cdb.addressToKey(addr)
	err := cdb.db.View(func(txn *badger.Txn) error {
		_, e := txn.Get(key)
		return e
	})
	return err == nil
}

// BatchPut: 다중 upsert
func (cdb *ContractDB) BatchPut(contracts []domain.Contract) error {
	if len(contracts) == 0 {
		return nil
	}
	for i := 0; i < len(contracts); i++ {
		cdb.incrementOperationCount()
	}

	keys := make([][]byte, len(contracts))
	vals := make([][]byte, len(contracts))
	for i, ct := range contracts {
		k := cdb.addressToKey(ct.Address)
		d, err := cdb.serializeContract(ct)
		if err != nil {
			return err
		}
		keys[i] = k
		vals[i] = d
	}
	return cdb.doWithRetriesAndShards(keys, func(txn *badger.Txn) error {
		for i := range keys {
			if err := txn.Set(keys[i], vals[i]); err != nil {
				return err
			}
		}
		return nil
	})
}

// BatchGet: 다중 조회(존재하지 않는 키는 건너뜀)
func (cdb *ContractDB) BatchGet(addresses []domain.Address) (map[domain.Address]domain.Contract, error) {
	if len(addresses) == 0 {
		return make(map[domain.Address]domain.Contract), nil
	}
	results := make(map[domain.Address]domain.Contract)
	err := cdb.db.View(func(txn *badger.Txn) error {
		for _, addr := range addresses {
			key := cdb.addressToKey(addr)
			item, err := txn.Get(key)
			if err != nil {
				if errors.Is(err, badger.ErrKeyNotFound) {
					continue
				}
				return err
			}
			if err := item.Value(func(val []byte) error {
				ct, e := cdb.deserializeContract(val)
				if e != nil {
					return e
				}
				results[addr] = ct
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	return results, err
}

// GetAll: 모든 Contract 조회(개발/디버깅용)
func (cdb *ContractDB) GetAll() ([]domain.Contract, error) {
	var contracts []domain.Contract
	err := cdb.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte("contract:")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			if err := item.Value(func(v []byte) error {
				contract, e := cdb.deserializeContract(v)
				if e != nil {
					return e
				}
				contracts = append(contracts, contract)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	return contracts, err
}

// Count: 전체 개수
func (cdb *ContractDB) Count() (int, error) {
	count := 0
	err := cdb.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte("contract:")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			count++
		}
		return nil
	})
	return count, err
}

// Delete: 단건 삭제
func (cdb *ContractDB) Delete(addr domain.Address) error {
	cdb.incrementOperationCount()

	key := cdb.addressToKey(addr)
	return cdb.doWithRetriesAndShard(key, func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

// BatchDelete: 다중 삭제
func (cdb *ContractDB) BatchDelete(addresses []domain.Address) error {
	if len(addresses) == 0 {
		return nil
	}
	for i := 0; i < len(addresses); i++ {
		cdb.incrementOperationCount()
	}

	keys := make([][]byte, 0, len(addresses))
	for _, a := range addresses {
		keys = append(keys, cdb.addressToKey(a))
	}
	return cdb.doWithRetriesAndShards(keys, func(txn *badger.Txn) error {
		for _, k := range keys {
			if err := txn.Delete(k); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
		}
		return nil
	})
}

// IterateContracts: 효율적 순회(콜백 false 반환 시 중단)
func (cdb *ContractDB) IterateContracts(callback func(position int, contract domain.Contract) bool) error {
	position := 0
	err := cdb.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 100
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte("contract:")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			if err := item.Value(func(v []byte) error {
				ct, e := cdb.deserializeContract(v)
				if e != nil {
					return e
				}
				if !callback(position, ct) {
					return errIterationStopped
				}
				position++
				return nil
			}); err != nil {
				if errors.Is(err, errIterationStopped) {
					return nil // 정상 중단
				}
				return err
			}
		}
		return nil
	})
	return err
}
