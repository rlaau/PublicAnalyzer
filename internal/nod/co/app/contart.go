package app

import (
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/mode"
)

const (
	// GC_CHECK_INTERVAL GC 체크 주기 (연산 횟수 기준)
	GC_CHECK_INTERVAL = 1000000 // 100만번마다 GC 체크
	// GC_FACTOR GC 실행 조건 팩터
	GC_FACTOR = 0.3 // 30% 임계점에서 GC 실행
)

// ContractDB BadgerDB 기반 Contract 저장소
type ContractDB struct {
	db           *badger.DB
	mu           sync.RWMutex // 병렬 접근 보호
	operationCnt uint64       // 연산 횟수 카운터
	gcCounter    sync.RWMutex // GC 카운터 보호
}

// NewContractDB 새로운 ContractDB 인스턴스 생성
func NewContractDB(processMode mode.ProcessingMode) (*ContractDB, error) {
	var root string
	if processMode.IsTest() {
		root = computation.FindTestingStorageRootPath() + "/cont"
	} else {
		root = computation.FindProductionStorageRootPath() + "/cont"
	}
	opts := badger.DefaultOptions(root)
	opts.SyncWrites = true // 데이터 내구성 보장
	opts.CompactL0OnClose = true
	opts.Logger = nil // 로그 비활성화 (필요시 활성화 가능)

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &ContractDB{
		db:           db,
		operationCnt: 0,
	}, nil
}

// Close 데이터베이스 연결 종료
func (cdb *ContractDB) Close() error {
	return cdb.db.Close()
}

// incrementOperationCount 연산 횟수를 증가시키고 GC 체크를 수행
func (cdb *ContractDB) incrementOperationCount() {
	count := atomic.AddUint64(&cdb.operationCnt, 1)

	// GC_CHECK_INTERVAL마다 GC 체크
	if count%GC_CHECK_INTERVAL == 0 {
		go cdb.checkAndRunGC()
	}
}

// checkAndRunGC GC 조건 체크 후 필요시 GC 실행
func (cdb *ContractDB) checkAndRunGC() {
	cdb.gcCounter.Lock()
	defer cdb.gcCounter.Unlock()

	// BadgerDB의 LSM tree 통계 확인
	lsm := cdb.db.LevelsToString()

	// GC 실행 (BadgerDB 내부적으로 필요성 판단)
	err := cdb.db.RunValueLogGC(GC_FACTOR)
	if err != nil && err != badger.ErrNoRewrite &&
		err.Error() != "Value log GC attempt didn't result in any cleanup" {
		// ErrNoRewrite나 cleanup 불필요 메시지는 GC가 불필요한 경우이므로 무시
		// 실제 에러만 로그 (여기서는 무시하고 계속 진행)
		return
	}

	_ = lsm // 통계 정보는 향후 모니터링용으로 활용 가능
}

// ForceGC 강제 GC 실행 (테스트 또는 관리용)
func (cdb *ContractDB) ForceGC() error {
	cdb.gcCounter.Lock()
	defer cdb.gcCounter.Unlock()

	return cdb.db.RunValueLogGC(GC_FACTOR)
}

// GetOperationCount 현재 연산 횟수 반환
func (cdb *ContractDB) GetOperationCount() uint64 {
	return atomic.LoadUint64(&cdb.operationCnt)
}

// serializeContract Contract를 JSON으로 직렬화
func (cdb *ContractDB) serializeContract(contract domain.Contract) ([]byte, error) {
	return json.Marshal(contract)
}

// deserializeContract JSON을 Contract로 역직렬화
func (cdb *ContractDB) deserializeContract(data []byte) (domain.Contract, error) {
	var contract domain.Contract
	err := json.Unmarshal(data, &contract)
	return contract, err
}

// addressToKey Address를 BadgerDB 키로 변환
func (cdb *ContractDB) addressToKey(addr domain.Address) []byte {
	return []byte("contract:" + addr.String())
}

// Put Contract를 저장
func (cdb *ContractDB) Put(contract domain.Contract) error {
	cdb.mu.Lock()
	defer cdb.mu.Unlock()

	// 연산 횟수 증가 및 GC 체크
	cdb.incrementOperationCount()

	key := cdb.addressToKey(contract.Address)
	data, err := cdb.serializeContract(contract)
	if err != nil {
		return err
	}

	return cdb.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, data)
	})
}

// Get Address로 Contract 조회
func (cdb *ContractDB) Get(addr domain.Address) (domain.Contract, error) {
	cdb.mu.RLock()
	defer cdb.mu.RUnlock()

	// 연산 횟수 증가 및 GC 체크
	cdb.incrementOperationCount()

	var contract domain.Contract
	key := cdb.addressToKey(addr)

	err := cdb.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			contract, err = cdb.deserializeContract(val)
			return err
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

// IsContain Address가 존재하는지 확인
func (cdb *ContractDB) IsContain(addr domain.Address) bool {
	cdb.mu.RLock()
	defer cdb.mu.RUnlock()

	// 연산 횟수 증가 및 GC 체크
	cdb.incrementOperationCount()

	key := cdb.addressToKey(addr)

	err := cdb.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		return err
	})

	return err == nil
}

// BatchPut 여러 Contract를 배치로 저장
func (cdb *ContractDB) BatchPut(contracts []domain.Contract) error {
	if len(contracts) == 0 {
		return nil
	}

	cdb.mu.Lock()
	defer cdb.mu.Unlock()

	// 배치 연산의 경우 contracts 길이만큼 연산 횟수 증가
	for i := 0; i < len(contracts); i++ {
		cdb.incrementOperationCount()
	}

	return cdb.db.Update(func(txn *badger.Txn) error {
		for _, contract := range contracts {
			key := cdb.addressToKey(contract.Address)
			data, err := cdb.serializeContract(contract)
			if err != nil {
				return err
			}

			err = txn.Set(key, data)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// BatchGet 여러 Address로 Contract들을 배치로 조회
func (cdb *ContractDB) BatchGet(addresses []domain.Address) (map[domain.Address]domain.Contract, error) {
	if len(addresses) == 0 {
		return make(map[domain.Address]domain.Contract), nil
	}

	cdb.mu.RLock()
	defer cdb.mu.RUnlock()

	results := make(map[domain.Address]domain.Contract)

	err := cdb.db.View(func(txn *badger.Txn) error {
		for _, addr := range addresses {
			key := cdb.addressToKey(addr)
			item, err := txn.Get(key)
			if err != nil {
				// 키가 없으면 결과에서 제외하고 계속 진행
				if errors.Is(err, badger.ErrKeyNotFound) {
					continue
				}
				return err
			}

			err = item.Value(func(val []byte) error {
				contract, err := cdb.deserializeContract(val)
				if err != nil {
					return err
				}
				results[addr] = contract
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	return results, err
}

// GetAll 모든 Contract를 조회 (개발/디버깅용)
func (cdb *ContractDB) GetAll() ([]domain.Contract, error) {
	cdb.mu.RLock()
	defer cdb.mu.RUnlock()

	var contracts []domain.Contract

	err := cdb.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte("contract:")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			err := item.Value(func(v []byte) error {
				contract, err := cdb.deserializeContract(v)
				if err != nil {
					return err
				}
				contracts = append(contracts, contract)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	return contracts, err
}

// Count 저장된 Contract 개수 반환
func (cdb *ContractDB) Count() (int, error) {
	cdb.mu.RLock()
	defer cdb.mu.RUnlock()

	count := 0

	err := cdb.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // 값은 필요없고 키만 카운트
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

// Delete Contract 삭제
func (cdb *ContractDB) Delete(addr domain.Address) error {
	cdb.mu.Lock()
	defer cdb.mu.Unlock()

	// 연산 횟수 증가 및 GC 체크
	cdb.incrementOperationCount()

	key := cdb.addressToKey(addr)

	return cdb.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

// BatchDelete 여러 Contract를 배치로 삭제
func (cdb *ContractDB) BatchDelete(addresses []domain.Address) error {
	if len(addresses) == 0 {
		return nil
	}

	cdb.mu.Lock()
	defer cdb.mu.Unlock()

	// 배치 연산의 경우 addresses 길이만큼 연산 횟수 증가
	for i := 0; i < len(addresses); i++ {
		cdb.incrementOperationCount()
	}

	return cdb.db.Update(func(txn *badger.Txn) error {
		for _, addr := range addresses {
			key := cdb.addressToKey(addr)
			err := txn.Delete(key)
			if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
		}
		return nil
	})
}

// IterateContracts 효율적인 Contract iteration (콜백 함수 사용)
// callback 함수는 position과 contract를 받고, 계속 진행할지 여부를 bool로 반환
func (cdb *ContractDB) IterateContracts(callback func(position int, contract domain.Contract) bool) error {
	cdb.mu.RLock()
	defer cdb.mu.RUnlock()

	position := 0

	err := cdb.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 100 // 배치로 미리 가져와서 성능 향상
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte("contract:")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			err := item.Value(func(v []byte) error {
				contract, err := cdb.deserializeContract(v)
				if err != nil {
					return err
				}

				// 콜백 함수 호출 - false를 반환하면 iteration 중단
				if !callback(position, contract) {
					return errors.New("iteration_stopped") // 특별한 에러로 정상 중단 표시
				}

				position++
				return nil
			})
			if err != nil {
				if err.Error() == "iteration_stopped" {
					return nil // 정상 중단
				}
				return err
			}
		}
		return nil
	})

	return err
}

// NewContractDBWithPath 새로운 ContractDB 인스턴스 생성
func NewContractDBWithPath(path string) (*ContractDB, error) {

	opts := badger.DefaultOptions(path)
	opts.SyncWrites = true // 데이터 내구성 보장
	opts.CompactL0OnClose = true
	opts.Logger = nil // 로그 비활성화 (필요시 활성화 가능)

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &ContractDB{
		db:           db,
		operationCnt: 0,
	}, nil
}
