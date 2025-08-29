package infra

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/dgraph-io/badger/v4"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	shareddomain "github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
)

//TODO 현재 Other 타입 대해선 작업을 하지 않음
//TODO 관계 분석에 대해 "심화 정보"가 굳이 필요하지 않은 경우
//TODO 시각화 단계에서 이더스캔을 DB로 삼은 디멘디드 처리가 가능함!

// -------------------- OtherCoDB (샤드 기반 OLC) --------------------
// key layout:
//
//	infra      -> "tk" + 20B addr
//	ShardEpoch -> "tk!shard:" + u16(shardID)  (샤드별 에폭 카운터; 쓰기 충돌 검출 지점)
//	// NFT     -> "nf" + ...
//	// Proto   -> "pr" + ...
type OtherCoDB struct {
	db     *badger.DB
	shards uint16 // ex) 256 or 1024
}

func OpenOtherCoDB(dir string, shards uint16) (*OtherCoDB, error) {
	if shards == 0 {
		shards = 256
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir: %w", err)
	}
	opts := badger.DefaultOptions(filepath.Clean(dir)).
		WithValueThreshold(1024). // 토큰 row 작으면 인라인 선호
		WithNumVersionsToKeep(1).
		WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open badger: %w", err)
	}
	return &OtherCoDB{db: db, shards: shards}, nil
}

func (o *OtherCoDB) Close() error {
	if o == nil || o.db == nil {
		return nil
	}
	return o.db.Close()
}

func (o *OtherCoDB) DB() *badger.DB { return o.db }

// -------------------- 내부 키/인코딩 --------------------

const (
	prefixToken = "tk"
	prefixShard = "tk!shard:"
)

func tkKey(addr shareddomain.Address) []byte {
	var k [2 + 20]byte
	k[0], k[1] = 't', 'k'
	copy(k[2:], addr[:])
	return k[:]
}

func shardKey(id uint16) []byte {
	// "tk!shard:" + 2B LE
	k := make([]byte, len(prefixShard)+2)
	copy(k, prefixShard)
	binary.LittleEndian.PutUint16(k[len(prefixShard):], id)
	return k
}

func (o *OtherCoDB) shardID(addr shareddomain.Address) uint16 {
	// 간단한 해시: 상위 2바이트 → mod shards
	h := binary.LittleEndian.Uint16(addr[0:2])
	return h % o.shards
}

// value: [ symLen uvarint | sym | nameLen uvarint | name | decimals int64 LE ]
func encodeTokenRow(t domain.Token) []byte {
	sym := []byte(t.Symbol)
	name := []byte(t.Name)

	buf := make([]byte, 0, len(sym)+len(name)+8+20)
	buf = binary.AppendUvarint(buf, uint64(len(sym)))
	buf = append(buf, sym...)
	buf = binary.AppendUvarint(buf, uint64(len(name)))
	buf = append(buf, name...)
	var dec [8]byte
	binary.LittleEndian.PutUint64(dec[:], uint64(t.DecimalsOrZero))
	buf = append(buf, dec[:]...)
	return buf
}

func decodeTokenRow(addr shareddomain.Address, b []byte) (domain.Token, error) {
	var off int
	readU := func() (uint64, error) {
		v, n := binary.Uvarint(b[off:])
		if n <= 0 {
			return 0, errors.New("bad uvarint")
		}
		off += n
		return v, nil
	}
	need := func(n int) error {
		if off+n > len(b) {
			return errors.New("underflow")
		}
		return nil
	}
	sz, err := readU()
	if err != nil {
		return domain.Token{}, err
	}
	if err := need(int(sz)); err != nil {
		return domain.Token{}, err
	}
	sym := string(b[off : off+int(sz)])
	off += int(sz)

	sz, err = readU()
	if err != nil {
		return domain.Token{}, err
	}
	if err := need(int(sz)); err != nil {
		return domain.Token{}, err
	}
	name := string(b[off : off+int(sz)])
	off += int(sz)

	if err := need(8); err != nil {
		return domain.Token{}, err
	}
	dec := int64(binary.LittleEndian.Uint64(b[off:]))
	off += 8

	return domain.Token{
		Address:        addr,
		Symbol:         sym,
		Name:           name,
		DecimalsOrZero: dec,
	}, nil
}

// -------------------- 샤드 기반 낙관적 락(에폭 CAS) --------------------
// 트랜잭션 내에서 순서:
//   1) shardEpoch read (없으면 0 가정)
//   2) Token read/mutate
//   3) Token write
//   4) shardEpoch write(epoch+1)   ← 동시 업데이트는 여기서 ErrConflict로 충돌

func (o *OtherCoDB) PutToken(ctx context.Context, t domain.Token) error {
	key := tkKey(t.Address)
	epochKey := shardKey(o.shardID(t.Address))
	val := encodeTokenRow(t)

	for attempt := 0; attempt < 3; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		err := o.db.Update(func(txn *badger.Txn) error {
			// 1) read shard epoch
			curEpoch := uint64(0)
			if it, err := txn.Get(epochKey); err == nil {
				if vb, err := it.ValueCopy(nil); err == nil && len(vb) == 8 {
					curEpoch = binary.LittleEndian.Uint64(vb)
				}
			} else if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
			// 2) write token
			if err := txn.SetEntry(badger.NewEntry(key, val)); err != nil {
				return err
			}
			// 3) bump shard epoch
			var nb [8]byte
			binary.LittleEndian.PutUint64(nb[:], curEpoch+1)
			return txn.SetEntry(badger.NewEntry(epochKey, nb[:]))
		})
		if err == nil {
			return nil
		}
		if errors.Is(err, badger.ErrConflict) {
			continue // 낙관 충돌 → 재시도
		}
		return err
	}
	return fmt.Errorf("PutToken: conflict after retries")
}

func (o *OtherCoDB) UpdateToken(ctx context.Context, addr shareddomain.Address, mut func(cur domain.Token) (domain.Token, error)) error {
	key := tkKey(addr)
	epochKey := shardKey(o.shardID(addr))

	for attempt := 0; attempt < 3; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		err := o.db.Update(func(txn *badger.Txn) error {
			// 1) read shard epoch
			curEpoch := uint64(0)
			if it, err := txn.Get(epochKey); err == nil {
				if vb, err := it.ValueCopy(nil); err == nil && len(vb) == 8 {
					curEpoch = binary.LittleEndian.Uint64(vb)
				}
			} else if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}

			// 2) read current token (없으면 빈값)
			var cur domain.Token
			if it, err := txn.Get(key); err == nil {
				vb, err := it.ValueCopy(nil)
				if err != nil {
					return err
				}
				row, err := decodeTokenRow(addr, vb)
				if err != nil {
					return err
				}
				cur = row
			} else if errors.Is(err, badger.ErrKeyNotFound) {
				cur = domain.Token{Address: addr}
			} else if err != nil {
				return err
			}

			// 3) mutate
			next, err := mut(cur)
			if err != nil {
				return err
			}
			if next.Address != addr {
				return errors.New("UpdateToken: address mutated")
			}

			// 4) write token
			if err := txn.SetEntry(badger.NewEntry(key, encodeTokenRow(next))); err != nil {
				return err
			}
			// 5) bump shard epoch → 동시 쓰기 충돌은 여기서 ErrConflict
			var nb [8]byte
			binary.LittleEndian.PutUint64(nb[:], curEpoch+1)
			return txn.SetEntry(badger.NewEntry(epochKey, nb[:]))
		})
		if err == nil {
			return nil
		}
		if errors.Is(err, badger.ErrConflict) {
			continue
		}
		return err
	}
	return fmt.Errorf("UpdateToken: conflict after retries")
}

func (o *OtherCoDB) GetToken(ctx context.Context, addr shareddomain.Address) (domain.Token, error) {
	key := tkKey(addr)
	var out domain.Token
	err := o.db.View(func(txn *badger.Txn) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		it, err := txn.Get(key)
		if err != nil {
			return err
		}
		vb, err := it.ValueCopy(nil)
		if err != nil {
			return err
		}
		row, err := decodeTokenRow(addr, vb)
		if err != nil {
			return err
		}
		out = row
		return nil
	})
	return out, err
}
