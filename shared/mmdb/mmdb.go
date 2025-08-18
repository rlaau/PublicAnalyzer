// Package mmdb: a small mmap-based, sharded, append-only KV store (library)
//
//   - Key:   uint32 (monotonic increasing per shard; globally monotonic if caller enforces)
//   - Value: fixed-size blob; size can be set by Options.ValueSize, or **auto-inferred** from a POD value type
//     (no pointers / no variable-length fields) via Options.ValueType, WithValueType[T], or OpenTypedPOD[T].
//   - Ops:   Append (instead of Insert), Get, Update
//   - Sharding: id % NumShards
//   - Segments: each shard is split into rolling segments; old segments are quasi-immutable
//   - Concurrency: shard-level locks → parallelism across shards; SWMR within a shard
//   - Search: binary search over segments by [minKey, maxKey], then binary search inside segment
//
// Examples
//
//  1. Bytes API with type auto-sizing at Open time (runtime reflect):
//     opts := mmdb.Options{Dir: "./mmdbdata", NumShards: 8, SegmentCapacity: 100_000}
//     opts.ValueType = int32(0) // any zero value of the type you want to store
//     db, _ := mmdb.Open(opts)
//     defer db.Close()
//     _ = db.Append(100, []byte{0x2a,0,0,0})
//
//  2. Typed API with **POD** auto-size + zero-copy codec (fast & fixed-size only):
//     tdb, _ := mmdb.OpenTypedPOD[int32](mmdb.Options{Dir: "./mmdb", NumShards: 8, SegmentCapacity: 100_000})
//     defer tdb.Close()
//     _ = tdb.Append(101, 42)
//     v, ok := tdb.Get(101)
//     _ = tdb.Update(101, 7)
//
// Notes / Scope:
//   - "POD" here means **no pointers and no variable-length components** (no string/slice/map/interface/chan/func/ptr).
//   - POD auto mode stores raw bytes of the Go value (including padding, arch-size of int/uint). This is ultra-fast but
//     ties on-disk layout to the current architecture/Go version. For portable on-disk encoding, provide your own Codec[T]
//     (e.g., LE-encoded fields) and use OpenTyped[T].
//   - No crash recovery / WAL. Header edits rely on OS page cache; call Close() to flush mmap.
//   - Caller must ensure per-shard key monotonicity (use the same id%NumShards rule for all ops).
package mmdb

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
	"unsafe"

	mmap "github.com/edsrzf/mmap-go"
)

// ================================ Options & DB ================================

type Options struct {
	// Dir is the root directory for the DB (segments will be created per shard).
	Dir string
	// NumShards >= 1. Keys are assigned to a shard via id % NumShards.
	NumShards int
	// SegmentCapacity is the number of records per segment file per shard.
	SegmentCapacity uint32
	// ValueSize is the fixed size in bytes for the value payload of each record.
	// If 0 and ValueType is provided, it will be inferred automatically from the value type (POD-only).
	ValueSize int
	// ValueType (optional) – any zero value of the type you want to store; when set, Open() attempts
	// to auto-compute ValueSize via reflection. Disallowed: string, slice, map, interface, func,
	// pointer, chan, unsafe.Pointer, or structs/arrays containing any of those.
	ValueType any
	// File permissions for new files/directories (default 0o755 for dirs, 0o644 for files).
	DirPerm  os.FileMode
	FilePerm os.FileMode
}

func (o *Options) setDefaults() {
	if o.NumShards <= 0 {
		o.NumShards = 1
	}
	if o.SegmentCapacity == 0 {
		o.SegmentCapacity = 128_000
	}
	if o.ValueSize < 0 {
		o.ValueSize = 0
	}
	if o.DirPerm == 0 {
		o.DirPerm = 0o755
	}
	if o.FilePerm == 0 {
		o.FilePerm = 0o644
	}
}

// WithValueType sets ValueType and auto-resolves ValueSize (POD-only). Returns updated options or error.
func WithValueType[T any](opts Options) (Options, error) {
	var zero T
	sz, err := fixedPODSizeOf(reflect.TypeOf(zero))
	if err != nil {
		return opts, err
	}
	opts.ValueType = zero
	opts.ValueSize = sz
	return opts, nil
}

// SetValueType mutates opts in-place to a specific runtime sample value.
func SetValueType(opts *Options, sample any) error {
	if opts == nil {
		return errors.New("nil options")
	}
	if sample == nil {
		return errors.New("nil sample")
	}
	sz, err := fixedPODSizeOf(reflect.TypeOf(sample))
	if err != nil {
		return err
	}
	opts.ValueType = sample
	opts.ValueSize = sz
	return nil
}

// DB is the top-level handle.
type DB struct {
	opts   Options
	shards []*shard
}

// Open opens (or creates) a DB according to Options.
func Open(opts Options) (*DB, error) {
	opts.setDefaults()

	// Auto-resolve ValueSize from ValueType if provided
	if opts.ValueSize == 0 && opts.ValueType != nil {
		sz, err := fixedPODSizeOf(reflect.TypeOf(opts.ValueType))
		if err != nil {
			return nil, err
		}
		opts.ValueSize = sz
	}
	if opts.ValueSize <= 0 {
		return nil, fmt.Errorf("invalid ValueSize %d (set Options.ValueSize or provide Options.ValueType)", opts.ValueSize)
	}

	if err := os.MkdirAll(opts.Dir, opts.DirPerm); err != nil {
		return nil, err
	}

	db := &DB{opts: opts}
	db.shards = make([]*shard, opts.NumShards)
	for i := 0; i < opts.NumShards; i++ {
		sh, err := openShard(opts, i)
		if err != nil {
			for j := 0; j < i; j++ {
				_ = db.shards[j].close()
			}
			return nil, err
		}
		db.shards[i] = sh
	}
	return db, nil
}

func (db *DB) Close() error {
	var firstErr error
	for _, sh := range db.shards {
		if err := sh.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (db *DB) shardOf(id uint32) *shard { return db.shards[int(id%uint32(db.opts.NumShards))] }

// Append appends (id,value). Caller must keep ids monotonic (strictly increasing) per shard.
func (db *DB) Append(id uint32, value []byte) error {
	if len(value) != db.opts.ValueSize {
		return fmt.Errorf("value size %d != Options.ValueSize %d", len(value), db.opts.ValueSize)
	}
	return db.shardOf(id).Append(id, value)
}

// Update overwrites the value for id if present. Returns true if updated.
func (db *DB) Update(id uint32, value []byte) bool {
	if len(value) != db.opts.ValueSize {
		return false
	}
	return db.shardOf(id).Update(id, value)
}

// Get fills dst (must be len == ValueSize) and returns true if found.
func (db *DB) Get(id uint32, dst []byte) bool { return db.shardOf(id).Get(id, dst) }

// GetCopy returns a freshly-allocated copy of the value.
func (db *DB) GetCopy(id uint32) ([]byte, bool) {
	buf := make([]byte, db.opts.ValueSize)
	if !db.Get(id, buf) {
		return nil, false
	}
	return buf, true
}

// ================================ Typed wrappers ==============================

// Codec encodes/decodes a static-size value into a fixed-size byte slice.
type Codec[T any] interface {
	Size() int
	Encode(dst []byte, v T)
	Decode(src []byte) T
}

// TypedDB exposes a typed API backed by the raw DB.
type TypedDB[T any] struct {
	raw   *DB
	codec Codec[T]
}

// OpenTyped sets ValueSize from codec and returns a typed DB (portable if your codec is portable).
func OpenTyped[T any](opts Options, codec Codec[T]) (*TypedDB[T], error) {
	opts.ValueSize = codec.Size()
	raw, err := Open(opts)
	if err != nil {
		return nil, err
	}
	return &TypedDB[T]{raw: raw, codec: codec}, nil
}

func WrapTyped[T any](raw *DB, codec Codec[T]) *TypedDB[T] {
	return &TypedDB[T]{raw: raw, codec: codec}
}

func (t *TypedDB[T]) Close() error { return t.raw.Close() }
func (t *TypedDB[T]) Append(id uint32, v T) error {
	buf := make([]byte, t.codec.Size())
	t.codec.Encode(buf, v)
	return t.raw.Append(id, buf)
}
func (t *TypedDB[T]) Update(id uint32, v T) bool {
	buf := make([]byte, t.codec.Size())
	t.codec.Encode(buf, v)
	return t.raw.Update(id, buf)
}
func (t *TypedDB[T]) Get(id uint32) (T, bool) {
	b, ok := t.raw.GetCopy(id)
	var z T
	if !ok {
		return z, false
	}
	return t.codec.Decode(b), true
}
func (t *TypedDB[T]) GetInto(id uint32, dst *T) bool {
	b, ok := t.raw.GetCopy(id)
	if !ok {
		return false
	}
	*dst = t.codec.Decode(b)
	return true
}

// Built-in codecs (portable LE for common scalars)

type Int32LECodec struct{}

func (Int32LECodec) Size() int                  { return 4 }
func (Int32LECodec) Encode(dst []byte, v int32) { binary.LittleEndian.PutUint32(dst, uint32(v)) }
func (Int32LECodec) Decode(src []byte) int32    { return int32(binary.LittleEndian.Uint32(src)) }

type Uint64LECodec struct{}

func (Uint64LECodec) Size() int                   { return 8 }
func (Uint64LECodec) Encode(dst []byte, v uint64) { binary.LittleEndian.PutUint64(dst, v) }
func (Uint64LECodec) Decode(src []byte) uint64    { return binary.LittleEndian.Uint64(src) }

// FixedBytesCodec treats values as raw []byte of a fixed length N.
type FixedBytesCodec struct{ N int }

func (c FixedBytesCodec) Size() int { return c.N }
func (c FixedBytesCodec) Encode(dst []byte, v []byte) {
	if len(v) != c.N {
		panic("FixedBytesCodec: length mismatch")
	}
	copy(dst, v)
}
func (c FixedBytesCodec) Decode(src []byte) []byte {
	out := make([]byte, c.N)
	copy(out, src)
	return out
}

// ================================ POD auto-codec ==============================
// PODCodec does raw byte copies of T. Only safe for POD types (no pointers/var-len components).
// Super fast, but on-disk layout is architecture/Go-version dependent.

type PODCodec[T any] struct{}

func (PODCodec[T]) Size() int { return int(unsafe.Sizeof(*new(T))) }

func (PODCodec[T]) Encode(dst []byte, v T) {
	sz := int(unsafe.Sizeof(v))
	if len(dst) != sz {
		panic("PODCodec: dst size mismatch")
	}
	b := unsafe.Slice((*byte)(unsafe.Pointer(&v)), sz)
	copy(dst, b)
}

func (PODCodec[T]) Decode(src []byte) T {
	var v T
	sz := int(unsafe.Sizeof(v))
	if len(src) != sz {
		panic("PODCodec: src size mismatch")
	}
	b := unsafe.Slice((*byte)(unsafe.Pointer(&v)), sz)
	copy(b, src)
	return v
}

// OpenTypedPOD opens a typed DB using PODCodec[T] with auto ValueSize. Errors if T is not POD.
func OpenTypedPOD[T any](opts Options) (*TypedDB[T], error) {
	var zero T
	if err := ensurePODType(reflect.TypeOf(zero)); err != nil {
		return nil, err
	}
	opts.ValueSize = int(unsafe.Sizeof(zero))
	raw, err := Open(opts)
	if err != nil {
		return nil, err
	}
	return &TypedDB[T]{raw: raw, codec: PODCodec[T]{}}, nil
}

// ================================ On-disk layout ==============================

const (
	segMagic   uint32 = 0x4D4D4442 // "MMDB"
	headerSize        = 32         // bytes; padded to 32 for alignment
)

const (
	offMagic    = 0  // u32
	offVersion  = 4  // u32
	offRecSize  = 8  // u32  (4 + valueSize)
	offCapacity = 12 // u32  (#records)
	offCount    = 16 // u32  (#records written)
	offMinKey   = 20 // u32
	offMaxKey   = 24 // u32
	// 28..31 reserved
)

// ================================ Segment =====================================

type segment struct {
	path     string
	file     *os.File
	m        mmap.MMap
	capacity uint32
	count    uint32
	minKey   uint32
	maxKey   uint32
	recSize  int // 4 + valueSize
}

func openOrCreateSegment(path string, capacity uint32, recSize int, perm os.FileMode) (*segment, error) {
	isNew := false
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		isNew = true
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, perm)
	if err != nil {
		return nil, err
	}

	if isNew {
		total := int64(headerSize) + int64(capacity)*int64(recSize)
		if err := f.Truncate(total); err != nil {
			_ = f.Close()
			return nil, err
		}
	}

	mm, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	s := &segment{path: path, file: f, m: mm, recSize: recSize}

	if isNew {
		binary.LittleEndian.PutUint32(s.m[offMagic:offMagic+4], segMagic)
		binary.LittleEndian.PutUint32(s.m[offVersion:offVersion+4], 1)
		binary.LittleEndian.PutUint32(s.m[offRecSize:offRecSize+4], uint32(recSize))
		binary.LittleEndian.PutUint32(s.m[offCapacity:offCapacity+4], capacity)
		binary.LittleEndian.PutUint32(s.m[offCount:offCount+4], 0)
		binary.LittleEndian.PutUint32(s.m[offMinKey:offMinKey+4], 0)
		binary.LittleEndian.PutUint32(s.m[offMaxKey:offMaxKey+4], 0)
	}

	if magic := binary.LittleEndian.Uint32(s.m[offMagic : offMagic+4]); magic != segMagic {
		_ = s.m.Unmap()
		_ = f.Close()
		return nil, fmt.Errorf("bad segment magic: %x", magic)
	}
	if rs := binary.LittleEndian.Uint32(s.m[offRecSize : offRecSize+4]); int(rs) != recSize {
		_ = s.m.Unmap()
		_ = f.Close()
		return nil, fmt.Errorf("unexpected recSize: file=%d want=%d", rs, recSize)
	}

	s.capacity = binary.LittleEndian.Uint32(s.m[offCapacity : offCapacity+4])
	s.count = binary.LittleEndian.Uint32(s.m[offCount : offCount+4])
	s.minKey = binary.LittleEndian.Uint32(s.m[offMinKey : offMinKey+4])
	s.maxKey = binary.LittleEndian.Uint32(s.m[offMaxKey : offMaxKey+4])
	return s, nil
}

func (s *segment) close() error {
	if s == nil {
		return nil
	}
	_ = s.m.Flush()
	_ = s.m.Unmap()
	return s.file.Close()
}

func (s *segment) headerSetCountMinMax(count, minK, maxK uint32) {
	binary.LittleEndian.PutUint32(s.m[offCount:offCount+4], count)
	binary.LittleEndian.PutUint32(s.m[offMinKey:offMinKey+4], minK)
	binary.LittleEndian.PutUint32(s.m[offMaxKey:offMaxKey+4], maxK)
	s.count, s.minKey, s.maxKey = count, minK, maxK
}

func (s *segment) recordOffset(i uint32) int { return headerSize + int(i)*s.recSize }

func (s *segment) readKey(i uint32) uint32 {
	o := s.recordOffset(i)
	return binary.LittleEndian.Uint32(s.m[o : o+4])
}

func (s *segment) readValInto(i uint32, dst []byte) {
	o := s.recordOffset(i)
	copy(dst, s.m[o+4:o+s.recSize])
}

func (s *segment) writeRec(i uint32, key uint32, value []byte) {
	o := s.recordOffset(i)
	binary.LittleEndian.PutUint32(s.m[o:o+4], key)
	copy(s.m[o+4:o+s.recSize], value)
}

func (s *segment) updateValAtIndex(i uint32, value []byte) {
	o := s.recordOffset(i)
	copy(s.m[o+4:o+s.recSize], value)
}

// append requires strictly increasing key within (and across) segments.
func (s *segment) append(key uint32, value []byte) error {
	if s.count >= s.capacity {
		return errors.New("segment full")
	}
	if s.count == 0 {
		s.writeRec(0, key, value)
		s.headerSetCountMinMax(1, key, key)
		return nil
	}
	lastKey := s.readKey(s.count - 1)
	if key <= lastKey {
		return fmt.Errorf("append out of order: key=%d last=%d", key, lastKey)
	}
	s.writeRec(s.count, key, value)
	s.headerSetCountMinMax(s.count+1, s.minKey, key)
	return nil
}

// Binary search within segment
func (s *segment) findIndex(key uint32) (uint32, bool) {
	lo, hi := uint32(0), s.count
	for lo < hi {
		mid := lo + (hi-lo)/2
		k := s.readKey(mid)
		if k == key {
			return mid, true
		}
		if k < key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo, false
}

func (s *segment) get(key uint32, dst []byte) bool {
	if s.count == 0 || key < s.minKey || key > s.maxKey {
		return false
	}
	idx, ok := s.findIndex(key)
	if !ok {
		return false
	}
	s.readValInto(idx, dst)
	return true
}

func (s *segment) update(key uint32, value []byte) bool {
	if s.count == 0 || key < s.minKey || key > s.maxKey {
		return false
	}
	idx, ok := s.findIndex(key)
	if !ok {
		return false
	}
	s.updateValAtIndex(idx, value)
	return true
}

// ================================ Shard =======================================

type shard struct {
	id      int
	baseDir string
	mu      sync.RWMutex
	segCap  uint32
	recSize int
	segs    []*segment // sorted by minKey
}

func openShard(opts Options, shardID int) (*shard, error) {
	dir := filepath.Join(opts.Dir, fmt.Sprintf("shard-%02d", shardID))
	if err := os.MkdirAll(dir, opts.DirPerm); err != nil {
		return nil, err
	}

	sh := &shard{id: shardID, baseDir: dir, segCap: opts.SegmentCapacity, recSize: 4 + opts.ValueSize}

	// Discover existing segments
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var segPaths []string
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, "seg-") && strings.HasSuffix(name, ".mmdb") {
			segPaths = append(segPaths, filepath.Join(dir, name))
		}
	}
	sort.Strings(segPaths)
	for _, p := range segPaths {
		sg, err := openOrCreateSegment(p, sh.segCap, sh.recSize, opts.FilePerm)
		if err != nil {
			return nil, err
		}
		sh.segs = append(sh.segs, sg)
	}
	return sh, nil
}

func (sh *shard) close() error {
	var firstErr error
	for _, sg := range sh.segs {
		if err := sg.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (sh *shard) lastSegment() (*segment, error) {
	if len(sh.segs) == 0 {
		p := filepath.Join(sh.baseDir, "seg-000000.mmdb")
		sg, err := openOrCreateSegment(p, sh.segCap, sh.recSize, 0o644)
		if err != nil {
			return nil, err
		}
		sh.segs = append(sh.segs, sg)
		return sg, nil
	}
	return sh.segs[len(sh.segs)-1], nil
}

func (sh *shard) rollSegment() (*segment, error) {
	p := filepath.Join(sh.baseDir, fmt.Sprintf("seg-%06d.mmdb", len(sh.segs)))
	return openOrCreateSegment(p, sh.segCap, sh.recSize, 0o644)
}

func (sh *shard) Append(key uint32, value []byte) error {
	sh.mu.Lock()
	defer sh.mu.Unlock()

	last, err := sh.lastSegment()
	if err != nil {
		return err
	}

	if last.count >= last.capacity {
		last, err = sh.rollSegment()
		if err != nil {
			return err
		}
		sh.segs = append(sh.segs, last)
	}

	if last.count == 0 && len(sh.segs) > 1 {
		prev := sh.segs[len(sh.segs)-2]
		if key <= prev.maxKey {
			return fmt.Errorf("append out of order vs prev seg: key=%d prevMax=%d", key, prev.maxKey)
		}
	}
	return last.append(key, value)
}

func (sh *shard) findSegmentForKey(key uint32) int {
	lo, hi := 0, len(sh.segs)
	for lo < hi {
		mid := lo + (hi-lo)/2
		sg := sh.segs[mid]
		if key < sg.minKey {
			hi = mid
		} else if key > sg.maxKey {
			lo = mid + 1
		} else {
			return mid
		}
	}
	return -1
}

func (sh *shard) Get(key uint32, dst []byte) bool {
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	if len(sh.segs) == 0 {
		return false
	}
	idx := sh.findSegmentForKey(key)
	if idx < 0 {
		return false
	}
	return sh.segs[idx].get(key, dst)
}

func (sh *shard) Update(key uint32, value []byte) bool {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if len(sh.segs) == 0 {
		return false
	}
	idx := sh.findSegmentForKey(key)
	if idx < 0 {
		return false
	}
	return sh.segs[idx].update(key, value)
}

// ================================ Utilities ===================================

// Write a tiny manifest for visibility. Optional helper; safe to ignore errors.
func writeManifest(dir string, opts Options) {
	_ = os.MkdirAll(dir, opts.DirPerm)
	f, err := os.Create(filepath.Join(dir, "manifest.txt"))
	if err != nil {
		return
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	fmt.Fprintln(w, "MMDB Manifest")
	fmt.Fprintln(w, "Created:", time.Now().Format(time.RFC3339))
	fmt.Fprintln(w, "Go:", runtime.Version())
	fmt.Fprintln(w, "OS/Arch:", runtime.GOOS+"/"+runtime.GOARCH)
	fmt.Fprintln(w, "NumShards:", opts.NumShards)
	fmt.Fprintln(w, "SegmentCapacity:", opts.SegmentCapacity)
	fmt.Fprintln(w, "ValueSize:", opts.ValueSize)
	_ = w.Flush()
}

// ================================ Type checks ================================

// ensurePODType returns error if t contains pointers or any var-len components.
func ensurePODType(t reflect.Type) error {
	if t == nil {
		return errors.New("nil type")
	}
	if !isPODType(t) {
		return fmt.Errorf("type %v is not POD (no pointers/var-len allowed)", t)
	}
	return nil
}

func isPODType(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Bool,
		reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int,
		reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint,
		reflect.Float32, reflect.Float64,
		reflect.Complex64, reflect.Complex128:
		return true
	case reflect.Array:
		return isPODType(t.Elem())
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			if !isPODType(t.Field(i).Type) {
				return false
			}
		}
		return true
	default:
		// Disallowed kinds: Ptr, String, Slice, Map, Interface, Func, Chan, UnsafePointer
		return false
	}
}

// fixedPODSizeOf returns the byte size for a POD type t (struct padding included for structs).
func fixedPODSizeOf(t reflect.Type) (int, error) {
	if err := ensurePODType(t); err != nil {
		return 0, err
	}
	// Use reflect.Type.Size (includes struct padding; stable for raw-bytes POD codec)
	return int(t.Size()), nil
}
