// Package ons
// Owner Name System (ONS) + Ownable Name System (공유 ID 풀)
//
// 변경 요약
// - 처리 순서: Owner(선행) → Module(후행) → Ownable(최후행)
// - 휴리스틱 전면 금지: rename/add/delete는 사용자 질의 + 더블체크
// - "설명만 변경"은 자동 반영(+로그) 후 저장
// - 원소별 reselect(잘못 선택시 해당 원소만 재질의) 지원
// - SystemID는 하나의 풀을 공유(ONS_next_id.txt)
// - CSV는 정렬 출력, 저장은 *.tmp → rename 원자적 보장
//
// 공개 API(발췌)
//
//	RegisterModule, RegisterRelation, RegisterOwner, GetSystemIdByOwnerName ...
//	RegisterOwnable, GetSystemIdByOwnableName ...
//
// 사용 흐름
//   - init()에서 Register* 호출로 등록 버퍼 채움
//   - 첫 조회/Initialize 시 디스크(CSV)와 등록 버퍼간 diff를 대화형으로 반영
package ons

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
)

// ---------- 타입 ----------

type ModuleName string
type OwnerName string
type OwnableName string

type SystemID uint64

// ---------- 싱글톤 ----------

var (
	instance *ONS
	once     sync.Once
)

// ---------- 상태 ----------

type ONS struct {
	mu sync.RWMutex

	// 모듈 → 포함 오너
	moduleToIncludedOwners map[ModuleName][]OwnerName
	moduleDescs            map[ModuleName]string

	// OWNER 네임스페이스
	ownerNameToSystemID map[OwnerName]SystemID
	ownerDescs          map[OwnerName]string
	systemToOwnerName   map[SystemID]OwnerName

	// OWNABLE 네임스페이스 (신규)
	ownableNameToSystemID map[OwnableName]SystemID
	ownableDescs          map[OwnableName]string
	systemToOwnableName   map[SystemID]OwnableName

	// 단일 공유 증가 ID
	nextSystemID SystemID

	// 파일 경로
	moduleCSVPath  string // ONS_modules.csv
	ownerCSVPath   string // ONS_owners.csv
	ownableCSVPath string // ONS_ownables.csv

	// init() 버퍼
	registeredModules   map[ModuleName]string  // 모듈명 → desc
	registeredRelations map[string]bool        // "모듈:오너"
	registeredOwners    map[OwnerName]string   // 오너명 → desc
	registeredOwnables  map[OwnableName]string // 오너블명 → desc

	initialized bool
	initOnce    sync.Once
}

// ---------- 옵션 ----------

type InitOptions struct{ Interactive bool }

var defaultInitOpts = InitOptions{Interactive: true}

// ---------- 싱글톤 팩토리 ----------

func GetInstance() *ONS {
	once.Do(func() {
		root := filepath.Join(computation.GetModuleRoot(), "ONS")
		instance = &ONS{
			moduleToIncludedOwners: make(map[ModuleName][]OwnerName),
			moduleDescs:            make(map[ModuleName]string),

			ownerNameToSystemID: make(map[OwnerName]SystemID),
			ownerDescs:          make(map[OwnerName]string),
			systemToOwnerName:   make(map[SystemID]OwnerName),

			ownableNameToSystemID: make(map[OwnableName]SystemID),
			ownableDescs:          make(map[OwnableName]string),
			systemToOwnableName:   make(map[SystemID]OwnableName),

			registeredModules:   make(map[ModuleName]string),
			registeredRelations: make(map[string]bool),
			registeredOwners:    make(map[OwnerName]string),
			registeredOwnables:  make(map[OwnableName]string),

			nextSystemID:   SystemID(loadNextSystemID(root)),
			moduleCSVPath:  filepath.Join(root, "ONS_modules.csv"),
			ownerCSVPath:   filepath.Join(root, "ONS_owners.csv"),
			ownableCSVPath: filepath.Join(root, "ONS_ownables.csv"),
			initialized:    false,
		}
	})
	return instance
}

// ---------- 초기화 ----------

func (m *ONS) ensureInitialized() {
	m.initOnce.Do(func() {
		if err := m.InitializeWith(defaultInitOpts); err != nil {
			panic(fmt.Errorf("ONS init failed: %w", err))
		}
	})
}

func Initialize() error { return GetInstance().InitializeWith(defaultInitOpts) }

func (m *ONS) InitializeWith(_ InitOptions) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.initialized {
		return nil
	}

	// CSV 로드
	if err := m.loadModuleCSV(); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := m.loadOwnerCSV(); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := m.loadOwnableCSV(); err != nil && !os.IsNotExist(err) {
		return err
	}

	// 1) OWNER 선행 (모듈 포함목록에 파급)
	if err := m.applyOwnerDiffInteractive(); err != nil {
		return err
	}

	// 2) MODULE 후행
	if err := m.applyModuleDiffInteractive(); err != nil {
		return err
	}

	// 3) OWNABLE 최후행 (파급 없음)
	if err := m.applyOwnableDiffInteractive(); err != nil {
		return err
	}

	m.initialized = true
	return nil
}

// ---------- 입력 유틸 ----------

func confirmYesNo(prompt string) bool {
	sc := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print(prompt)
		sc.Scan()
		in := strings.ToLower(strings.TrimSpace(sc.Text()))
		switch in {
		case "y", "yes":
			return true
		case "n", "no":
			return false
		default:
			fmt.Println("올바른 입력이 아닙니다. yes 또는 no 로 입력하세요.")
		}
	}
}

func doubleCheck(expectedPhrase string) bool {
	sc := bufio.NewScanner(os.Stdin)
	for {
		fmt.Printf("\n확인을 위해 다음 문장을 정확히 입력하세요:\n\"%s\"\n입력 (또는 'no'로 취소): ", expectedPhrase)
		sc.Scan()
		in := strings.TrimSpace(sc.Text())
		if in == expectedPhrase {
			return true
		}
		if strings.ToLower(in) == "no" {
			return false
		}
		fmt.Println("입력이 일치하지 않습니다. 다시 시도해주세요.")
	}
}

func sanitizeDescription(desc string) string {
	desc = strings.ReplaceAll(desc, ",", " ")
	desc = strings.ReplaceAll(desc, ".", " ")
	desc = strings.ReplaceAll(desc, "\n", " ")
	desc = strings.ReplaceAll(desc, "\r", " ")
	desc = strings.ReplaceAll(desc, "\"", "'")
	desc = strings.Join(strings.Fields(desc), " ")
	return strings.TrimSpace(desc)
}

// =====================================================================
// 공통 이름 네임스페이스 처리기 (Owner/Ownable에서 재사용)
// =====================================================================

// applyNameDiffInteractive:
// - label: "OWNER" / "OWNABLE" (표시 메시지 용)
// - registered: 이번 실행 등록 (name -> desc)
// - listExisting: 디스크 상 존재 name 목록
// - exists/getID/getDesc/setDesc: 조회/설명 갱신
// - addNew: 신규 추가(공유 nextID 증가 포함해야 함)
// - rename: 이름 변경(파급 필요시 내부에서 수행)
// - delete: 삭제(파급 필요시 내부에서 수행)
// - save: CSV 저장 함수
func (m *ONS) applyNameDiffInteractive(
	label string,
	registered map[string]string,
	listExisting func() []string,
	exists func(name string) bool,
	getID func(name string) (SystemID, bool),
	getDesc func(name string) string,
	setDesc func(name, desc string),
	addNew func(name, desc string) (SystemID, error),
	rename func(oldName, newName, newDesc string) error,
	deleteFn func(name string) error,
	save func() error,
) error {

	// (0) 설명만 변경 자동 반영
	descOnlyChanged := false
	for name, newDesc := range registered {
		if exists(name) {
			ns := sanitizeDescription(newDesc)
			os := getDesc(name)
			if ns != os {
				fmt.Printf("ONS: %s [%s]의 설명이 변경되었습니다: \"%s\" -> \"%s\"\n", label, name, os, ns)
				setDesc(name, ns)
				descOnlyChanged = true
			}
		}
	}

	// new/missing 계산
	news := make(map[string]string)   // 실행 O / CSV X
	miss := make(map[string]SystemID) // CSV O / 실행 X

	for name, desc := range registered {
		if !exists(name) {
			news[name] = sanitizeDescription(desc)
		}
	}
	for _, name := range listExisting() {
		if _, ok := registered[name]; !ok {
			if sid, ok := getID(name); ok {
				miss[name] = sid
			}
		}
	}

	// 설명만 변경
	if len(news) == 0 && len(miss) == 0 {
		if descOnlyChanged {
			return save()
		}
		return nil
	}

	// 결정적 순서
	newList := make([]string, 0, len(news))
	for n := range news {
		newList = append(newList, n)
	}
	sort.Strings(newList)

	usedMissing := make(map[string]bool)

	// 1) 새 이름 처리 (원소별 reselect)
	for _, newName := range newList {
		for {
			handled, err := func() (bool, error) {
				sc := bufio.NewScanner(os.Stdin)
				fmt.Printf("\n=== ONS: 새로운 %s 감지 ===\n", label)
				fmt.Printf("새 %s: [%s]\n설명: %s\n", label, newName, news[newName])
				fmt.Println("이 항목은 무엇입니까?")
				fmt.Println("  1) 신규 추가 (새 SystemID 할당)")
				fmt.Println("  2) 기존 항목의 이름 변경 (rename)")

				choice := ""
				for {
					fmt.Print("번호를 입력하세요 (1/2): ")
					sc.Scan()
					choice = strings.TrimSpace(sc.Text())
					if choice == "1" || choice == "2" {
						break
					}
					fmt.Println("올바른 입력이 아닙니다.")
				}

				if choice == "1" {
					if !confirmYesNo(fmt.Sprintf("이 %s를 새로 추가합니까? (yes/no): ", label)) {
						if confirmYesNo("이 선택을 다시 하시겠습니까? (yes/no): ") {
							return false, nil
						}
						return false, fmt.Errorf("%s addition aborted", strings.ToLower(label))
					}
					if !doubleCheck(fmt.Sprintf("%s를 ONS에 새로운 %s로 추가합니다.", newName, strings.ToLower(label))) {
						if confirmYesNo("문장 일치 실패. 이 선택을 다시 하시겠습니까? (yes/no): ") {
							return false, nil
						}
						return false, fmt.Errorf("%s addition failed double-check", strings.ToLower(label))
					}
					if _, err := addNew(newName, news[newName]); err != nil {
						return false, err
					}
					fmt.Println("→ 추가 완료")
					return true, nil
				}

				// rename
				if len(miss) == 0 {
					fmt.Println("rename 대상으로 사용할 기존 항목이 없습니다.")
					if confirmYesNo("이 선택을 다시 하시겠습니까? (yes/no): ") {
						return false, nil
					}
					return false, fmt.Errorf("no candidate to rename from")
				}

				type cand struct {
					name string
					sys  SystemID
					desc string
				}
				var cands []cand
				for old, sid := range miss {
					if usedMissing[old] {
						continue
					}
					cands = append(cands, cand{old, sid, getDesc(old)})
				}
				if len(cands) == 0 {
					fmt.Println("rename 후보가 모두 사용되었습니다.")
					if confirmYesNo("이 선택을 다시 하시겠습니까? (yes/no): ") {
						return false, nil
					}
					return false, fmt.Errorf("no remaining candidate to rename from")
				}
				sort.Slice(cands, func(i, j int) bool { return cands[i].name < cands[j].name })

				fmt.Printf("\n어떤 기존 %s를 %s 로 변경하시겠습니까?\n", label, newName)
				for i, c := range cands {
					fmt.Printf("  %d) [%s] (SystemID: %d, %s)\n", i+1, c.name, c.sys, c.desc)
				}
				idx := -1
				for {
					fmt.Printf("번호(1~%d)를 입력하세요 (0: 이전으로): ", len(cands))
					sc.Scan()
					s := strings.TrimSpace(sc.Text())
					v, err := strconv.Atoi(s)
					if err == nil && v == 0 {
						return false, nil
					}
					if err == nil && v >= 1 && v <= len(cands) {
						idx = v - 1
						break
					}
					fmt.Println("올바른 입력이 아닙니다.")
				}
				chosen := cands[idx]

				fmt.Printf("\n확인: %s [%s] → [%s] 로 이름을 변경합니다.\n", label, chosen.name, newName)
				if !confirmYesNo("이 변경을 진행합니까? (yes/no): ") {
					if confirmYesNo("이 선택을 다시 하시겠습니까? (yes/no): ") {
						return false, nil
					}
					return false, fmt.Errorf("%s rename aborted", strings.ToLower(label))
				}
				if !doubleCheck(fmt.Sprintf("%s를 %s로 변경합니다.", chosen.name, newName)) {
					if confirmYesNo("문장 일치 실패. 이 선택을 다시 하시겠습니까? (yes/no): ") {
						return false, nil
					}
					return false, fmt.Errorf("%s rename failed double-check", strings.ToLower(label))
				}

				if err := rename(chosen.name, newName, news[newName]); err != nil {
					return false, err
				}
				usedMissing[chosen.name] = true
				delete(miss, chosen.name)
				fmt.Println("→ 이름 변경 완료 (SystemID 유지)")
				return true, nil
			}()
			if err != nil {
				return err
			}
			if handled {
				break
			} // 다음 원소
		}
	}

	// 2) 남은 missing 처리 (삭제/유지) — 원소별 reselect
	if len(miss) > 0 {
		type pair struct {
			name string
			sys  SystemID
			desc string
		}
		var arr []pair
		for n, s := range miss {
			arr = append(arr, pair{n, s, getDesc(n)})
		}
		sort.Slice(arr, func(i, j int) bool { return arr[i].name < arr[j].name })

		for _, p := range arr {
			for {
				handled, err := func() (bool, error) {
					sc := bufio.NewScanner(os.Stdin)
					fmt.Printf("\n기존 %s [%s] (SystemID: %d, %s)가 더 이상 등록되지 않았습니다.\n", label, p.name, p.sys, p.desc)
					fmt.Println("이 항목을 어떻게 하시겠습니까?")
					fmt.Println("  1) 삭제")
					fmt.Println("  2) 유지(아무 작업 안 함)")

					choice := ""
					for {
						fmt.Print("번호를 입력하세요 (1/2): ")
						sc.Scan()
						choice = strings.TrimSpace(sc.Text())
						if choice == "1" || choice == "2" {
							break
						}
						fmt.Println("올바른 입력이 아닙니다.")
					}
					if choice == "2" {
						fmt.Println("→ 유지됨")
						return true, nil
					}

					if !confirmYesNo(fmt.Sprintf("이 %s를 삭제합니까? (yes/no): ", label)) {
						if confirmYesNo("이 선택을 다시 하시겠습니까? (yes/no): ") {
							return false, nil
						}
						return false, fmt.Errorf("%s delete aborted", strings.ToLower(label))
					}
					if !doubleCheck(fmt.Sprintf("%s를 ONS에서 제거합니다.", p.name)) {
						if confirmYesNo("문장 일치 실패. 이 선택을 다시 하시겠습니까? (yes/no): ") {
							return false, nil
						}
						return false, fmt.Errorf("%s delete failed double-check", strings.ToLower(label))
					}
					if err := deleteFn(p.name); err != nil {
						return false, err
					}
					fmt.Println("→ 삭제 완료")
					return true, nil
				}()
				if err != nil {
					return err
				}
				if handled {
					break
				}
			}
		}
	}

	return save()
}

// =====================================================================
// OWNER 단계 (선행)
// =====================================================================

func (m *ONS) applyOwnerDiffInteractive() error {
	// 어댑터 클로저들
	registered := make(map[string]string, len(m.registeredOwners))
	for k, v := range m.registeredOwners {
		registered[string(k)] = v
	}

	listExisting := func() []string {
		out := make([]string, 0, len(m.ownerNameToSystemID))
		for n := range m.ownerNameToSystemID {
			out = append(out, string(n))
		}
		sort.Strings(out)
		return out
	}
	exists := func(name string) bool { _, ok := m.ownerNameToSystemID[OwnerName(name)]; return ok }
	getID := func(name string) (SystemID, bool) {
		id, ok := m.ownerNameToSystemID[OwnerName(name)]
		return id, ok
	}
	getDesc := func(name string) string { return m.ownerDescs[OwnerName(name)] }
	setDesc := func(name, desc string) { m.ownerDescs[OwnerName(name)] = desc }
	addNew := func(name, desc string) (SystemID, error) {
		sys := m.nextSystemID
		m.nextSystemID++
		if err := m.saveNextSystemID(); err != nil {
			return 0, err
		}
		m.ownerNameToSystemID[OwnerName(name)] = sys
		m.systemToOwnerName[sys] = OwnerName(name)
		m.ownerDescs[OwnerName(name)] = desc
		return sys, nil
	}
	rename := func(oldName, newName, newDesc string) error {
		sys := m.ownerNameToSystemID[OwnerName(oldName)]
		delete(m.ownerNameToSystemID, OwnerName(oldName))
		m.ownerNameToSystemID[OwnerName(newName)] = sys
		m.systemToOwnerName[sys] = OwnerName(newName)
		m.ownerDescs[OwnerName(newName)] = newDesc
		delete(m.ownerDescs, OwnerName(oldName))
		// 모듈 포함목록 치환
		m.replaceOwnerNameInModules(OwnerName(oldName), OwnerName(newName))
		return nil
	}
	deleteFn := func(name string) error {
		sys := m.ownerNameToSystemID[OwnerName(name)]
		delete(m.ownerNameToSystemID, OwnerName(name))
		delete(m.systemToOwnerName, sys)
		delete(m.ownerDescs, OwnerName(name))
		m.removeOwnerFromAllModules(OwnerName(name))
		return nil
	}
	save := func() error {
		// owner 변경은 모듈 포함목록에 영향 가능 → 둘 다 저장
		if err := m.saveOwnerCSV(); err != nil {
			return err
		}
		return m.saveModuleCSV()
	}

	return m.applyNameDiffInteractive("OWNER", registered, listExisting, exists, getID, getDesc, setDesc, addNew, rename, deleteFn, save)
}

func (m *ONS) replaceOwnerNameInModules(oldName, newName OwnerName) {
	for mod, owners := range m.moduleToIncludedOwners {
		changed := false
		for i, o := range owners {
			if o == oldName {
				owners[i] = newName
				changed = true
			}
		}
		if changed {
			m.moduleToIncludedOwners[mod] = uniqueOwnersSorted(owners)
		}
	}
}
func (m *ONS) removeOwnerFromAllModules(name OwnerName) {
	for mod, owners := range m.moduleToIncludedOwners {
		dst := owners[:0]
		for _, o := range owners {
			if o != name {
				dst = append(dst, o)
			}
		}
		m.moduleToIncludedOwners[mod] = dst
	}
}
func uniqueOwnersSorted(in []OwnerName) []OwnerName {
	seen := make(map[OwnerName]struct{}, len(in))
	out := make([]OwnerName, 0, len(in))
	for _, o := range in {
		if _, ok := seen[o]; !ok {
			seen[o] = struct{}{}
			out = append(out, o)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// =====================================================================
// MODULE 단계 (후행) — 전용 처리 (포함목록 존재)
// =====================================================================

func (m *ONS) applyModuleDiffInteractive() error {
	regNow := m.registeredModules
	exist := m.moduleDescs

	newMods := make(map[ModuleName]string)
	missing := make(map[ModuleName]string)

	// (0) 설명만 변경 자동 반영
	for name, newDesc := range regNow {
		if _, ok := exist[name]; ok {
			ns := sanitizeDescription(newDesc)
			os := m.moduleDescs[name]
			if ns != os {
				fmt.Printf("ONS: MODULE [%s]의 설명이 변경되었습니다: \"%s\" -> \"%s\"\n", name, os, ns)
				m.moduleDescs[name] = ns
			}
		}
	}

	for name, desc := range regNow {
		if _, ok := exist[name]; !ok {
			newMods[name] = sanitizeDescription(desc)
		}
	}
	for name, desc := range exist {
		if _, ok := regNow[name]; !ok {
			missing[name] = sanitizeDescription(desc)
		}
	}

	if len(newMods) == 0 && len(missing) == 0 {
		// 관계 버퍼 보정 + 저장
		m.applyRegisteredRelationsPostfix()
		return m.saveModuleCSV()
	}

	sc := bufio.NewScanner(os.Stdin)

	// 결정적 순서
	newList := make([]ModuleName, 0, len(newMods))
	for n := range newMods {
		newList = append(newList, n)
	}
	sort.Slice(newList, func(i, j int) bool { return newList[i] < newList[j] })

	usedMissing := make(map[ModuleName]bool)

	// 1) 신규 모듈: 추가 vs rename (reselect)
	for _, newName := range newList {
		for {
			handled, err := func() (bool, error) {
				desc := newMods[newName]
				fmt.Printf("\n=== ONS: 새로운 MODULE 감지 ===\n")
				fmt.Printf("새 MODULE: [%s]\n설명: %s\n", newName, desc)
				fmt.Println("이 항목은 무엇입니까?")
				fmt.Println("  1) 신규 추가")
				fmt.Println("  2) 기존 MODULE 의 이름 변경 (rename)")
				choice := ""
				for {
					fmt.Print("번호를 입력하세요 (1/2): ")
					sc.Scan()
					choice = strings.TrimSpace(sc.Text())
					if choice == "1" || choice == "2" {
						break
					}
					fmt.Println("올바른 입력이 아닙니다.")
				}
				if choice == "1" {
					if !confirmYesNo("이 MODULE 을 새로 추가합니까? (yes/no): ") {
						if confirmYesNo("이 선택을 다시 하시겠습니까? (yes/no): ") {
							return false, nil
						}
						return false, fmt.Errorf("module addition aborted")
					}
					if !doubleCheck(fmt.Sprintf("%s를 ONS에 신규 MODULE로 추가합니다.", newName)) {
						if confirmYesNo("문장 일치 실패. 이 선택을 다시 하시겠습니까? (yes/no): ") {
							return false, nil
						}
						return false, fmt.Errorf("module addition failed double-check")
					}
					m.moduleDescs[newName] = desc
					if _, ok := m.moduleToIncludedOwners[newName]; !ok {
						m.moduleToIncludedOwners[newName] = []OwnerName{}
					}
					fmt.Println("→ 모듈 추가 완료")
					return true, nil
				}
				// rename
				if len(missing) == 0 {
					fmt.Println("rename 대상으로 사용할 기존 MODULE 이 없습니다.")
					if confirmYesNo("이 선택을 다시 하시겠습니까? (yes/no): ") {
						return false, nil
					}
					return false, fmt.Errorf("no candidate module to rename from")
				}
				type cand struct {
					name   ModuleName
					desc   string
					owners []OwnerName
				}
				var cands []cand
				for old, d := range missing {
					if usedMissing[old] {
						continue
					}
					owners := append([]OwnerName(nil), m.moduleToIncludedOwners[old]...)
					sort.Slice(owners, func(i, j int) bool { return owners[i] < owners[j] })
					cands = append(cands, cand{old, d, owners})
				}
				if len(cands) == 0 {
					fmt.Println("rename 후보가 모두 사용되었습니다.")
					if confirmYesNo("이 선택을 다시 하시겠습니까? (yes/no): ") {
						return false, nil
					}
					return false, fmt.Errorf("no remaining candidate module to rename from")
				}
				sort.Slice(cands, func(i, j int) bool { return cands[i].name < cands[j].name })
				fmt.Println("\n어떤 기존 MODULE 을", newName, "로 변경하시겠습니까?")
				for i, c := range cands {
					fmt.Printf("  %d) [%s] (%s) - owners: %s\n", i+1, c.name, c.desc, ownersToString(c.owners))
				}
				idx := -1
				for {
					fmt.Printf("번호(1~%d)를 입력하세요 (0: 이전으로): ", len(cands))
					sc.Scan()
					s := strings.TrimSpace(sc.Text())
					v, err := strconv.Atoi(s)
					if err == nil && v == 0 {
						return false, nil
					}
					if err == nil && v >= 1 && v <= len(cands) {
						idx = v - 1
						break
					}
					fmt.Println("올바른 입력이 아닙니다.")
				}
				chosen := cands[idx]

				fmt.Printf("\n확인: MODULE [%s] → [%s] 로 이름을 변경합니다.\n", chosen.name, newName)
				if !confirmYesNo("이 변경을 진행합니까? (yes/no): ") {
					if confirmYesNo("이 선택을 다시 하시겠습니까? (yes/no): ") {
						return false, nil
					}
					return false, fmt.Errorf("module rename aborted")
				}
				if !doubleCheck(fmt.Sprintf("%s를 %s로 변경하겠습니다.", chosen.name, newName)) {
					if confirmYesNo("문장 일치 실패. 이 선택을 다시 하시겠습니까? (yes/no): ") {
						return false, nil
					}
					return false, fmt.Errorf("module rename failed double-check")
				}

				m.moduleDescs[newName] = desc
				delete(m.moduleDescs, chosen.name)
				if owners, ok := m.moduleToIncludedOwners[chosen.name]; ok {
					m.moduleToIncludedOwners[newName] = owners
					delete(m.moduleToIncludedOwners, chosen.name)
				} else if _, ok := m.moduleToIncludedOwners[newName]; !ok {
					m.moduleToIncludedOwners[newName] = []OwnerName{}
				}
				usedMissing[chosen.name] = true
				delete(missing, chosen.name)
				fmt.Println("→ 모듈 이름 변경 완료")
				return true, nil
			}()
			if err != nil {
				return err
			}
			if handled {
				break
			}
		}
	}

	// 2) 남은 missing 모듈: 삭제/유지 (reselect)
	if len(missing) > 0 {
		names := make([]ModuleName, 0, len(missing))
		for n := range missing {
			names = append(names, n)
		}
		sort.Slice(names, func(i, j int) bool { return names[i] < names[j] })
		for _, oldName := range names {
			for {
				handled, err := func() (bool, error) {
					desc := m.moduleDescs[oldName]
					fmt.Printf("\n기존 MODULE [%s] (%s)가 더 이상 등록되지 않았습니다.\n", oldName, desc)
					fmt.Println("이 MODULE을 어떻게 하시겠습니까?")
					fmt.Println("  1) 삭제")
					fmt.Println("  2) 유지(아무 작업 안 함)")
					choice := ""
					for {
						fmt.Print("번호를 입력하세요 (1/2): ")
						sc.Scan()
						choice = strings.TrimSpace(sc.Text())
						if choice == "1" || choice == "2" {
							break
						}
						fmt.Println("올바른 입력이 아닙니다.")
					}
					if choice == "2" {
						fmt.Println("→ 유지됨")
						return true, nil
					}
					if !confirmYesNo("이 MODULE 을 삭제합니까? (yes/no): ") {
						if confirmYesNo("이 선택을 다시 하시겠습니까? (yes/no): ") {
							return false, nil
						}
						return false, fmt.Errorf("module delete aborted")
					}
					if !doubleCheck(fmt.Sprintf("%s를 ONS에서 제거합니다.", oldName)) {
						if confirmYesNo("문장 일치 실패. 이 선택을 다시 하시겠습니까? (yes/no): ") {
							return false, nil
						}
						return false, fmt.Errorf("module delete failed double-check")
					}
					delete(m.moduleDescs, oldName)
					delete(m.moduleToIncludedOwners, oldName)
					fmt.Println("→ 모듈 삭제 완료")
					return true, nil
				}()
				if err != nil {
					return err
				}
				if handled {
					break
				}
			}
		}
	}

	// 3) 관계 버퍼 보정 + 저장
	m.applyRegisteredRelationsPostfix()
	return m.saveModuleCSV()
}

func (m *ONS) applyRegisteredRelationsPostfix() {
	for key := range m.registeredRelations {
		parts := strings.Split(key, ":")
		if len(parts) != 2 {
			continue
		}
		mod := ModuleName(parts[0])
		own := OwnerName(parts[1])
		if _, ok := m.moduleDescs[mod]; !ok {
			continue
		}
		owners := m.moduleToIncludedOwners[mod]
		found := false
		for _, o := range owners {
			if o == own {
				found = true
				break
			}
		}
		if !found {
			owners = append(owners, own)
			owners = uniqueOwnersSorted(owners)
			m.moduleToIncludedOwners[mod] = owners
		}
	}
}

// =====================================================================
// OWNABLE 단계 (최후행) — 공통 처리기 재사용
// =====================================================================

func (m *ONS) applyOwnableDiffInteractive() error {
	registered := make(map[string]string, len(m.registeredOwnables))
	for k, v := range m.registeredOwnables {
		registered[string(k)] = v
	}

	listExisting := func() []string {
		out := make([]string, 0, len(m.ownableNameToSystemID))
		for n := range m.ownableNameToSystemID {
			out = append(out, string(n))
		}
		sort.Strings(out)
		return out
	}
	exists := func(name string) bool { _, ok := m.ownableNameToSystemID[OwnableName(name)]; ok2 := ok; return ok2 }
	getID := func(name string) (SystemID, bool) {
		id, ok := m.ownableNameToSystemID[OwnableName(name)]
		return id, ok
	}
	getDesc := func(name string) string { return m.ownableDescs[OwnableName(name)] }
	setDesc := func(name, desc string) { m.ownableDescs[OwnableName(name)] = desc }
	addNew := func(name, desc string) (SystemID, error) {
		sys := m.nextSystemID
		m.nextSystemID++
		if err := m.saveNextSystemID(); err != nil {
			return 0, err
		}
		m.ownableNameToSystemID[OwnableName(name)] = sys
		m.systemToOwnableName[sys] = OwnableName(name)
		m.ownableDescs[OwnableName(name)] = desc
		return sys, nil
	}
	rename := func(oldName, newName, newDesc string) error {
		sys := m.ownableNameToSystemID[OwnableName(oldName)]
		delete(m.ownableNameToSystemID, OwnableName(oldName))
		m.ownableNameToSystemID[OwnableName(newName)] = sys
		m.systemToOwnableName[sys] = OwnableName(newName)
		m.ownableDescs[OwnableName(newName)] = newDesc
		delete(m.ownableDescs, OwnableName(oldName))
		return nil
	}
	deleteFn := func(name string) error {
		sys := m.ownableNameToSystemID[OwnableName(name)]
		delete(m.ownableNameToSystemID, OwnableName(name))
		delete(m.systemToOwnableName, sys)
		delete(m.ownableDescs, OwnableName(name))
		return nil
	}
	save := func() error { return m.saveOwnableCSV() }

	return m.applyNameDiffInteractive("OWNABLE", registered, listExisting, exists, getID, getDesc, setDesc, addNew, rename, deleteFn, save)
}

// =====================================================================
// CSV / NEXT-ID
// =====================================================================

func loadNextSystemID(root string) uint64 {
	idPath := filepath.Join(root, "ONS_next_id.txt")
	data, err := os.ReadFile(idPath)
	if err != nil {
		return 1
	}
	s := strings.TrimSpace(string(data))
	if s == "" {
		_ = os.WriteFile(idPath, []byte("1"), 0644)
		return 1
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 1
	}
	return v
}

func (m *ONS) saveNextSystemID() error {
	root := filepath.Join(computation.GetModuleRoot(), "ONS")
	idPath := filepath.Join(root, "ONS_next_id.txt")
	tmp := idPath + ".tmp"
	data := []byte(strconv.FormatUint(uint64(m.nextSystemID), 10))
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, idPath)
}

func (m *ONS) loadModuleCSV() error {
	f, err := os.Open(m.moduleCSVPath)
	if err != nil {
		return err
	}
	defer f.Close()
	r := csv.NewReader(f)
	r.LazyQuotes = true
	recs, err := r.ReadAll()
	if err != nil {
		return err
	}
	if len(recs) > 0 {
		recs = recs[1:]
	}
	for _, rec := range recs {
		if len(rec) < 3 {
			continue
		}
		mod := ModuleName(rec[0])
		desc := rec[1]
		m.moduleDescs[mod] = sanitizeDescription(desc)
		var owners []OwnerName
		if s := strings.TrimSpace(rec[2]); s != "" {
			for _, t := range strings.Split(s, ";") {
				t = strings.TrimSpace(t)
				if t != "" {
					owners = append(owners, OwnerName(t))
				}
			}
		}
		m.moduleToIncludedOwners[mod] = owners
	}
	return nil
}
func (m *ONS) saveModuleCSV() error {
	tmp := m.moduleCSVPath + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	if err := w.Write([]string{"ModuleID", "Description", "Structs"}); err != nil {
		f.Close()
		return err
	}

	type row struct {
		mod  ModuleName
		desc string
	}
	var rows []row
	for mod, desc := range m.moduleDescs {
		rows = append(rows, row{mod, desc})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].mod < rows[j].mod })

	for _, r := range rows {
		owners := append([]OwnerName(nil), m.moduleToIncludedOwners[r.mod]...)
		sort.Slice(owners, func(i, j int) bool { return owners[i] < owners[j] })
		ss := make([]string, len(owners))
		for i, o := range owners {
			ss[i] = string(o)
		}
		if err := w.Write([]string{string(r.mod), sanitizeDescription(r.desc), strings.Join(ss, ";")}); err != nil {
			f.Close()
			return err
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, m.moduleCSVPath)
}

func (m *ONS) loadOwnerCSV() error {
	f, err := os.Open(m.ownerCSVPath)
	if err != nil {
		return err
	}
	defer f.Close()
	r := csv.NewReader(f)
	r.LazyQuotes = true
	recs, err := r.ReadAll()
	if err != nil {
		return err
	}
	if len(recs) > 0 {
		recs = recs[1:]
	}
	for _, rec := range recs {
		if len(rec) < 3 {
			continue
		}
		name := OwnerName(rec[0])
		sys, err := strconv.ParseUint(rec[1], 10, 64)
		if err != nil {
			continue
		}
		desc := rec[2]
		m.ownerNameToSystemID[name] = SystemID(sys)
		m.systemToOwnerName[SystemID(sys)] = name
		m.ownerDescs[name] = sanitizeDescription(desc)
	}
	return nil
}
func (m *ONS) saveOwnerCSV() error {
	tmp := m.ownerCSVPath + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	if err := w.Write([]string{"StructName", "SystemID", "Description"}); err != nil {
		f.Close()
		return err
	}

	type row struct {
		name OwnerName
		sys  SystemID
		desc string
	}
	var rows []row
	for name, sys := range m.ownerNameToSystemID {
		rows = append(rows, row{name, sys, m.ownerDescs[name]})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].name < rows[j].name })

	for _, r := range rows {
		if err := w.Write([]string{
			string(r.name),
			strconv.FormatUint(uint64(r.sys), 10),
			sanitizeDescription(r.desc),
		}); err != nil {
			f.Close()
			return err
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, m.ownerCSVPath)
}

func (m *ONS) loadOwnableCSV() error {
	f, err := os.Open(m.ownableCSVPath)
	if err != nil {
		return err
	}
	defer f.Close()
	r := csv.NewReader(f)
	r.LazyQuotes = true
	recs, err := r.ReadAll()
	if err != nil {
		return err
	}
	if len(recs) > 0 {
		recs = recs[1:]
	}
	for _, rec := range recs {
		if len(rec) < 3 {
			continue
		}
		name := OwnableName(rec[0])
		sys, err := strconv.ParseUint(rec[1], 10, 64)
		if err != nil {
			continue
		}
		desc := rec[2]
		m.ownableNameToSystemID[name] = SystemID(sys)
		m.systemToOwnableName[SystemID(sys)] = name
		m.ownableDescs[name] = sanitizeDescription(desc)
	}
	return nil
}
func (m *ONS) saveOwnableCSV() error {
	tmp := m.ownableCSVPath + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	if err := w.Write([]string{"OwnableName", "SystemID", "Description"}); err != nil {
		f.Close()
		return err
	}

	type row struct {
		name OwnableName
		sys  SystemID
		desc string
	}
	var rows []row
	for name, sys := range m.ownableNameToSystemID {
		rows = append(rows, row{name, sys, m.ownableDescs[name]})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].name < rows[j].name })

	for _, r := range rows {
		if err := w.Write([]string{
			string(r.name),
			strconv.FormatUint(uint64(r.sys), 10),
			sanitizeDescription(r.desc),
		}); err != nil {
			f.Close()
			return err
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, m.ownableCSVPath)
}

// =====================================================================
// 퍼블릭 API
// =====================================================================

func RegisterModule(moduleID ModuleName, description string) {
	m := GetInstance()
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.initialized {
		m.registeredModules[moduleID] = sanitizeDescription(description)
		return
	}
	m.moduleDescs[moduleID] = sanitizeDescription(description)
	if _, ok := m.moduleToIncludedOwners[moduleID]; !ok {
		m.moduleToIncludedOwners[moduleID] = []OwnerName{}
	}
	_ = m.saveModuleCSV()
}

func RegisterRelation(moduleID ModuleName, owner OwnerName) {
	m := GetInstance()
	m.mu.Lock()
	defer m.mu.Unlock()
	key := fmt.Sprintf("%s:%s", moduleID, owner)
	if !m.initialized {
		m.registeredRelations[key] = true
		return
	}
	if _, ok := m.moduleToIncludedOwners[moduleID]; !ok {
		m.moduleToIncludedOwners[moduleID] = []OwnerName{}
	}
	owners := m.moduleToIncludedOwners[moduleID]
	for _, o := range owners {
		if o == owner {
			return
		}
	}
	m.moduleToIncludedOwners[moduleID] = uniqueOwnersSorted(append(owners, owner))
	_ = m.saveModuleCSV()
}

func RegisterOwner(owner OwnerName, description string) SystemID {
	m := GetInstance()
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.initialized {
		m.registeredOwners[owner] = sanitizeDescription(description)
		return 0
	}
	if sys, ok := m.ownerNameToSystemID[owner]; ok {
		m.ownerDescs[owner] = sanitizeDescription(description)
		_ = m.saveOwnerCSV()
		return sys
	}
	sys := m.nextSystemID
	m.nextSystemID++
	_ = m.saveNextSystemID()
	m.ownerNameToSystemID[owner] = sys
	m.systemToOwnerName[sys] = owner
	m.ownerDescs[owner] = sanitizeDescription(description)
	_ = m.saveOwnerCSV()
	return sys
}

func RegisterOwnable(name OwnableName, description string) SystemID {
	m := GetInstance()
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.initialized {
		m.registeredOwnables[name] = sanitizeDescription(description)
		return 0
	}
	if sys, ok := m.ownableNameToSystemID[name]; ok {
		m.ownableDescs[name] = sanitizeDescription(description)
		_ = m.saveOwnableCSV()
		return sys
	}
	sys := m.nextSystemID
	m.nextSystemID++
	_ = m.saveNextSystemID()
	m.ownableNameToSystemID[name] = sys
	m.systemToOwnableName[sys] = name
	m.ownableDescs[name] = sanitizeDescription(description)
	_ = m.saveOwnableCSV()
	return sys
}

// 조회 계열
func GetSystemIdByOwnerName(owner OwnerName) (SystemID, error) {
	m := GetInstance()
	m.ensureInitialized()
	m.mu.RLock()
	defer m.mu.RUnlock()
	if id, ok := m.ownerNameToSystemID[owner]; ok {
		return id, nil
	}
	return 0, fmt.Errorf("owner %s not found", owner)
}
func GetOwnerNameBySystemID(systemID SystemID) (OwnerName, error) {
	m := GetInstance()
	m.ensureInitialized()
	m.mu.RLock()
	defer m.mu.RUnlock()
	if owner, ok := m.systemToOwnerName[systemID]; ok {
		return owner, nil
	}
	return "", fmt.Errorf("SystemID %d not found (owner)", systemID)
}

func GetSystemIdByOwnableName(name OwnableName) (SystemID, error) {
	m := GetInstance()
	m.ensureInitialized()
	m.mu.RLock()
	defer m.mu.RUnlock()
	if id, ok := m.ownableNameToSystemID[name]; ok {
		return id, nil
	}
	return 0, fmt.Errorf("ownable %s not found", name)
}
func GetOwnableNameBySystemID(systemID SystemID) (OwnableName, error) {
	m := GetInstance()
	m.ensureInitialized()
	m.mu.RLock()
	defer m.mu.RUnlock()
	if n, ok := m.systemToOwnableName[systemID]; ok {
		return n, nil
	}
	return "", fmt.Errorf("SystemID %d not found (ownable)", systemID)
}

func GetOwnersOfModule(module ModuleName) []OwnerName {
	m := GetInstance()
	m.ensureInitialized()
	m.mu.RLock()
	defer m.mu.RUnlock()
	if owners, ok := m.moduleToIncludedOwners[module]; ok {
		out := make([]OwnerName, len(owners))
		copy(out, owners)
		return out
	}
	return []OwnerName{}
}

func ownersToString(owners []OwnerName) string {
	if len(owners) == 0 {
		return "-"
	}
	ss := make([]string, len(owners))
	for i, o := range owners {
		ss[i] = string(o)
	}
	return strings.Join(ss, ";")
}
