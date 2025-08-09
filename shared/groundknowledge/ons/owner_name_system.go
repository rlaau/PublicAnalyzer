// Package ons
// Owner Name System (ONS)
//
// 변경 요약:
// - 변경 절차: Owner(선행) → Module(후행)
// - 휴리스틱 전면 금지: 모든 rename/add/delete는 사용자 질의 + 더블체크
// - "설명만 변경"은 자동 반영(+로그) 후 즉시 저장
// - 원소별 재선택(reselect) 지원: 잘못 고르면 해당 원소만 재질의
// - SystemID 불변: rename 시 유지, add 시에만 next_id 증가
// - 저장은 전부 *.tmp → rename (원자적)
// - CSV는 정렬 순서로 기록(결정성)
//
// 사용 예:
//   - 패키지 init()에서 Register* 호출
//   - 런타임 첫 조회 시 ensureInitialized()가 자동으로 대화형 초기화 진행
//   - 또는 main()에서 ons.Initialize() 호출로 선제 초기화
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

type ModuleName string // 모듈 고유명 (가변 가능)
type OwnerName string  // 구조체/소유자 고유명 (가변 가능)
type SystemID uint64   // 시스템 전체 불변 ID (auto-increment)

// ---------- 싱글톤 ----------

var (
	instance *ONS
	once     sync.Once
)

// ---------- 상태 ----------

type ONS struct {
	mu sync.RWMutex

	// 모듈 → 포함 오너들
	moduleToIncludedOwners map[ModuleName][]OwnerName
	moduleDescs            map[ModuleName]string

	// 오너 ↔ 시스템ID
	ownerNameToSystemID map[OwnerName]SystemID
	systemToOwnerName   map[SystemID]OwnerName
	ownerDescs          map[OwnerName]string

	// 증가만 하는 다음 SystemID
	nextSystemID SystemID

	// 파일 경로
	moduleCSVPath string // ONS_modules.csv
	ownerCSVPath  string // ONS_owners.csv

	// init()에서 누적 등록 버퍼
	registeredModules   map[ModuleName]string // 모듈명 → 설명
	registeredRelations map[string]bool       // "모듈:오너" → true
	registeredOwners    map[OwnerName]string  // 오너명 → 설명

	// 초기화 상태
	initialized bool
	initOnce    sync.Once
}

// ---------- 옵션 ----------

type InitOptions struct {
	Interactive bool // 본 요구사항: 항상 true
}

var defaultInitOpts = InitOptions{Interactive: true}

// ---------- 싱글톤 ----------

func GetInstance() *ONS {
	once.Do(func() {
		instance = &ONS{
			moduleToIncludedOwners: make(map[ModuleName][]OwnerName),
			moduleDescs:            make(map[ModuleName]string),
			ownerNameToSystemID:    make(map[OwnerName]SystemID),
			systemToOwnerName:      make(map[SystemID]OwnerName),
			ownerDescs:             make(map[OwnerName]string),
			registeredModules:      make(map[ModuleName]string),
			registeredRelations:    make(map[string]bool),
			registeredOwners:       make(map[OwnerName]string),
			nextSystemID:           SystemID(loadNextSystemID()),
			moduleCSVPath:          filepath.Join(computation.GetModuleRoot(), "ONS", "ONS_modules.csv"),
			ownerCSVPath:           filepath.Join(computation.GetModuleRoot(), "ONS", "ONS_owners.csv"),
			initialized:            false,
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

func Initialize() error {
	return GetInstance().InitializeWith(defaultInitOpts)
}

func (m *ONS) InitializeWith(opts InitOptions) error {
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

	// A) OWNER 변경(선행, 인터랙티브)
	if err := m.applyOwnerDiffInteractive(); err != nil {
		return err
	}

	// B) MODULE 변경(후행, 인터랙티브)
	if err := m.applyModuleDiffInteractive(); err != nil {
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

// ---------- OWNER 단계 (선행) ----------

func (m *ONS) applyOwnerDiffInteractive() error {
	regNow := m.registeredOwners   // 실행 등록 (오너명 → 설명)
	exist := m.ownerNameToSystemID // CSV 상태   (오너명 → SystemID)

	newOwners := make(map[OwnerName]string) // 실행 O / CSV X
	missing := make(map[OwnerName]SystemID) // CSV O / 실행 X

	// (0) 설명만 변경 자동 반영
	descOnlyChanged := false
	for name, newDesc := range regNow {
		if _, ok := exist[name]; ok {
			ns := sanitizeDescription(newDesc)
			os := m.ownerDescs[name]
			if ns != os {
				fmt.Printf("ONS: OWNER [%s]의 설명이 변경되었습니다: \"%s\" -> \"%s\"\n", name, os, ns)
				m.ownerDescs[name] = ns
				descOnlyChanged = true
			}
		}
	}

	// new/missing 계산
	for name, desc := range regNow {
		if _, ok := exist[name]; !ok {
			newOwners[name] = sanitizeDescription(desc)
		}
	}
	for name, sid := range exist {
		if _, ok := regNow[name]; !ok {
			missing[name] = sid
		}
	}

	// new/missing이 없으면 설명만 변경 → 저장 후 종료
	if len(newOwners) == 0 && len(missing) == 0 {
		if descOnlyChanged {
			if err := m.saveOwnerCSV(); err != nil {
				return err
			}
		}
		return nil
	}

	// 결정적 순서
	newList := make([]OwnerName, 0, len(newOwners))
	for n := range newOwners {
		newList = append(newList, n)
	}
	sort.Slice(newList, func(i, j int) bool { return newList[i] < newList[j] })

	usedMissing := make(map[OwnerName]bool) // rename에 사용된 missing 재사용 금지

	// 1) 새 오너 처리 (원소별 재선택 루프)
	for _, newName := range newList {
		for {
			handled, err := m.handleNewOwnerInteractive(newName, newOwners[newName], missing, usedMissing)
			if err != nil {
				return err // 사용자 취소 등
			}
			if handled {
				break // 다음 원소로
			}
			// handled == false → reselect 요청 → 같은 원소 다시 루프
		}
	}

	// 2) 남은 missing OWNER 처리 (삭제/유지) — 원소별 재선택 루프
	if len(missing) > 0 {
		type pair struct {
			name OwnerName
			sys  SystemID
			desc string
		}
		var arr []pair
		for n, s := range missing {
			arr = append(arr, pair{n, s, m.ownerDescs[n]})
		}
		sort.Slice(arr, func(i, j int) bool { return arr[i].name < arr[j].name })

		for _, p := range arr {
			for {
				handled, err := m.handleMissingOwnerInteractive(p.name, p.sys, p.desc)
				if err != nil {
					return err
				}
				if handled {
					break // 다음 원소로
				}
				// reselect 요청 → 현재 원소 재질의
			}
		}
	}

	// 저장 (owner 변경은 모듈 포함 목록에도 영향 가능)
	if err := m.saveOwnerCSV(); err != nil {
		return err
	}
	if err := m.saveModuleCSV(); err != nil {
		return err
	}
	return nil
}

// 단일 새 OWNER 처리 (추가 vs rename) — reselect 지원
// 반환: handled=true 처리확정 / handled=false 재질의 / err != nil 중단
func (m *ONS) handleNewOwnerInteractive(
	newName OwnerName,
	desc string,
	missing map[OwnerName]SystemID,
	usedMissing map[OwnerName]bool,
) (bool, error) {

	sc := bufio.NewScanner(os.Stdin)
	fmt.Printf("\n=== ONS: 새로운 OWNER 감지 ===\n")
	fmt.Printf("새 OWNER: [%s]\n설명: %s\n", newName, desc)
	fmt.Println("이 항목은 무엇입니까?")
	fmt.Println("  1) 신규 추가 (새 SystemID 할당)")
	fmt.Println("  2) 기존 OWNER의 이름 변경 (rename)")

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
		if !confirmYesNo("이 OWNER를 새로 추가합니까? (yes/no): ") {
			// 사용자 “아니오” → reselect 여부
			if confirmYesNo("이 OWNER에 대한 선택을 다시 하시겠습니까? (yes/no): ") {
				return false, nil // reselect
			}
			return false, fmt.Errorf("owner addition aborted")
		}
		if !doubleCheck(fmt.Sprintf("%s를 ONS에서 새로운 OWNER로 추가합니다.", newName)) {
			if confirmYesNo("문장 일치 실패. 이 OWNER에 대한 선택을 다시 하시겠습니까? (yes/no): ") {
				return false, nil // reselect
			}
			return false, fmt.Errorf("owner addition failed double-check")
		}

		sysID := m.nextSystemID
		m.nextSystemID++
		if err := m.saveNextSystemID(); err != nil {
			return false, err
		}
		m.ownerNameToSystemID[newName] = sysID
		m.systemToOwnerName[sysID] = newName
		m.ownerDescs[newName] = sanitizeDescription(desc)
		fmt.Printf("→ 추가 완료 (SystemID: %d)\n", sysID)
		return true, nil
	}

	// choice == "2": rename
	if len(missing) == 0 {
		fmt.Println("rename 대상으로 사용할 기존 OWNER가 없습니다.")
		// reselect?
		if confirmYesNo("이 OWNER에 대한 선택을 다시 하시겠습니까? (yes/no): ") {
			return false, nil
		}
		return false, fmt.Errorf("no candidate to rename from")
	}

	// 후보 목록
	type cand struct {
		name OwnerName
		sys  SystemID
		desc string
	}
	var cands []cand
	for oldName, sid := range missing {
		if usedMissing[oldName] {
			continue
		}
		cands = append(cands, cand{oldName, sid, m.ownerDescs[oldName]})
	}
	if len(cands) == 0 {
		fmt.Println("rename 후보가 모두 사용되었습니다. 더 이상 rename 불가.")
		if confirmYesNo("이 OWNER에 대한 선택을 다시 하시겠습니까? (yes/no): ") {
			return false, nil
		}
		return false, fmt.Errorf("no remaining candidate to rename from")
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i].name < cands[j].name })

	fmt.Println("\n어떤 기존 OWNER 를", newName, "로 변경하시겠습니까?")
	for i, c := range cands {
		fmt.Printf("  %d) [%s] (SystemID: %d, %s)\n", i+1, c.name, c.sys, c.desc)
	}
	idx := -1
	for {
		fmt.Printf("번호(1~%d)를 입력하세요 (이전 선택으로 돌아가려면 0 입력하세요.): ", len(cands))
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

	// 최종 확인
	fmt.Printf("\n확인: OWNER [%s] → [%s] 로 이름을 변경합니다.\n", chosen.name, newName)
	if !confirmYesNo("이 변경을 진행합니까? (yes/no): ") {
		if confirmYesNo("이 OWNER에 대한 선택을 다시 하시겠습니까? (yes/no): ") {
			return false, nil // reselect
		}
		return false, fmt.Errorf("owner rename aborted")
	}
	if !doubleCheck(fmt.Sprintf("%s를 %s로 변경합니다.", chosen.name, newName)) {
		if confirmYesNo("문장 일치 실패. 이 OWNER에 대한 선택을 다시 하시겠습니까? (yes/no): ") {
			return false, nil // reselect
		}
		return false, fmt.Errorf("owner rename failed double-check")
	}

	// rename 적용 (SystemID 유지)
	sysID := m.ownerNameToSystemID[chosen.name]
	delete(m.ownerNameToSystemID, chosen.name)
	m.ownerNameToSystemID[newName] = sysID
	m.systemToOwnerName[sysID] = newName
	m.ownerDescs[newName] = sanitizeDescription(desc)
	delete(m.ownerDescs, chosen.name)
	m.replaceOwnerNameInModules(chosen.name, newName)

	usedMissing[chosen.name] = true
	delete(missing, chosen.name)
	fmt.Println("→ 이름 변경 완료 (SystemID 유지)")
	return true, nil
}

// 단일 missing OWNER 처리 (삭제/유지) — reselect 지원
func (m *ONS) handleMissingOwnerInteractive(name OwnerName, sysID SystemID, desc string) (bool, error) {
	sc := bufio.NewScanner(os.Stdin)
	fmt.Printf("\n기존 OWNER [%s] (SystemID: %d, %s)가 더 이상 등록되지 않았습니다.\n", name, sysID, desc)
	fmt.Println("이 OWNER를 어떻게 하시겠습니까?")
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

	// 삭제
	if !confirmYesNo("이 OWNER를 삭제합니까? (yes/no): ") {
		if confirmYesNo("이 OWNER에 대한 선택을 다시 하시겠습니까? (yes/no): ") {
			return false, nil // reselect
		}
		return false, fmt.Errorf("owner delete aborted")
	}
	if !doubleCheck(fmt.Sprintf("%s를 ONS에서 제거합니다.", name)) {
		if confirmYesNo("문장 일치 실패. 이 OWNER에 대한 선택을 다시 하시겠습니까? (yes/no): ") {
			return false, nil // reselect
		}
		return false, fmt.Errorf("owner delete failed double-check")
	}

	delete(m.ownerNameToSystemID, name)
	delete(m.systemToOwnerName, sysID)
	delete(m.ownerDescs, name)
	m.removeOwnerFromAllModules(name)
	fmt.Println("→ 삭제 완료")
	return true, nil
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

// ---------- MODULE 단계 (후행) ----------

func (m *ONS) applyModuleDiffInteractive() error {
	regNow := m.registeredModules // 실행 등록 (모듈명 → 설명)
	exist := m.moduleDescs        // CSV 상태   (모듈명 → 설명)

	newMods := make(map[ModuleName]string) // 실행 O / CSV X
	missing := make(map[ModuleName]string) // CSV O / 실행 X

	// (0) 설명만 변경 자동 반영
	descOnlyChanged := false
	for name, newDesc := range regNow {
		if _, ok := exist[name]; ok {
			ns := sanitizeDescription(newDesc)
			os := m.moduleDescs[name]
			if ns != os {
				fmt.Printf("ONS: MODULE [%s]의 설명이 변경되었습니다: \"%s\" -> \"%s\"\n", name, os, ns)
				m.moduleDescs[name] = ns
				descOnlyChanged = true
			}
		}
	}

	// new/missing 계산
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

	// new/missing 없고 설명만 변경 → 관계 보정 + 저장 후 종료
	if len(newMods) == 0 && len(missing) == 0 {
		m.applyRegisteredRelationsPostfix()
		if descOnlyChanged {
			if err := m.saveModuleCSV(); err != nil {
				return err
			}
			return nil
		}
		// desc도 안 바뀐 경우라도 관계 버퍼는 반영
		if err := m.saveModuleCSV(); err != nil {
			return err
		}
		return nil
	}

	// 결정적 순서
	newList := make([]ModuleName, 0, len(newMods))
	for n := range newMods {
		newList = append(newList, n)
	}
	sort.Slice(newList, func(i, j int) bool { return newList[i] < newList[j] })

	usedMissing := make(map[ModuleName]bool)

	// 1) 새 모듈 처리 (원소별 재선택 루프)
	for _, newName := range newList {
		for {
			handled, err := m.handleNewModuleInteractive(newName, newMods[newName], missing, usedMissing)
			if err != nil {
				return err
			}
			if handled {
				break
			}
		}
	}

	// 2) 남은 missing 모듈 처리 (원소별 재선택 루프)
	if len(missing) > 0 {
		names := make([]ModuleName, 0, len(missing))
		for n := range missing {
			names = append(names, n)
		}
		sort.Slice(names, func(i, j int) bool { return names[i] < names[j] })

		for _, oldName := range names {
			for {
				handled, err := m.handleMissingModuleInteractive(oldName, m.moduleDescs[oldName])
				if err != nil {
					return err
				}
				if handled {
					break
				}
			}
		}
	}

	// 3) init()에서 등록된 관계 보정 + 저장
	m.applyRegisteredRelationsPostfix()
	return m.saveModuleCSV()
}

// 단일 새 MODULE 처리 (추가 vs rename) — reselect 지원
func (m *ONS) handleNewModuleInteractive(
	newName ModuleName,
	desc string,
	missing map[ModuleName]string,
	usedMissing map[ModuleName]bool,
) (bool, error) {

	sc := bufio.NewScanner(os.Stdin)
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
			if confirmYesNo("이 MODULE에 대한 선택을 다시 하시겠습니까? (yes/no): ") {
				return false, nil
			}
			return false, fmt.Errorf("module addition aborted")
		}
		if !doubleCheck(fmt.Sprintf("%s를 ONS에 신규 MODULE로 추가합니다.", newName)) {
			if confirmYesNo("문장 일치 실패. 이 MODULE에 대한 선택을 다시 하시겠습니까? (yes/no): ") {
				return false, nil
			}
			return false, fmt.Errorf("module addition failed double-check")
		}

		m.moduleDescs[newName] = sanitizeDescription(desc)
		if _, ok := m.moduleToIncludedOwners[newName]; !ok {
			m.moduleToIncludedOwners[newName] = []OwnerName{}
		}
		fmt.Println("→ 모듈 추가 완료")
		return true, nil
	}

	// choice == "2": rename
	if len(missing) == 0 {
		fmt.Println("rename 대상으로 사용할 기존 MODULE 이 없습니다.")
		if confirmYesNo("이 MODULE에 대한 선택을 다시 하시겠습니까? (yes/no): ") {
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
	for oldName, oldDesc := range missing {
		if usedMissing[oldName] {
			continue
		}
		owners := append([]OwnerName(nil), m.moduleToIncludedOwners[oldName]...)
		sort.Slice(owners, func(i, j int) bool { return owners[i] < owners[j] })
		cands = append(cands, cand{oldName, oldDesc, owners})
	}
	if len(cands) == 0 {
		fmt.Println("rename 후보가 모두 사용되었습니다. 더 이상 rename 불가.")
		if confirmYesNo("이 MODULE에 대한 선택을 다시 하시겠습니까? (yes/no): ") {
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
		fmt.Printf("번호(1~%d)를 입력하세요 (이전 선택으로 돌아가려면 0을 입력하세요): ", len(cands))
		scan := bufio.NewScanner(os.Stdin)
		scan.Scan()
		s := strings.TrimSpace(scan.Text())

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
		if confirmYesNo("이 MODULE에 대한 선택을 다시 하시겠습니까? (yes/no): ") {
			return false, nil
		}
		return false, fmt.Errorf("module rename aborted")
	}
	if !doubleCheck(fmt.Sprintf("%s를 %s로 변경하겠습니다.", chosen.name, newName)) {
		if confirmYesNo("문장 일치 실패. 이 MODULE에 대한 선택을 다시 하시겠습니까? (yes/no): ") {
			return false, nil
		}
		return false, fmt.Errorf("module rename failed double-check")
	}

	// rename 적용
	m.moduleDescs[newName] = sanitizeDescription(desc) // 최신 설명
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
}

// 단일 missing MODULE 처리 (삭제/유지) — reselect 지원
func (m *ONS) handleMissingModuleInteractive(name ModuleName, desc string) (bool, error) {
	sc := bufio.NewScanner(os.Stdin)
	fmt.Printf("\n기존 MODULE [%s] (%s)가 더 이상 등록되지 않았습니다.\n", name, desc)
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

	// 삭제
	if !confirmYesNo("이 MODULE 을 삭제합니까? (yes/no): ") {
		if confirmYesNo("이 MODULE에 대한 선택을 다시 하시겠습니까? (yes/no): ") {
			return false, nil // reselect
		}
		return false, fmt.Errorf("module delete aborted")
	}
	if !doubleCheck(fmt.Sprintf("%s를 ONS에서 제거합니다.", name)) {
		if confirmYesNo("문장 일치 실패. 이 MODULE에 대한 선택을 다시 하시겠습니까? (yes/no): ") {
			return false, nil // reselect
		}
		return false, fmt.Errorf("module delete failed double-check")
	}

	delete(m.moduleDescs, name)
	delete(m.moduleToIncludedOwners, name)
	fmt.Println("→ 모듈 삭제 완료")
	return true, nil
}

// registeredRelations 버퍼 반영 (존재 모듈만)
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

// ---------- CSV/ID IO ----------

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

func loadNextSystemID() uint64 {
	idPath := filepath.Join(computation.GetModuleRoot(), "ONS", "ONS_next_id.txt")
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
	idPath := filepath.Join(computation.GetModuleRoot(), "ONS", "ONS_next_id.txt")
	data := []byte(strconv.FormatUint(uint64(m.nextSystemID), 10))
	tmp := idPath + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, idPath)
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

// ---------- 퍼블릭 API ----------

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
	sysID := m.nextSystemID
	m.nextSystemID++
	_ = m.saveNextSystemID()

	m.ownerNameToSystemID[owner] = sysID
	m.systemToOwnerName[sysID] = owner
	m.ownerDescs[owner] = sanitizeDescription(description)

	_ = m.saveOwnerCSV()
	return sysID
}

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
	return "", fmt.Errorf("SystemID %d not found", systemID)
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
