package mns

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/computation"
)

// ModuleName는 모듈의 고유 식별자
type ModuleName string

// StructName는 구조체의 고유 식별자
type StructName string

// SystemID는 시스템 전체에서 고유한 ID (auto increment)
type SystemID uint64

// 싱글톤 인스턴스
var (
	instance *MNS
	once     sync.Once
)

// MNS는 ModuleNameSystem의 핵심 구조체
type MNS struct {
	mu sync.RWMutex

	// 매핑 1: 모듈 -> 구조체들
	// 아키텍쳐 레벨의 관리를 위함
	moduleToStructs map[ModuleName][]StructName // 모듈이 포함하는 구조체들
	moduleDescs     map[ModuleName]string       // 모듈 설명

	// 매핑 2: 구조체 -> 시스템ID
	// 실제 시스템 자원은 오직 구조체ID를 통해 접근함.
	structToSystem map[StructName]SystemID // 구조체 -> 시스템ID
	systemToStruct map[SystemID]StructName // 시스템ID -> 구조체
	structDescs    map[StructName]string   // 구조체 설명

	// 다음 시스템ID
	nextSystemID SystemID

	// CSV 파일 경로
	moduleCSVPath string
	structCSVPath string

	// init에서 등록된 항목들 추적
	registeredModules   map[ModuleName]string // 모듈ID -> 설명
	registeredRelations map[string]bool       // "모듈ID:구조체ID" -> true
	registeredStructs   map[StructName]string // 구조체ID -> 설명

	// 초기화 완료 여부
	initialized bool
	initOnce    sync.Once // 초기화는 한 번만
}

// GetInstance는 MNS 싱글톤 인스턴스 반환
func GetInstance() *MNS {
	once.Do(func() {
		instance = &MNS{
			moduleToStructs:     make(map[ModuleName][]StructName),
			moduleDescs:         make(map[ModuleName]string),
			structToSystem:      make(map[StructName]SystemID),
			systemToStruct:      make(map[SystemID]StructName),
			structDescs:         make(map[StructName]string),
			registeredModules:   make(map[ModuleName]string),
			registeredRelations: make(map[string]bool),
			registeredStructs:   make(map[StructName]string),
			nextSystemID:        1,
			moduleCSVPath:       computation.GetModuleRoot() + "/mns_modules.csv",
			structCSVPath:       computation.GetModuleRoot() + "/mns_structs.csv",
			initialized:         false,
		}
	})
	return instance
}

// ensureInitialized는 초기화가 필요한 경우 자동으로 초기화 수행
func (m *MNS) ensureInitialized() {
	m.initOnce.Do(func() {
		// runtime.Gosched()를 호출하여 다른 고루틴(init)들이 실행될 기회를 줌
		runtime.Gosched()

		// 약간의 지연을 줘서 모든 init()이 완료되도록 함
		time.Sleep(10 * time.Millisecond)

		if err := m.Initialize(); err != nil {
			panic(fmt.Sprintf("MNS 자동 초기화 실패: %v", err))
		}
	})
}

// RegisterModule은 모듈을 등록
func (m *MNS) RegisterModule(moduleID ModuleName, description string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	//일단은 버퍼에만 넣어두는 것
	//init()시에 아직은 모듈명 추가,변경,삭제를 고려x
	if !m.initialized {
		m.registeredModules[moduleID] = description
		return
	}

	m.moduleDescs[moduleID] = description
	if _, exists := m.moduleToStructs[moduleID]; !exists {
		m.moduleToStructs[moduleID] = []StructName{}
	}
	m.saveModuleCSV()
}

// RegisterRelation은 모듈-구조체 관계를 등록
func (m *MNS) RegisterRelation(moduleID ModuleName, structID StructName) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%s:%s", moduleID, structID)
	// 일단 버퍼에 등록
	if !m.initialized {
		m.registeredRelations[key] = true
		return
	}

	// 모듈이 없으면 생성
	if _, exists := m.moduleToStructs[moduleID]; !exists {
		m.moduleToStructs[moduleID] = []StructName{}
	}

	// 이미 관계가 있는지 확인
	for _, sid := range m.moduleToStructs[moduleID] {
		if sid == structID {
			return
		}
	}

	m.moduleToStructs[moduleID] = append(m.moduleToStructs[moduleID], structID)
	m.saveModuleCSV()
}

// RegisterStruct는 구조체를 등록하고 SystemID 반환
func (m *MNS) RegisterStruct(structID StructName, description string) SystemID {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.initialized {
		m.registeredStructs[structID] = description
		return 0
	}

	// 이미 등록되어 있으면 기존 ID 반환
	if sysID, exists := m.structToSystem[structID]; exists {
		// 설명 업데이트
		m.structDescs[structID] = description
		m.saveStructCSV()
		return sysID
	}

	// 새 시스템ID 할당
	sysID := m.nextSystemID
	m.nextSystemID++

	m.structToSystem[structID] = sysID
	m.systemToStruct[sysID] = structID
	m.structDescs[structID] = description

	m.saveStructCSV()
	return sysID
}

// GetSystemID는 구조체ID로 시스템ID 조회
func (m *MNS) GetSystemID(structID StructName) (SystemID, error) {
	m.ensureInitialized()

	m.mu.RLock()
	defer m.mu.RUnlock()

	if sysID, exists := m.structToSystem[structID]; exists {
		return sysID, nil
	}

	return 0, fmt.Errorf("struct %s not found", structID)
}

// GetStructID는 시스템ID로 구조체ID 조회
func (m *MNS) GetStructID(systemID SystemID) (StructName, error) {
	m.ensureInitialized()

	m.mu.RLock()
	defer m.mu.RUnlock()

	if structID, exists := m.systemToStruct[systemID]; exists {
		return structID, nil
	}

	return "", fmt.Errorf("SystemID %d not found", systemID)
}

// GetModuleStructs는 모듈의 모든 구조체 조회
func (m *MNS) GetModuleStructs(moduleID ModuleName) []StructName {
	m.ensureInitialized()
	m.mu.RLock()
	defer m.mu.RUnlock()

	if structs, exists := m.moduleToStructs[moduleID]; exists {
		result := make([]StructName, len(structs))
		copy(result, structs)
		return result
	}

	return []StructName{}
}

// Initialize는 모든 init 등록 후 호출하여 diff 처리
func (m *MNS) Initialize() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.initialized {
		return nil
	}

	// 1단계: 모듈 CSV 로드 및 diff 처리
	if err := m.loadModuleCSV(); err != nil && !os.IsNotExist(err) {
		return err
	}

	if err := m.processModuleDiff(); err != nil {
		return err
	}

	// 2단계: 구조체 CSV 로드 및 diff 처리
	if err := m.loadStructCSV(); err != nil && !os.IsNotExist(err) {
		return err
	}

	if err := m.processStructDiff(); err != nil {
		return err
	}

	m.initialized = true
	return nil
}

// doubleCheck는 중요한 작업 전 사용자에게 확인 문구를 입력받아 검증
func doubleCheck(expectedPhrase string) bool {
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Printf("\n확인을 위해 다음 문장을 정확히 입력해주세요:\n\"%s\"\n입력 (또는 'no'로 취소): ", expectedPhrase)
		scanner.Scan()
		input := strings.TrimSpace(scanner.Text())

		if input == expectedPhrase {
			return true
		} else if strings.ToLower(input) == "no" {
			return false
		} else {
			fmt.Println("입력이 일치하지 않습니다. 다시 시도해주세요.")
		}
	}
}

// confirmYesNo는 yes/no 확인을 받아 true/false 반환
func confirmYesNo(prompt string) bool {
	prompt = "다시 묻겠습니다." + prompt
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print(prompt)
		scanner.Scan()
		input := strings.ToLower(strings.TrimSpace(scanner.Text()))

		switch input {
		case "yes":
			return true
		case "no":
			return false
		default:
			fmt.Println("올바른 입력이 아닙니다. yes 또는 no로 입력해주세요.")
		}
	}
}

// processModuleDiff는 모듈 관련 변경사항 처리
func (m *MNS) processModuleDiff() error {
	scanner := bufio.NewScanner(os.Stdin)

	// 새로 등록된 모듈들
	newModules := make(map[ModuleName]string)
	for modID, desc := range m.registeredModules {
		if _, exists := m.moduleDescs[modID]; !exists {
			newModules[modID] = desc
		}
	}

	// CSV에 있지만 등록되지 않은 모듈들
	missingModules := make(map[ModuleName]string)
	for modID, desc := range m.moduleDescs {
		if _, exists := m.registeredModules[modID]; !exists {
			missingModules[modID] = desc
		}
	}

	if len(newModules) > 0 || len(missingModules) > 0 {
		fmt.Println("\n=== MNS: 모듈 변경사항 감지 ===")

		// 1. 새 모듈 처리 (추가 또는 변경)
		for modName, desc := range newModules {
			fmt.Printf("\n등록되지 않은 모듈 발견. 신규 모듈 혹은 수정 모듈입니다: [%s] (%s)\n", modName, desc)

			// 1단계: 추가인지 확인
			fmt.Print("저장된 모듈과 이름이 다른 모듈입니다. 이것은 기존 모델 이름을 변경한 것이 아닌, 새로 추가된 모듈입니까? \n 신중히 답해주세요. (yes/no): ")
			scanner.Scan()

			if confirmYesNo("이것은 이름 변경이 아니라, 새 모듈 추가입니까? (yes/no): ") && (doubleCheck(fmt.Sprintf("%s를 MNS에 신규 모듈로 추가합니다.", modName))) {
				// 추가 확정

				m.moduleDescs[modName] = desc
				if _, exists := m.moduleToStructs[modName]; !exists {
					m.moduleToStructs[modName] = []StructName{}
				}
				fmt.Println("→ 새 모듈로 추가되었습니다.")
			} else {
				// 2단계: 변경인지 확인
				if len(missingModules) > 0 {
					fmt.Println("\n그렇다면 모듈 이름이 변경된 것이군요 \n 다음 중 어떤 모듈이 입력한 모듈 이름으로 변경되었나요?")
					candidates := make([]ModuleName, 0)
					idx := 1
					for oldID, oldDesc := range missingModules {
						fmt.Printf("%d. [%s] (%s)\n", idx, oldID, oldDesc)
						candidates = append(candidates, oldID)
						idx++
					}
					fmt.Printf("%d. 해당 없음\n", idx)

					fmt.Print("번호를 선택하세요: ")
					scanner.Scan()
					choiceStr := scanner.Text()
					choice, err := strconv.Atoi(choiceStr)

					if err == nil && choice > 0 && choice <= len(candidates) {
						oldID := candidates[choice-1]
						fmt.Printf("\n확인: [%s] → [%s]로 변경이 맞습니까? (yes/no): ", oldID, modName)
						scanner.Scan()

						if confirmYesNo("이것은 이름을 변경하는 것입니까? (yes/no): ") && (doubleCheck(fmt.Sprintf("%s를 %s로 변경하겠습니다.", oldID, modName))) {
							// 변경 확정
							m.moduleDescs[modName] = desc
							delete(m.moduleDescs, oldID)

							if structs, exists := m.moduleToStructs[oldID]; exists {
								m.moduleToStructs[modName] = structs
								delete(m.moduleToStructs, oldID)
							}

							delete(missingModules, oldID)
							fmt.Println("→ 모듈 이름이 변경되었습니다.")
						}
					} else {
						// 3단계: 삭제 확인
						fmt.Printf("\n뭔가 실수 하셨습니다. 추가된 모듈이거나 변경된 모듈이어야 합니다. \n현재 MNS상의 변경은 없으니 잠시 끄고 다시 생각하시기 바랍니다. \n 정말 실수가 아니라면, 모듈 [%s]를 삭제하시겠습니까? (yes!!!!!!!!!라고 하면 삭제함): ", modName)
						scanner.Scan()
						deleteAnswer := strings.ToLower(strings.TrimSpace(scanner.Text()))

						if deleteAnswer == "yes!!!!!!!!!" {
							fmt.Println("→ 모듈이 삭제 처리되었습니다.")
							// 등록 목록에서 제거
							delete(m.registeredModules, modName)
						}
					}
				} else {
					// missingModules가 없으면 바로 삭제 확인
					fmt.Printf("\n이 모듈이 추가된 것도 아니고 변경할 것도 아닌 겁니까?\n뭔가 실수한 것 같습니다. 재고해 보십시오 \n굳이 정말 모듈 [%s]를 삭제하시겠습니까? (yes!!!!!!!!!라고 하면 삭제함): ", modName)
					scanner.Scan()
					deleteAnswer := strings.ToLower(strings.TrimSpace(scanner.Text()))

					if deleteAnswer == "yes!!!!!!!!!" {
						fmt.Println("→ 모듈이 삭제 처리되었습니다.")
						delete(m.registeredModules, modName)
					}
				}
			}
		}

		// 2. 남은 missing 모듈들 처리 (삭제 확인)
		for oldName, desc := range missingModules {
			fmt.Printf("\n기존 모듈 [%s] (%s)가 더 이상 등록되지 않았습니다.\n", oldName, desc)
			fmt.Print("이 모듈을 삭제하시겠습니까? (yes/no): ")
			scanner.Scan()

			if confirmYesNo("이것은 모듈 이름을 삭제하는 것입니까? (yes/no): ") && (doubleCheck(fmt.Sprintf("%s를 MNS에서 제거합니다.", oldName))) {
				delete(m.moduleDescs, oldName)
				delete(m.moduleToStructs, oldName)
				fmt.Println("→ 모듈이 삭제되었습니다.")
			} else {
				// 삭제하지 않으면 유지
				fmt.Println("→ 모듈이 유지됩니다.")
			}
		}

		// 관계 처리
		for key := range m.registeredRelations {
			parts := strings.Split(key, ":")
			if len(parts) == 2 {
				moduleID := ModuleName(parts[0])
				structID := StructName(parts[1])

				// 모듈이 존재하는 경우에만 관계 추가
				if _, exists := m.moduleToStructs[moduleID]; exists {
					found := false
					for _, sid := range m.moduleToStructs[moduleID] {
						if sid == structID {
							found = true
							break
						}
					}
					if !found {
						m.moduleToStructs[moduleID] = append(m.moduleToStructs[moduleID], structID)
					}
				}
			}
		}
	}

	return m.saveModuleCSV()
}

// processStructDiff는 구조체 관련 변경사항 처리
func (m *MNS) processStructDiff() error {
	scanner := bufio.NewScanner(os.Stdin)

	// 새로 등록된 구조체들
	newStructs := make(map[StructName]string)
	for structID, desc := range m.registeredStructs {
		if _, exists := m.structToSystem[structID]; !exists {
			newStructs[structID] = desc
		}
	}

	// CSV에 있지만 등록되지 않은 구조체들
	missingStructs := make(map[StructName]SystemID)
	for structID, sysID := range m.structToSystem {
		if _, exists := m.registeredStructs[structID]; !exists {
			missingStructs[structID] = sysID
		}
	}

	if len(newStructs) > 0 || len(missingStructs) > 0 {
		fmt.Println("\n=== MNS: 구조체 변경사항 감지 ===")

		// 1. 새 구조체 처리 (추가 또는 변경)
		for structName, desc := range newStructs {
			fmt.Printf("\n등록되지 않은 구조체 이름 발견. 신규 구조체 혹은 수정된 구조체 입니다: [%s] (%s)\n", structName, desc)

			// 1단계: 추가인지 확인
			fmt.Print("이것은 구조체 이름이 변경된 걸수도 있고, 구조체가 아예 추가된 것일 수 있습나다. \n 이것은 변경이 아닌, 새로 추가된 구조체입니까? (yes/no): ")
			scanner.Scan()

			if confirmYesNo("이것은 모듈 이름 변경이 아닌, 새 모듈 등록하는 것입니까?(yes/no): ") && (doubleCheck(fmt.Sprintf("%s를 MNS에서 새로운 구조체로 추가합니다.", structName))) {
				// 추가 확정
				sysID := m.nextSystemID
				m.nextSystemID++

				m.structToSystem[structName] = sysID
				m.systemToStruct[sysID] = structName
				m.structDescs[structName] = desc
				fmt.Printf("→ 새 구조체로 추가되었습니다. (SystemID: %d)\n", sysID)
			} else {
				// 2단계: 변경인지 확인
				if len(missingStructs) > 0 {
					fmt.Println("\n구조체 이름이 변경된 것이라 하였습니다. \n다음 중 어떤 구조체 이름이 입력한 구조체체 이름으로 변경되었나요?")
					candidates := make([]StructName, 0)
					candidateSysIDs := make([]SystemID, 0)
					idx := 1
					for oldID, sysID := range missingStructs {
						desc := m.structDescs[oldID]
						fmt.Printf("%d. [%s] (SystemID: %d, %s)\n", idx, oldID, sysID, desc)
						candidates = append(candidates, oldID)
						candidateSysIDs = append(candidateSysIDs, sysID)
						idx++
					}
					fmt.Printf("%d. 해당 없음\n", idx)

					fmt.Print("번호를 선택하세요: ")
					scanner.Scan()
					choiceStr := scanner.Text()
					choice, err := strconv.Atoi(choiceStr)

					if err == nil && choice > 0 && choice <= len(candidates) {
						oldName := candidates[choice-1]
						sysID := candidateSysIDs[choice-1]

						fmt.Printf("\n확인: [%s] → [%s]로 변경이 맞습니까? (yes/no): ", oldName, structName)
						scanner.Scan()

						if confirmYesNo("이것은 모듈 이름을 변경하는 것입니까? (yes/no): ") && (doubleCheck(fmt.Sprintf("%s를 MNS에서 %s로 변경합니다.", oldName, structName))) {
							// 변경 확정
							delete(m.structToSystem, oldName)
							m.structToSystem[structName] = sysID
							m.systemToStruct[sysID] = structName
							m.structDescs[structName] = desc
							delete(m.structDescs, oldName)

							delete(missingStructs, oldName)
							fmt.Printf("→ 구조체 이름이 변경되었습니다. (SystemID %d 유지)\n", sysID)
						}
					} else {
						// 3단계: 삭제 확인
						fmt.Printf("\n구조체 [%s]를 삭제하시겠습니까? 근데 뭔가 실수한듯 합니다. (yes!!!!!!시 허용/no): ", structName)
						scanner.Scan()
						deleteAnswer := strings.ToLower(strings.TrimSpace(scanner.Text()))

						if deleteAnswer == "yes!!!!!!" {
							fmt.Println("→ 구조체가 삭제 처리되었습니다.")
							delete(m.registeredStructs, structName)
						}
					}
				} else {
					// missingStructs가 없으면 바로 삭제 확인
					fmt.Printf("\n구조체 [%s]를 삭제하시겠습니까? (yes/no): ", structName)
					scanner.Scan()

					if confirmYesNo("이것은 모듈을 삭제하는 것입니까? (yes/no): ") && (doubleCheck(fmt.Sprintf("%s를 MNS에서 제거합니다.", structName))) {
						fmt.Println("→ 구조체가 삭제 처리되었습니다.")
						delete(m.registeredStructs, structName)
					}
				}
			}
		}

		// 2. 남은 missing 구조체들 처리 (삭제 확인)
		for structID, sysID := range missingStructs {
			desc := m.structDescs[structID]
			fmt.Printf("\n기존 구조체 [%s] (SystemID: %d, %s)가 더 이상 등록되지 않았습니다.\n",
				structID, sysID, desc)
			fmt.Print("이 구조체를 삭제하시겠습니까? (yes/no): ")
			scanner.Scan()
			answer := strings.ToLower(strings.TrimSpace(scanner.Text()))

			if answer == "yes" || answer == "y" {
				delete(m.structToSystem, structID)
				delete(m.systemToStruct, sysID)
				delete(m.structDescs, structID)
				fmt.Println("→ 구조체가 삭제되었습니다.")
			} else {
				// 삭제하지 않으면 유지
				fmt.Println("→ 구조체가 유지됩니다.")
			}
		}
	}

	return m.saveStructCSV()
}

// loadModuleCSV는 모듈 CSV 로드
func (m *MNS) loadModuleCSV() error {
	file, err := os.Open(m.moduleCSVPath)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.LazyQuotes = true // 한글 처리를 위해
	records, err := reader.ReadAll()
	if err != nil {
		return err
	}

	if len(records) > 0 {
		records = records[1:] // 헤더 스킵
	}

	for _, record := range records {
		if len(record) >= 3 {
			moduleID := ModuleName(record[0])
			desc := record[1]
			structsStr := record[2]

			m.moduleDescs[moduleID] = desc

			// 구조체 목록 파싱
			structs := []StructName{}
			if structsStr != "" {
				for _, s := range strings.Split(structsStr, ";") {
					if s != "" {
						structs = append(structs, StructName(s))
					}
				}
			}
			m.moduleToStructs[moduleID] = structs
		}
	}

	return nil
}

// saveModuleCSV는 모듈 정보 저장
func (m *MNS) saveModuleCSV() error {
	file, err := os.Create(m.moduleCSVPath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 헤더
	if err := writer.Write([]string{"ModuleID", "Description", "Structs"}); err != nil {
		return err
	}

	// 데이터
	for modID, desc := range m.moduleDescs {
		structs := m.moduleToStructs[modID]
		structStrs := make([]string, len(structs))
		for i, s := range structs {
			structStrs[i] = string(s)
		}
		structsStr := strings.Join(structStrs, ";")

		if err := writer.Write([]string{string(modID), desc, structsStr}); err != nil {
			return err
		}
	}

	return nil
}

// loadStructCSV는 구조체 CSV 로드
func (m *MNS) loadStructCSV() error {
	file, err := os.Open(m.structCSVPath)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.LazyQuotes = true // 한글 처리를 위해
	records, err := reader.ReadAll()
	if err != nil {
		return err
	}

	if len(records) > 0 {
		records = records[1:] // 헤더 스킵
	}

	for _, record := range records {
		if len(record) >= 3 {
			structID := StructName(record[0])
			sysID, err := strconv.ParseUint(record[1], 10, 64)
			if err != nil {
				continue
			}
			desc := record[2]

			m.structToSystem[structID] = SystemID(sysID)
			m.systemToStruct[SystemID(sysID)] = structID
			m.structDescs[structID] = desc

			if SystemID(sysID) >= m.nextSystemID {
				m.nextSystemID = SystemID(sysID) + 1
			}
		}
	}

	return nil
}

// saveStructCSV는 구조체 정보 저장
func (m *MNS) saveStructCSV() error {
	file, err := os.Create(m.structCSVPath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 헤더
	if err := writer.Write([]string{"StructID", "SystemID", "Description"}); err != nil {
		return err
	}

	// 데이터
	for structID, sysID := range m.structToSystem {
		desc := m.structDescs[structID]
		if err := writer.Write([]string{
			string(structID),
			strconv.FormatUint(uint64(sysID), 10),
			desc,
		}); err != nil {
			return err
		}
	}

	return nil
}

// 편의 함수들

// RegisterModule은 전역 MNS에 모듈 등록
func RegisterModule(moduleID ModuleName, description string) {
	GetInstance().RegisterModule(moduleID, description)
}

// RegisterRelation은 전역 MNS에 관계 등록
func RegisterRelation(moduleID ModuleName, structID StructName) {
	GetInstance().RegisterRelation(moduleID, structID)
}

// RegisterStruct는 전역 MNS에 구조체 등록
func RegisterStruct(structID StructName, description string) SystemID {
	return GetInstance().RegisterStruct(structID, description)
}

// GetSystemIDByStuctID는 전역 MNS에서 조회
func GetSystemIDByStuctID(structID StructName) (SystemID, error) {
	return GetInstance().GetSystemID(structID)
}

// GetStructIDBySystemID는 전역 MNS에서 조회
func GetStructIDBySystemID(systemID SystemID) (StructName, error) {
	return GetInstance().GetStructID(systemID)
}

// GetStructsOfModule는 전역 MNS에서 조회
func GetStructsOfModule(moduleID ModuleName) []StructName {
	return GetInstance().GetModuleStructs(moduleID)
}

// Initialize는 전역 MNS 초기화
func Initialize() error {
	return GetInstance().Initialize()
}
