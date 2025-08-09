package main

import (
	"fmt"
	"log"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/groundknowledge/ons"
)

const (
	ModuleID      = ons.ModuleName("ex_leys")
	NewModuleName = ons.ModuleName("ex_user")

	// 구조체 ID들
	UserLLStructID   = ons.OwnerName("ex_UserLL")
	ProfileStructID  = ons.OwnerName("ex_Profile")
	SettingsStructID = ons.OwnerName("ex_Settings")
	UserStructName   = ons.OwnerName("ex_User")
)

var (
	// SystemID들을 저장
	UserSystemID     ons.SystemID
	ProfileSystemID  ons.SystemID
	SettingsSystemID ons.SystemID
)

func init() {
	//0. 새 모듈 등록
	ons.RegisterModule(NewModuleName, "신규 모듈~~~")

	ons.RegisterRelation(NewModuleName, UserStructName)
	ons.RegisterOwner(UserStructName, "신규 구조체")
	// 1. 모듈 등록
	ons.RegisterModule(ModuleID, "사용자 관련 모듈")

	// 2. 모듈-구조체 관계 등록
	ons.RegisterRelation(ModuleID, UserLLStructID)
	ons.RegisterRelation(ModuleID, SettingsStructID)

	// 3. 구조체 등록 (SystemID는 Initialize 후에 할당됨)
	ons.RegisterOwner(UserLLStructID, "사용자의!!,,@@ 기본 정보")
	ons.RegisterOwner(SettingsStructID, "ssㄴㄴ")

}

// User 구조체
type User struct {
	ID   int
	Name string
}

// GetSystemID는 User 구조체의 System ID 반환
func (u *User) GetSystemID() ons.SystemID {
	if UserSystemID == 0 {
		UserSystemID, _ = ons.GetSystemIdByOwnerName(UserLLStructID)
	}
	return UserSystemID
}

func main() {

	// 이제 SystemID 사용 가능

	userSysID, err := ons.GetSystemIdByOwnerName("User")
	println("(얘는 없어야 하는 것)")
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		fmt.Printf("User SystemID: %d\n", userSysID)
	}

	// 2. SystemID로 구조체ID 얻기
	structID, err := ons.GetOwnerNameBySystemID(userSysID)
	if err == nil {
		fmt.Printf("SystemID %d -> Struct: %s\n", userSysID, structID)
	}

	// 3. 모듈의 모든 구조체 조회
	userStructs := ons.GetOwnersOfModule(ModuleID)
	fmt.Printf("\nuser 모듈의 구조체들: %v\n", userStructs)

	// 4. 각 구조체의 SystemID 확인
	fmt.Println("\n모든 구조체의 SystemID:")
	for _, sid := range userStructs {
		if sysID, err := ons.GetSystemIdByOwnerName(sid); err == nil {
			fmt.Printf("  %s -> SystemID: %d\n", sid, sysID)
		}
	}
}
