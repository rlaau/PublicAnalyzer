package main

import (
	"fmt"
	"log"

	"github.com/rlaaudgjs5638/chainAnalyzer/shared/groundknowledge/ons"
)

const (
	ModuleID      = ons.ModuleName("leys")
	NewModuleName = ons.ModuleName("user")

	// 구조체 ID들
	UserLLStructID   = ons.OwnerName("UserL")
	ProfileStructID  = ons.OwnerName("Profile")
	SettingsStructID = ons.OwnerName("Settings")
	UserStructName   = ons.OwnerName("User")
)

var (
	// SystemID들을 저장
	UserSystemID     ons.SystemID
	ProfileSystemID  ons.SystemID
	SettingsSystemID ons.SystemID
)

func init() {
	//0. 새 모듈 등록
	ons.RegisterModule(NewModuleName, "신규 모듈")

	ons.RegisterRelation(NewModuleName, UserStructName)
	ons.RegisterOwner(UserStructName, "신규 구조체")
	// 1. 모듈 등록
	ons.RegisterModule(ModuleID, "사용자 관련 모듈")

	// 2. 모듈-구조체 관계 등록
	ons.RegisterRelation(ModuleID, UserLLStructID)
	ons.RegisterRelation(ModuleID, ProfileStructID)
	ons.RegisterRelation(ModuleID, SettingsStructID)

	// 3. 구조체 등록 (SystemID는 Initialize 후에 할당됨)
	ons.RegisterOwner(UserLLStructID, "사용자 기본 정보")
	ons.RegisterOwner(ProfileStructID, "사용자 프로필")
	ons.RegisterOwner(SettingsStructID, "ssss")

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

	// 1. 구조체ID로 시스템ID 얻기 (핵심 기능!)
	userSysID, err := ons.GetSystemIdByOwnerName("User")
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
	fmt.Printf("\nuser 모듈의 구조체들(없는게 정상임): %v\n", userStructs)

	// 4. 각 구조체의 SystemID 확인
	fmt.Println("\n모든 구조체의 SystemID:")
	for _, sid := range userStructs {
		if sysID, err := ons.GetSystemIdByOwnerName(sid); err == nil {
			fmt.Printf("  %s -> SystemID: %d\n", sid, sysID)
		}
	}
}
