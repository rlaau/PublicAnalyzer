package computation

import (
	"os"
	"path/filepath"
)

func FindWebDir() string {
	rootPath := FindProjectRootPath()
	webDir := filepath.Join(rootPath, "web")
	return webDir

}

// rel을 받아서 baseDir+rel을 하는 함수 반환
func ComputeRelClosure(baseDir string) func(string) string {
	return func(rel string) string {
		return filepath.Join(baseDir, rel)
	}
}

// 고립환경 테스팅 시의 루트패시 전달
// 사용 시엔 FindTestingStorageRootPath+{테스트명}+{각 저장소}로 쓰면 됨
func FindTestingStorageRootPath() string {
	return filepath.Join(FindProjectRootPath(), "testing_enviroment")
}

func FindProductionStorageRootPath() string {
	return filepath.Join(FindProjectRootPath(), "production_storage")
}

// 프로젝트 루트 경로 리턴
func FindProjectRootPath() string {
	cwd, _ := os.Getwd()
	for {
		if _, err := os.Stat(filepath.Join(cwd, "go.mod")); err == nil {
			return cwd
		}
		parent := filepath.Dir(cwd)
		if parent == cwd { // 루트까지 왔는데 못 찾으면 중단
			break
		}
		cwd = parent
	}
	return ""
}
