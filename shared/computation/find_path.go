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

func ComputeRelClosure(root string) func(...string) string {
	return func(subpaths ...string) string {
		return filepath.Join(append([]string{root}, subpaths...)...)
	}
}

// @/testing_enviroment/{name1/name2/...}
func ComputeThisTestingStorage(name ...string) string {
	relFn := ComputeRelClosure(FindTestingStorageRootPath())
	return relFn(name...)
}

// @/production_storage/{name1/name2/...}
func ComputeThisProductionStorage(name ...string) string {
	relFn := ComputeRelClosure(FindProductionStorageRootPath())
	return relFn(name...)
}

// 고립환경 테스팅 시의 루트패시 전달
// 사용 시엔 FindTestingStorageRootPath+{테스트명}+{각 저장소}로 쓰면 됨
// @/testing_enviroment
func FindTestingStorageRootPath() string {
	return filepath.Join(FindProjectRootPath(), "testing_enviroment")
}

// @/production_storage
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
