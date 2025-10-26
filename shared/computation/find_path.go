package computation

import (
	"os"
	"path/filepath"
)

// func(...name)=> root/n1/n2
func ComputeRelClosure(root string) func(...string) string {
	return func(subpaths ...string) string {
		return filepath.Join(append([]string{root}, subpaths...)...)
	}
}

// @/testing_enviroment/{name1/name2/...}
func ComputeThisTestingDocker(name ...string) string {
	return ComputeRelClosure(DockerTestingStorage())(name...)
}

// @/production_storage/{name1/name2/...}
func ComputeThisProductionDocker(name ...string) string {
	relFn := ComputeRelClosure(DockerProductionStorage())
	return relFn(name...)
}

type StorageName string

const (
	PRODUCTION_STORAG StorageName = "production_storage"
	TESTING_STORAGE   StorageName = "testing_enviroment"
	WEB_STORAGE       StorageName = "web"
)

// 고립환경 테스팅 시의 루트패시 전달
// 사용 시엔 DockerTestingStorage+{테스트명}+{각 저장소}로 쓰면 됨
// @/testing_enviroment
func DockerTestingStorage() string {
	return DockerRootResolver(TESTING_STORAGE)
}
func DockerWebStroage() string {
	return DockerRootResolver(WEB_STORAGE)
}

// @/production_storage
func DockerProductionStorage() string {
	return DockerRootResolver(PRODUCTION_STORAG)
}

func DockerRootResolver(name StorageName) string {
	return filepath.Join(DockerRoot(), string(name))
}

// 프로젝트 루트 경로 리턴
func DockerRoot() string {
	return GoModWD()
}
func LocalRoot() string {
	return GoModWD()
}

func GetENV() string {
	return filepath.Join(GoModWD(), ".env")
}
func GoModWD() string {
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
	panic("go.mod파일을 찾지 못함")
}
