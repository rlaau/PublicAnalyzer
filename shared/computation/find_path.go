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

// // @/testing_enviroment/{name1/name2/...}
// func ComputeThisTestingDocker(name ...string) string {
// 	return ComputeRelClosure(DockerTestingStorage())(name...)
// }

// // @/production_storage/{name1/name2/...}
// func ComputeThisProductionDocker(name ...string) string {
// 	relFn := ComputeRelClosure(DockerProductionStorage())
// 	return relFn(name...)
// }

// root/(test| prod)/processName/ paths...
func GetStoragePath(paths ...string) string {
	root := GoModWD() // or storage base root resolver
	storage := DATA_STORAGE
	mode := os.Getenv(string(PROCESS_MODE_KEY))
	if mode == "" {
		panic("PROCESS_MODE 환경변수가 설정되지 않았습니다.")
	}
	var modeStr string
	if mode == string(ProductionMode) {
		modeStr = "prod"
	}
	if mode == string(TestingMode) {
		modeStr = "test"
	}
	if mode != string(ProductionMode) && mode != string(TestingMode) {
		panic("GetStoragePath 오류 발생. 현재 모드가 prod도 아니도 test도 아님")
	}
	name := os.Getenv(string(PROCESS_NAME_KEY))

	if name == "" {
		panic("PROCESS_NAME 환경변수가 설정되지 않았습니다.")
	}

	// prefix 3개(root, mode, name) + variadic paths 를 하나의 슬라이스로 합치기
	parts := make([]string, 0, 4+len(paths))
	parts = append(parts, root, string(storage), modeStr, name)
	parts = append(parts, paths...) // 이제 ... 사용 가능

	return filepath.Join(parts...)
}

func GetWebPath(paths ...string) string {
	root := GoModWD()
	web := WEB_STORAGE
	parts := make([]string, 0, 2+len(paths))
	parts = append(parts, root, string(web))
	parts = append(parts, paths...)
	return filepath.Join(parts...)
}

type StorageName string

const (
	DATA_STORAGE StorageName = "storage"
	WEB_STORAGE  StorageName = "web"
)

// // 고립환경 테스팅 시의 루트패시 전달
// // 사용 시엔 DockerTestingStorage+{테스트명}+{각 저장소}로 쓰면 됨
// // @/testing_enviroment
// func DockerTestingStorage() string {
// 	return DockerRootResolver(TESTING_STORAGE)
// }
// func DockerWebStroage() string {
// 	return DockerRootResolver(WEB_STORAGE)
// }

// // @/production_storage
// func DockerProductionStorage() string {
// 	return DockerRootResolver(PRODUCTION_STORAG)
// }

// func DockerRootResolver(name StorageName) string {
// 	return filepath.Join(DockerRoot(), string(name))
// }

// // 프로젝트 루트 경로 리턴
// func DockerRoot() string {
// 	return GoModWD()
// }
// func LocalRoot() string {
// 	return GoModWD()
// }

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
