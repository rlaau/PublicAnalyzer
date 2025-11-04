package iface

import (
	ldomain "github.com/rlaaudgjs5638/chainAnalyzer/internal/dxan/rel/triplet/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/domain"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/workflow/workerpool"
)

//i_model.go: 인프라가 지녀야 할 기능들 모델링함.
//아무래도 핵심 기능보단 그를 위한 부가적 이펙트 관리에 집중된 모델링이다 보니
// 중요도는 a_model.go나 d_model.go보단 낮음.
// 이 모델링 부분은 비개발자 용이기보단, 개발자 용임.
// a_model.go나 d_model.go는 비개발자 팀원들도 읽을 수 있도록 적어둔 데 반해
// i_model.go는 app에 대한 의존성 주입+ 주요 이펙트 관리 기능들을 기술하는데 초점을 맞췄음.

// TripletInfraPort는 Triplet이 지니는 인프라를 정의함
type TripletInfraPort interface {
	GetTimeBucketManager() TimeBuckerManagerPort
	GetWorkerPool() *workerpool.Pool
}

// TimeBuckerManagerPort는 타임버킷매니져가 지니는 기능임.
// 타임버킷 매니져는 Triplet이 "버퍼링"해 두어야 할 값들을 인덱싱하고, 버퍼링-윈도윙 함.
// Load,Close를 통해 프로세스 종료-재시작에 대비함.
type TimeBuckerManagerPort interface {
	AddIfAbsent(to domain.Address, fromScala ldomain.FromScala) (bool, int)
	GetFromScalas(toAddr domain.Address) ldomain.FromScalas
	Load() error
	Close() error
}
