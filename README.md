## 기술적 방향성
## 아키텍쳐 배경

### 아키텍쳐 레퍼런스

1. 시스템 전체 구성 1:
모듈러-모노리식 아키텍쳐
https://ijung.github.io/categories/modular-monolith/
2. 시스템 전체 구성 2:
스트리밍 데이터 파이프라인 아키텍쳐
https://product.kyobobook.co.kr/detail/S000200716128
3. 모듈 내부 구성:
도메인 주도 설계
[https://ijung.github.io/categories/도메인-주도-설계-첫걸음-done/](https://ijung.github.io/categories/%EB%8F%84%EB%A9%94%EC%9D%B8-%EC%A3%BC%EB%8F%84-%EC%84%A4%EA%B3%84-%EC%B2%AB%EA%B1%B8%EC%9D%8C-done/)
4. 모듈 외피 구성:
헥사고날 아키텍쳐
[https://ijung.github.io/categories/헥사고날-아키텍처/](https://ijung.github.io/categories/%ED%97%A5%EC%82%AC%EA%B3%A0%EB%82%A0-%EC%95%84%ED%82%A4%ED%85%8D%EC%B2%98/)
(단, Go의 철학 상 모듈마다 명시적인 포트를 달지는 않았음. 소비하는 측에서 그때그때 xface만들어서 소비하는 양태임)
5. 모듈 연결 구성:
이벤트 주도 아키텍쳐
[https://ijung.github.io/categories/이벤트-드리븐-아키텍처/](https://ijung.github.io/categories/%EC%9D%B4%EB%B2%A4%ED%8A%B8-%EB%93%9C%EB%A6%AC%EB%B8%90-%EC%95%84%ED%82%A4%ED%85%8D%EC%B2%98/)

### Why MMA

1. 생산성 & 단순성 : MSA대비 단순하고, 그렇기에 생산성 높고, 배포가 쉬움.
2. 준수한 확장성 : 잘만 격리하면 원하는 기능은 다 넣을 수 있음
3. 성능: 내부 네트워크 지연이 사라짐.

## 프로젝트에 반영된 아키텍쳐

### 폴더

1. internal:
수직적 모듈들을 담은 폴더
(수직적 모듈= 비즈니스 도메인의 일정 부분을 담당)
2. shared:
수평적 모듈들을 담은 폴더
(수평적 모듈= 공통적으로 사용하는 저수준로직 담당. 보안, 로깅, 모니터링 등)
3. cmd:
컴포지션 루트 담은 폴더
4. server/web:
백엔드 기능 담은 폴더
5. storage/ enviroment:
DB 담은 폴더

### 모듈 내 구조

1. domain:
모듈이 관리하는 엔티티, 벨류, 이벤트, 어그리게이트 정의
2. iface:
infra, app 에 대한 의존성 격리 용도
3. infra:
app이 사용할 인프라 마련.
4. app:
비즈니스 로직 담당

## 모듈 설명 (제안 단계)

### internal

1. DexFeeder:
(dexSoruce)→{dexFeeder}→(dexAN)
dex의 raw 지표 받아오기 담당
2. DxAn:
(dexFeeder)→{dxAn}→(interpreter)
raw dex 지표를 유의미한 dex metric으로 변환.
분석 과정에서 다수의 sub Module이용해서 고차원적인 metric을 추출해 냄.
    1. ”어떤 Dex지표”를 분석할 지를 “세팅”으로 받아서 서브모듈로 구현
    (임의 세팅을 구현하는 테스크루프도 보조로 추가 가능)
    2. 그 세팅에 따라 ‘실시간 Dex데이터’를 받은 후 가공해서 interpreter에 전달
3. CexFeeder(미구현):
(cexSource)→{cexFeeder}→(cxAn)
cex의 raw지표 받아오기 담당.
4. CxAn(미구현):
(cexFeeder)→{cxAn}→(interpreter)
raw cex 지표를 유의미한 cex metric으로 변환
분석 과정에서 다수의 sub Module이용해서 고차원적인 metric을 추출해 냄.
    1. “어떤  Cex지표”를 분석할 지를 “세팅”으로 받아서 서브 모듈로 구현
    (임의 세팅을 구현하는 테스크루프도 보조로 추가 가능)
    2. 그 세팅에 따라 ‘실시간 Cex데이터’를 받은 후 가공해서 interpreter에 전달
5. Interpreter(미구현):
(dxAn, cxAn) → {interpreter} → (actor)
지표를 해석해서 명령어로 변환하는 모듈
    1. dxAn, cxAn이 보내준 지표에 “반응”할 “스크립트(=사용자 입력 코드)”를 저장.
    (같은 지표를 받고도, 개별 스크립트 따라 개별적으로 인터프리터실행)
    2. 최종적으로 
        1. 코드= 정적인 사용자 스크립트
        2. 레코드 데이터 = 사용자 포트폴리오
        3. 값 입력 = dxAn, cxAn의 지표
        
        을 입력으로 삼아서 (인터프리터의 코드 & 환경)
        
        1. 오퍼랜드 = 어떤 것을 대상으로 (대상 사용자, 토큰, 거래소)
        2. 오피코드 = 무엇을 할지 (매수, 매도, 대기)
        
        를 도출함. (명령어는 오퍼랜드, 오피코드로 구성)
        
    3. 별개로 이 경우 언어의 일급 대상을 잘 정하기.
        1. 여기서부턴 느낌은 오는데 정확히 표현이 힘듦.
        2. 지표값, 유저 포트폴리오 등을 일급 대상 & base 연산자에 포함시키자는 것임.
        (js가 언어 차원에서 구조적 공유를 지원하듯, 이것도 언어적 차원에서 스프레드 차익이나 이자율 반영한 가치 계산 필요)
        3. 런타임도 잘 설계 필요함.
        4. 개인적으론 ai모델 자체를 일급 대상으로 두는 것도 생각 중.
        (ex: m = NewModel(”buy and pray”); m.Start() 등)
6. Actor(미구현):
(interpreter)→ {actor}→(api call)
    1. 인터프리터가 제공한 “선언적 명령어”를 절차적으로 수행함.
    2. 실시간 평가하며 불가능한 거래에 대한 에러 처리, 로깅, 모니터링하기.
    3. 해당 모듈에선 성능이나 고전적 에러 처리 등이 중요할듯.

### shared

1. gface/ workflow:
각각 모듈에 대한 고차 인터페이스, 동작에 대한 고차함수 제공.
특정 컨벤션 강제에 쓸 수 있음.
2. domain:
공통으로 쓰이는 도메인 정의
3. meter:
미터 기반 계측을 함.
각 미터는 프로메테우스를 적절히 래핑해서 성능 조절.
4. chaintimer:
체인 이벤트 시간을 저수준 레벨에서 컨트롤함.
cell의 작동 방식 모방.
5. dblib:
각종 재사용 가능한, 혹은 기초가 되는 DB 구현함.
6. kafka/ eventub:
이벤트 통신 수단.

## 컴포지션 루트
1. cmd/streamable.go