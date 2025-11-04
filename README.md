## 아키텍쳐 배경

### 아키텍쳐 레퍼런스

1. 시스템 전체 구성 1:
모듈러-모노리식 아키텍쳐
https://ijung.github.io/categories/modular-monolith/
[프로젝트에선 폴더 별 모듈 분리로 관리]
2. 시스템 전체 구성 2:
스트리밍 데이터 파이프라인 아키텍쳐
https://product.kyobobook.co.kr/detail/S000200716128
[프로젝트에선 모듈별 gface.Module 강제를 통해서 스트리밍의 시작, 끝, 에러 처리를 제어함]
3. 시스템 전체 구성 3:
반응형 아키텍쳐
https://kooku0.github.io/docs/books/grokking-simplicity/18/
(함께 서술된 어니언 아키텍쳐는 쓰지 않음. 도메인 주도 설계와 겹치고, 도메인 주도 설계가 어니언 아키텍쳐보다 더 깔끔하다 생각함.)
[시스템에선, <모듈 간 이벤트 브로킹= 카프카 혹은 RabbitMQ>, <모듈 내 이벤트브로킹= 인메모리 자체 이벤트 자료형>으로 처리할 예정]
(모듈 내에선 되도록 자체 자료형 쓰는게 테스트에 훨씬 유리하며, 성능 오버헤드도 적다고 판단함.)
4. 모듈 내부 구성:
도메인 주도 설계
[https://ijung.github.io/categories/도메인-주도-설계-첫걸음-done/](https://ijung.github.io/categories/%EB%8F%84%EB%A9%94%EC%9D%B8-%EC%A3%BC%EB%8F%84-%EC%84%A4%EA%B3%84-%EC%B2%AB%EA%B1%B8%EC%9D%8C-done/)
도메인을 잘 모델링하는 법
[https://www.youtube.com/watch?v=PLFl95c-IiU]
[프로젝트에선 모듈 내 폴더 분리로 관리함]
5. 모듈 외피 구성:
헥사고날 아키텍쳐
[https://ijung.github.io/categories/헥사고날-아키텍처/](https://ijung.github.io/categories/%ED%97%A5%EC%82%AC%EA%B3%A0%EB%82%A0-%EC%95%84%ED%82%A4%ED%85%8D%EC%B2%98/)
(단, Go의 철학 상 모듈마다 명시적인 포트를 달지는 않았음. 소비하는 측에서 그때그때 xface만들어서 소비하는 양태임)
[프로젝트에선 xface로 관리함]

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
모델 디자인
1. domain/d_model.go:
모듈이 지녀야 할 도메인 로직을 모델링함.
2. app/a_model.go:
모듈이 지녀야 할 비즈니스 로직을 모델링함.
3. iface/i_model.go
모듈이 의존하는 인프라를 모델링함.

폴더 디자인
1. domain:
모듈이 관리하는 엔티티, 벨류, 이벤트, 어그리게이트 정의.
d_model.go에 도메인의 핵심 로직 모델링함.
2. iface:
infra, app 에 대한 의존성 격리 용도.
3. infra:
app이 사용할 인프라 마련.
iface의 모델링을 바탕으로 구현.
4. app:
비즈니스 로직 담당.
외부 포트 및 a_model.go를 바탕으로 구현.

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
        5. 또한, 
        내부적으론 TSDB를 둬서 시계열을 관리하고, 
        이로부터 “시계열 기반 구조”를 “일급 구조”로 만들고 싶음.
        6. ex: type Delta struct 
        {timeWindow time.Second, 
        metricKind      MetricKind, 
        ptr                   *[]MetricData}
        를 go의 slice처럼 원시 타입으로 두고,
        %%로 생성 가능하게 한다 치면,
        valanceDelta :=  %(valanceMetric)%로 정의 시, 
        print(valanceDelta)를 하면, 프린트 할 때마다 1초간 변화량 나오는 등, 시계열 분석을 자동으로 해주게 하기. 
        혹은 미분 연산을 추가해도 좋고.
        7. 개인적으론 시계열이나 미분에 대해서, 언어가 다루는 모든 primitive value 타입들 (valnce, token 등등)이 특정 ‘미분 가능한’ 인터페이스를 충족하게 해서, 언어의 “일급 연산”만으로 이들을 쉽게 구해내게 하고싶음.
        ex) 위의 Delta 타입이 
        type Delatable interface {
        epsilon() time.Microsecond
        Divive() int
        Window () []int
        }
        등을 충족하게 해서
        ~~등이 window연산에 대응된다고 하면
        for ~~valanceDelta range := valanceDelta 등으로 매번 델타값의 윈도우 순회 가능케 하기 등 가능하게.
        
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