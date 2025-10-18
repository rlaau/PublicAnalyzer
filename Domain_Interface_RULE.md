# 인터페이스 & 도메인 규칙
## 인터페이스 규칙- (G/IXD-PC)
### 모듈 간 공유 인터페이스(G 인터페이스)
1. gface (ground interface)
다른 인터페이스의 인터페이스가 되는 추상 인터페이스.
예컨데, 모듈 내 모든 모듈 인터페이스가 지녀야 할 표쥰 규격을 정의하는 것임.
그러니까, 모듈의 gface는 Start(ctx) error, Close()error 등이 존재
ex:
```go
    //gface에 정의된 인터페이스는
    //시스템 모듈 전체의 인터페이스 정의함 
    type GroundModule interface {
        Start(ctx context.Context) error
        Close()error
    }
```
굳이 왜 이러냐 싶지만, "스트리밍 시스템"에선 "모듈의 제어"가 워낙 중요해지기 때문.
다 겪어보고 하는 말임. 이런것도 통일하지 못하면 컴포지션 루트가 더러워지고, 에러도 많이 남.
### 모듈 내의 인터페이스 with IXD 인테페이스
1. iface (infra interface)
자기 인프라의 인터페이스 정의
2. xface (external module interface)
외부 모듈의 인터페이스 정의
소비자 시점에서 정의한 외부 모듈의 인터페이스
(eface시 go의 런타임 인터페이스인 eface와 출돌 위험.)
3. dface (design interface)
자신의 인터페이스 정의.
이는 다른 모듈에서 임포트하지 않음.
순수하게 "문서화"와 "디자인"을 위한 인터페이스.
### 부모-자식 관계를 위한 PC-IED 인터페이스
상술한 face만으론 "부모-자식"이 복잡하게 얽힌 모듈을 정의하기 어려움
(실은, 애초에 이런 부모-자식으로 얽힌 모듈은 하나의 모듈로 만드는게 맞음.)
(그러나 하나로 합치면 부피가 커지고, 여럿으로 쪼개면 통신 비용이 막대해짐. 결국 이런 괴상한 모듈이 만들어 진 것임)
때문에 아래와 같은 시도를 했음.

1. xface (external module interface)
xface는 남발할 경우 인터페이스 중첩이 일어나므로, 주요 계약에만 정의.
또한, 외부 모듈의 dface임포트 금지.
부모-자식 모듈에선 "최상위 모듈"에서만 eface정의
ex: 
```go
    type ExternalModulePort interface {
        FetchSomething()
    }
```

2. pface (parent interface)
자기가 가져오고픈 부모의 기능, 즉 자신이 본 부모의 포트를 정의

ex:
```go
    type MyParentPort interface {
        RegisterChild()
    }
```

3. cface (children interface)
자기가 보고픈 자식의 기능, 즉, 자신이 본 자식의 포트를 정의

ex:
```go 
    type MyChildernPort interface {
        RegisterParent()
    }
```

4. iface (infra interface)
자신의 "인프라"에 대한 인터페이스를 정의
이때, iface는 "내부 전용"임. 외부에 노출하지 않음.
인프라에 대한 핸들링은 자신 모듈의 메서드콜이나 브로커를 통해서 하도록 함.
ex:
```go
    type MyInfraInterface interface {
        HandleMyInfra()
    }
```
5. dface (design interface)
순수 문서화를 위한 인터페이스.
"자기 자신의 설계도"를 그리는 인터페이스임.
다른 모듈은 이 인터페이스를 임포트 해선 안됨.
"코드로 하는 문서화"라고 보면 됨.
ex:
```go
    type XxxDesign interface {
        DomSometing()
    }
```
으로 표기함.

## 도메인 규칙 (shared/internal)
### 공통 도메인
1. shared domain
모듈 간에 공유하는 도메인은 shared domain에 정의
ex:
``` go
    type TxEvent struct {
        Message string
    }
```
### 모듈 내부 도메인
1. domain
모듈 내 도메인은 domain에 정의
ex:
```go
    type SpecialTxEvent struct {
        Message string
        Signal int64
    }
```