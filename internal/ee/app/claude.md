## 그래프 모듈 디자인
### 개요
1. 해당 모듈은 MarkedTransaction을 받아서 실시간으로 그래프DB를 업데이트 하는 모듈이야
2. 해당 모듈은 (GroundKnowledge= cexSet,detectedDepositSet,detectedDepositDB),(DualManager=(firstActiveTimeTimeBuckets)&(key value DB)), (GraphDB=BedgerDB등 kvDB)로 구성돼.
3. GroundKnowlege는 알려진 사실이야. 사전 수집한 cex set, 밝혀낸 deposit set과 그 db혹은 set임.
4. deposit set은 "입금 계좌"로써, "from-> to" 트랜잭션에서 to가 howWalletAddress일 경우, 해당 from 주소가 deposit주소임을 탐지하지.
5. DualManager는 "from,to"관계로부터 두 쌍의 관계를 관리해. 주로 "to:[]from"으로 관계가 매핑됨. 이후 "someDepositAddress:[]multi-user"같은 듀얼이 탐지되면, 이걸 그래프 DB에 반영하는 거지
6. 그래프DB는 "from,to, vertexInfo, edgeInfo"로 구성됨. 사실 kvDB임.

### 플로우 with 함수
1. checkTransaction(tx domain.transaction)
tx가 들어오면 우선 GroundKnowledge를 통해 해당 tx의 from, to에 cex나 detectedDeposit이 있는지 검사해.
chekcTransaction()->handleAddress()
2. handleAddress()
tx의 from이 cex면 to는 "txFromCexAddress"로 특수 처리해서 handleExceptionalAddress(address), to가 cex면 handleDetectionDeposit(cex, detectedDepositAdddress) 절차를 진행해. from이 detectedDepositAddress면 to는 "txFromDepositAddress"를 handleExceptionalAddress(address). to가 detectedDepositAddress시 from,to의 듀얼을 그래프DB에 저장해.
handleExceptionalAddress(address)는 추후 구현할 것임.
3. handleDetectionDeposit(cex, detectedDepositAddress)
to가 cex인 경우 시스템은 "새로운 입금주소"를 발견한 것으로 여겨. 
from은 detectedDepositAddress에 추가 하고,
이후, DualManager를 통해서 현재 듀얼매니져의 kvDB에서 to가 depositAddress인 []from값을 k,v로 가져와서 이를 [](to,from)으로 분해해서 그래프DB에 저장해.
4. 만약 그냥 tx일 경우 이는 DualManager에 들어가.(듀얼매니져의 윈도우 버퍼에 인서트.)
5. 듀얼 메니져의 윈도우 버퍼 고나리 로직.
DualManger는 우선 firstActiveTimeTimeBuckets를 업데이트. 각 버킷은 "4개원의 윈도우 크기 중 특정 타임버킷 기간에 처음 윈도우에 진입한 'to유저' 집합"을 관리해. 중요한 것은, "한 번 윈도우에 들어온 to user의 값은 갱신하지 않는다"야. 그러니까, 자신이 점차 늙어가긴 하지만, 일단 map에 있으면 "갱신하지 않음"(일종의 도메인 로직이야. 윈도우에 들어와서도 4개월 간 선택받지 못하면 그냥 떨어져 나가는거지. 이건 중요한 도메인 로직이니까, 특별한 주석으로 이 알고리즘을 강조해줘. 또한, 이러한 "에이징"의 대상은 "to user"야. from hotWallet에서 to Deposit address를 탐지하기 위해 이 에이징을 거는 거거든)이고, 윈도우 맵에 없는 경우에만 "타임 버킷에 값을 추가"하지. 이 모든 "활성 체크"의 대상은 "to address"야.->이를 통해 지속적으로 윈도우 내의 첫 거래가 가장 오래된 유저를 뽑아낼 수 있는거야. 그리고 해당 from,to를 DualManager의 kvDB에 저장해.-> 이떄, 윈도우의 크기는 4개월이고, 트리거 조건은 1주일, 슬라이드도 1주일 씩 슬라이드해. 그러려면 타임버킷은 21개 정도가 필요하겠지.
-> 타임버킷의 "타임"은 tx에 기록된 time을 기준으로 자동으로 1주일씩 끊어내면 될 거야. 그러다가 버킷 개수가 22개가 될 때, 맨 과거의 버킷을 가져와서 해당 버킷에 기록된 to유저들을 검사해. 이후 그 버킷을 버림과 동시에, 그 버킷에 기록되어 있던 to유저를 키로 하는 kv값들 역시 kvDB에서 제거해.
