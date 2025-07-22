### 기능
1. TxIngester는 tx의 추출 및 마킹을 담당함.
2. 마킹이란, 해당 tx의 "from", "to"가 EOA to EOA인지, EOA to Contract인지, Contract to EOA인지, Contract to Contract인지 확인해서 domain.Rawtransaction을 domain.MarkedTransaction으로 바꾸는 것임.
3. 이때, 추출 및 마킹 과정에서 TxIngester는 Contract의 정보를 관리하는 CCE모듈과 메서드 콜로 통힌함. CCE모듈은 컨트렉트 개별에 대한 정보를 담고 있음
4. TxIngester는 IngestTransaction과정에서 
-1. 해당 트랜잭션이 "컨트렉트 생성"임을 감지한 경우 그 만든이를 찾아서 "만든이", "컨트렉트"로 MarkedTransaction을 변환함. 또한 이 생성 정보를 CCE모듈에 동기적 메서드 콜로 전달하면, CCE모듈은 해당 정보를 저장함.
5. CCE모듈은 해당 컨트렉트 생성 정보를 받아서 자체DB에 insert하고, TxIngester로부터 "CheckIsContract"요청이 들어온 경우 true,false를 반환함.
6. 해당 과정에서 CCE모듈은 "컨트렉트에 대한 블룸필터"구조체를 유지함으로써 빠른 응답을 시도함