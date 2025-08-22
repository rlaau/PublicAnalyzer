### 공통
	txFeederGenConfig := &feederdomain.TxGeneratorConfig{
		TotalTransactions:            2_000_000,
		TransactionsPerSecond:        50_000,
		StartTime:                    startTime,
		TransactionsPerTimeIncrement: 1,               // 하나의 tx마다 10분이 지난 것으로 설정 (순환 테스트 가속화)
		TimeIncrementDuration:        2 * time.Minute, // 10분씩 시간 증가
		DepositToCexRatio:            50,              // 1/50 비율로 CEX 주소 사용
		RandomToDepositRatio:         30,              // 1/15 비율로 Deposit 주소 사용
	}

### 기존 그래프DB
1. 총 그래프. 96그래프/ 46엣지
2. 총 런타임. 60초
3. 총 성공: 110_405 (총 전달: 2000000 )

### RopeDB
1. 총 그래프. 미정
2. 총 런타임. 60초
3. 총 성공.140_355
