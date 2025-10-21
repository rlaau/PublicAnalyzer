package main

// // 5분 윈도우 TPS 기록
// type WindowTPSRecord struct {
// 	WindowStart time.Time
// 	WindowEnd   time.Time
// 	EventCount  int64
// 	TPS         float64
// }

// // 통계 추적을 위한 구조체
// type IngestionStats struct {
// 	totalContracts     int64
// 	processedContracts int64
// 	publishedContracts int64
// 	convertedEvents    int64
// 	startTime          time.Time

// 	// 타이밍 윈도우를 위한 필드
// 	windowStart     time.Time
// 	windowProcessed int64
// 	currentTPS      float64

// 	// 5분 윈도우 TPS 히스토리 (시간 순서로 저장)
// 	tpsHistory []WindowTPSRecord

// 	// 마지막 10개 처리된 컨트랙트 (순환 버퍼)
// 	lastContracts [10]shareddomain.Contract
// 	contractIndex int
// 	contractCount int64

// 	mu sync.RWMutex
// }

// func main() {
// 	log.Println("🚀 Starting Contract Creator/Created Ingestion...")

// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer cancel()

// 	// 2. RelPool 생성 (RopeDB 및 Triplet 포함)
// 	relPool, co, err := GetRelPoolAndCo()
// 	// 1. Contract DB 초기화
// 	contractDB := co.GetContDB()
// 	defer contractDB.Close()
// 	if err != nil {
// 		log.Fatalf("❌ Failed to create RelPool: %v", err)
// 	}

// 	// 3. 모니터링 서버 설정 (포트 8080)
// 	monitoringServer := server.NewServer(":8080")
// 	RegisterModule(monitoringServer, relPool)

// 	// 4. Triplet에서 RopeDB 가져오기
// 	triplet := relPool.GetTripletPort()
// 	// GetRopeDB가 없다면 직접 TraitEvent를 처리할 수 있는 버스나 메서드 사용
// 	// 여기서는 triplet이 내부적으로 RopeDB를 관리한다고 가정

// 	// 5. Contract Bridge 생성 (길이 300,000의 인메모리 이벤트버스)
// 	contBridge := eventbus.NewInMemory[shareddomain.Contract](300_000)
// 	defer contBridge.Close()

// 	// 6. 통계 초기화
// 	stats := &IngestionStats{
// 		startTime:     time.Now(),
// 		windowStart:   time.Now(),
// 		tpsHistory:    make([]WindowTPSRecord, 0),
// 		contractIndex: 0,
// 		contractCount: 0,
// 	}

// 	// 7. WaitGroup for goroutines
// 	var wg sync.WaitGroup

// 	// 8. Contract 총 개수 파악
// 	totalCount, err := countTotalContracts(contractDB)
// 	if err != nil {
// 		log.Printf("⚠️ Could not count total contracts: %v", err)
// 		totalCount = -1 // Unknown
// 	}
// 	stats.totalContracts = totalCount
// 	log.Printf("📊 Total contracts in DB: %d", totalCount)

// 	// 9. 모니터링 서버 시작
// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		fmt.Printf("   🌐 Starting API server on :8080\n")
// 		if err := monitoringServer.Start(); err != nil {
// 			fmt.Printf("   ⚠️ API server stopped: %v\n", err)
// 		}
// 	}()
// 	printServerInfo()

// 	// 10. Triplet 시작
// 	wg.Add(1)
// 	tripletDone := make(chan error, 1)
// 	go func() {
// 		defer wg.Done()
// 		tripletDone <- triplet.Start(ctx)
// 	}()
// 	fmt.Printf("   🔄 Triplet started\n")

// 	// 11. Contract DB에서 배치로 읽어서 Bridge에 발행하는 고루틴
// 	iterationDone := make(chan struct{})
// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		defer close(iterationDone)

// 		batchSize := 50_000
// 		log.Printf("📦 Starting contract iteration with batch size: %d", batchSize)

// 		err := contractDB.IterateContractsBatchedNonBlocking(batchSize, func(batch []shareddomain.Contract) bool {
// 			// Context 체크
// 			select {
// 			case <-ctx.Done():
// 				log.Println("🛑 Contract iteration stopped by context")
// 				return false
// 			default:
// 			}

// 			// 배치를 Bridge에 발행
// 			for _, contract := range batch {
// 				select {
// 				case <-ctx.Done():
// 					return false
// 				default:
// 					// Bridge가 꽉 차면 대기 (블로킹)
// 					if err := contBridge.Publish(contract); err != nil {
// 						log.Printf("⚠️ Failed to publish contract: %v", err)
// 						continue
// 					}
// 					atomic.AddInt64(&stats.publishedContracts, 1)
// 				}
// 			}

// 			atomic.AddInt64(&stats.processedContracts, int64(len(batch)))
// 			return true // 계속 진행
// 		})

// 		if err != nil {
// 			log.Printf("❌ Contract iteration error: %v", err)
// 		} else {
// 			log.Println("✅ Contract iteration completed")
// 		}
// 	}()

// 	// 12. Bridge에서 Contract를 받아 TraitEvent로 변환하고 Triplet에 전달하는 고루틴
// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()

// 		bridgeChannel := contBridge.Dequeue()
// 		log.Println("🔄 Contract to TraitEvent converter started")

// 		for {
// 			select {
// 			case <-ctx.Done():
// 				log.Println("🛑 Converter stopped by context")
// 				return

// 			case contract, ok := <-bridgeChannel:
// 				if !ok {
// 					log.Println("✅ Contract bridge closed, converter stopping")
// 					return
// 				}

// 				// 마지막 10개 컨트랙트 기록 (순환 버퍼)
// 				stats.mu.Lock()
// 				stats.lastContracts[stats.contractIndex] = contract
// 				stats.contractIndex = (stats.contractIndex + 1) % 10
// 				stats.contractCount++
// 				stats.mu.Unlock()

// 				// Contract를 TraitEvent로 변환 (더미 데이터)
// 				traitEvent, isOk := TransfromContractToTraitEvent(contract)
// 				if !isOk {
// 					//생성자 없을 시 이번 컨트렉트는 건너뛰기
// 					atomic.AddInt64(&stats.convertedEvents, 1)

// 					continue
// 				}
// 				//해당 생성 이벤트 전달
// 				relPool.ClusterRepo.HandleTraitEvent(traitEvent)

// 				atomic.AddInt64(&stats.convertedEvents, 1)
// 			}
// 		}
// 	}()

// 	// 13. 20초마다 모니터링 출력하는 고루틴
// 	wg.Add(1)
// 	monitorTicker := time.NewTicker(20 * time.Second)
// 	defer monitorTicker.Stop()

// 	go func() {
// 		defer wg.Done()

// 		// 5분 타이밍 윈도우
// 		windowTicker := time.NewTicker(5 * time.Minute)
// 		defer windowTicker.Stop()

// 		for {
// 			select {
// 			case <-ctx.Done():
// 				return

// 			case <-monitorTicker.C:
// 				printMonitoringStats(stats, contBridge, triplet)

// 			case <-windowTicker.C:
// 				// 타이밍 윈도우 업데이트 및 히스토리 저장
// 				stats.mu.Lock()

// 				windowEnd := time.Now()
// 				elapsed := windowEnd.Sub(stats.windowStart).Seconds()
// 				currentEvents := atomic.LoadInt64(&stats.convertedEvents)
// 				windowEvents := currentEvents - stats.windowProcessed

// 				var windowTPS float64
// 				if elapsed > 0 {
// 					windowTPS = float64(windowEvents) / elapsed
// 					stats.currentTPS = windowTPS
// 				}

// 				// 5분 윈도우 기록을 히스토리에 추가
// 				record := WindowTPSRecord{
// 					WindowStart: stats.windowStart,
// 					WindowEnd:   windowEnd,
// 					EventCount:  windowEvents,
// 					TPS:         windowTPS,
// 				}
// 				stats.tpsHistory = append(stats.tpsHistory, record)

// 				// 새 윈도우 시작
// 				stats.windowStart = windowEnd
// 				stats.windowProcessed = currentEvents

// 				// TPS 히스토리와 마지막 컨트랙트들 출력
// 				printTPSHistory(stats)
// 				printLastContracts(stats)

// 				stats.mu.Unlock()
// 			}
// 		}
// 	}()

// 	// 14. 시그널 처리 및 종료 대기
// 	sigChan := make(chan os.Signal, 1)
// 	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

// 	log.Println("🎯 System running. Press Ctrl+C to stop...")

// 	// 메인 이벤트 루프
// 	select {
// 	case <-iterationDone:
// 		// 모든 컨트랙트 처리 완료
// 		log.Println("✅ All contracts processed from DB")
// 		log.Println("⏱️ Waiting 30 seconds for remaining events to process...")

// 		// 30초 대기
// 		shutdownTimer := time.NewTimer(30 * time.Second)
// 		defer shutdownTimer.Stop()

// 		select {
// 		case <-shutdownTimer.C:
// 			log.Println("⏰ 30 seconds elapsed - initiating graceful shutdown")
// 		case <-sigChan:
// 			log.Println("🛑 Manual shutdown signal received")
// 		}

// 	case <-sigChan:
// 		log.Println("🛑 Shutdown signal received")

// 	case err := <-tripletDone:
// 		if err != nil {
// 			log.Printf("❌ Triplet error: %v", err)
// 		} else {
// 			log.Println("✅ Triplet completed")
// 		}
// 	}

// 	// 15. Graceful Shutdown
// 	log.Println("🛑 Starting shutdown sequence...")

// 	// Context cancel로 모든 고루틴 정지 신호
// 	cancel()

// 	// Contract Bridge 닫기
// 	contBridge.Close()

// 	// Triplet 정지
// 	log.Println("🛑 Stopping Triplet...")
// 	if err := triplet.Stop(); err != nil {
// 		log.Printf("⚠️ Triplet stop error: %v", err)
// 	}

// 	// Triplet 종료 대기 (타임아웃 포함)
// 	select {
// 	case err := <-tripletDone:
// 		if err != nil {
// 			log.Printf("⚠️ Triplet completed with error: %v", err)
// 		} else {
// 			log.Println("✅ Triplet stopped successfully")
// 		}
// 	case <-time.After(30 * time.Second):
// 		log.Println("⏱️ Triplet stop timeout - forcing shutdown")
// 	}

// 	// 모든 고루틴 종료 대기
// 	log.Println("⏳ Waiting for all goroutines to complete...")
// 	done := make(chan struct{})
// 	go func() {
// 		wg.Wait()
// 		close(done)
// 	}()

// 	select {
// 	case <-done:
// 		log.Println("✅ All goroutines stopped")
// 	case <-time.After(10 * time.Second):
// 		log.Println("⚠️ Goroutine shutdown timeout - forcing exit")
// 	}

// 	// API 서버 종료
// 	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
// 	defer shutdownCancel()

// 	log.Printf("   🛑 Shutting down API server...")
// 	if err := monitoringServer.Shutdown(shutdownCtx); err != nil {
// 		log.Printf("   ⚠️ Server shutdown error: %v", err)
// 	} else {
// 		log.Printf("   ✅ API server shutdown completed")
// 	}

// 	// 최종 통계 출력
// 	printFinalStats(stats)

// 	// 프로세스 종료 시 마지막 컨트랙트들 출력
// 	log.Println("📋 Final processed contracts:")
// 	printLastContracts(stats)

// 	// Triplet 리소스 정리
// 	if triplet != nil {
// 		triplet.Close()
// 		log.Println("✅ Triplet resources cleaned up")
// 	}

// 	log.Println("✅ Shutdown completed successfully")

// 	// 최종 안전장치: 15초 후 강제 종료
// 	go func() {
// 		time.Sleep(30 * time.Second)
// 		log.Printf("   ⚠️ EMERGENCY: Force exit after 15s timeout")
// 		os.Exit(1)
// 	}()
// }

// // GetRelPoolAndCo creates and configures the RelationPool
// func GetRelPoolAndCo() (*relapp.RelationPool, cface.CoPort, error) {
// 	apool := &aapp.AnalyzerPool{}
// 	// nodPool을 강제로 생성
// 	nodPool := &nodapp.NodPool{}
// 	// CO생성 및 nodPool에 강제 등록
// 	co := contApp.NewCo(contApp.CoCfg{Mode: mode.ProductionModeProcess})
// 	eo := eoapp.NewEo(mode.ProductionModeProcess)
// 	nodPool.SetCoPort(co)
// 	nodPool.SetEoPort(eo)
// 	apool.RegisterPorts(nodPool, nil)
// 	relPool := relapp.ComposeRelPool(mode.ProductionModeProcess, "ingest_creator_created", apool)
// 	apool.RegisterPorts(nodPool, relPool)
// 	return relPool, co, nil
// }

// // RegisterModule registers the RelPool API with the monitoring server
// func RegisterModule(monitoringServer *server.Server, relPool *relapp.RelationPool) {
// 	monitoringServer.SetupBasicRoutes()
// 	relAPI := relapi.NewRelPoolAPIHandler(relPool)
// 	if err := monitoringServer.RegisterModule(relAPI); err != nil {
// 		fmt.Printf("   ❌ Failed to register Relation Pool API: %v\n", err)
// 	} else {
// 		fmt.Printf("   ✅ Relation Pool API registered successfully\n")
// 	}
// }

// // printServerInfo prints available API endpoints
// func printServerInfo() {
// 	fmt.Println("   📍 Available endpoints (Chi Router):")
// 	fmt.Println("   💡 API Endpoints (JSON responses):")
// 	fmt.Println("   - GET http://localhost:8080/api/health             - Server health")
// 	fmt.Println("   - GET http://localhost:8080/api/triplet/statistics      - Triplet statistics")
// 	fmt.Println("   - GET http://localhost:8080/api/triplet/health          - Triplet health")
// 	fmt.Println("   - GET http://localhost:8080/api/triplet/channel-status  - Triplet Channel status")
// 	fmt.Println("   - GET http://localhost:8080/api/triplet/dual-manager/window-stats - Window statistics")
// 	fmt.Println("   - GET http://localhost:8080/api/triplet/graph/stats     - Graph DB statistics")
// 	fmt.Println("   🌐 UI Endpoints (HTML pages):")
// 	fmt.Println("   - GET http://localhost:8080/ui/dashboard           - Main Dashboard")
// 	fmt.Println("   - GET http://localhost:8080/ui/triplet/            - Triplet Module Page")
// 	fmt.Println("   - GET http://localhost:8080/ui/cce/                - CCE Module Page")
// 	fmt.Println("   🔄 Legacy Redirects (for backward compatibility):")
// 	fmt.Println("   - GET http://localhost:8080/health → /api/health")
// 	fmt.Println("   - GET http://localhost:8080/triplet/* → /api/triplet/*")
// 	fmt.Println("   - GET http://localhost:8080/ → /ui/dashboard")
// }

// // Contract 총 개수 세기
// func countTotalContracts(contractDB *infra.ContractDB) (int64, error) {
// 	var count int64
// 	err := contractDB.IterateContracts(func(position int, contract shareddomain.Contract) bool {
// 		count++
// 		return true // 계속
// 	})
// 	return count, err
// }

// // 모니터링 통계 출력
// func printMonitoringStats(stats *IngestionStats, bridge *eventbus.EventBus[shareddomain.Contract], triplet rsface.TripletPort) {
// 	processed := atomic.LoadInt64(&stats.processedContracts)
// 	published := atomic.LoadInt64(&stats.publishedContracts)
// 	converted := atomic.LoadInt64(&stats.convertedEvents)
// 	total := atomic.LoadInt64(&stats.totalContracts)

// 	// Progress percentage
// 	var progress float64
// 	if total > 0 {
// 		progress = float64(processed) / float64(total) * 100
// 	}

// 	// Bridge status
// 	bridgeLen := bridge.Len()
// 	bridgeCap := bridge.Cap()
// 	bridgeUsage := float64(bridgeLen) / float64(bridgeCap) * 100

// 	// TPS calculation
// 	stats.mu.RLock()
// 	currentTPS := stats.currentTPS
// 	stats.mu.RUnlock()

// 	// Triplet/RopeDB stats
// 	tripletStats := triplet.GetRopeDBStats()

// 	// Elapsed time
// 	elapsed := time.Since(stats.startTime)

// 	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
// 	fmt.Printf("📊 MONITORING STATS [%s elapsed]\n", elapsed.Round(time.Second))
// 	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
// 	fmt.Printf("📦 Contracts: %d / %d processed (%.2f%%)\n", processed, total, progress)
// 	fmt.Printf("📤 Published to Bridge: %d contracts\n", published)
// 	fmt.Printf("🔄 Converted to TraitEvents: %d events\n", converted)
// 	fmt.Printf("🌉 Bridge Status: %d / %d (%.1f%% full)\n", bridgeLen, bridgeCap, bridgeUsage)
// 	fmt.Printf("⚡ Current TPS: %.2f events/sec\n", currentTPS)
// 	fmt.Printf("🔗 RopeDB Stats: nodes=%v, ropes=%v, traits=%v\n",
// 		tripletStats["nodes"], tripletStats["ropes"], tripletStats["traits"])
// 	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
// }

// // TPS 히스토리 출력 (5분 윈도우별)
// func printTPSHistory(stats *IngestionStats) {
// 	if len(stats.tpsHistory) == 0 {
// 		return
// 	}

// 	fmt.Println("\n⏰ ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
// 	fmt.Println("📈 5-MINUTE WINDOW TPS HISTORY")
// 	fmt.Println("⏰ ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

// 	// 최근 12개 윈도우만 출력 (최대 1시간)
// 	start := 0
// 	if len(stats.tpsHistory) > 24 {
// 		start = len(stats.tpsHistory) - 24
// 	}

// 	for i := start; i < len(stats.tpsHistory); i++ {
// 		record := stats.tpsHistory[i]
// 		windowNum := i + 1
// 		duration := record.WindowEnd.Sub(record.WindowStart)

// 		fmt.Printf("📊 Window #%d: %s - %s (%.1fmin) | Events: %d | TPS: %.2f\n",
// 			windowNum,
// 			record.WindowStart.Format("15:04:05"),
// 			record.WindowEnd.Format("15:04:05"),
// 			duration.Minutes(),
// 			record.EventCount,
// 			record.TPS)
// 	}

// 	// 추세 분석 (최근 3개 윈도우)
// 	if len(stats.tpsHistory) >= 7 {
// 		recent := stats.tpsHistory[len(stats.tpsHistory)-3:]

// 		trend := "stable"
// 		if recent[2].TPS > recent[1].TPS*1.1 && recent[1].TPS > recent[0].TPS*1.1 {
// 			trend = "increasing ↗️"
// 		} else if recent[2].TPS < recent[1].TPS*0.9 && recent[1].TPS < recent[0].TPS*0.9 {
// 			trend = "decreasing ↘️"
// 		} else if recent[2].TPS > recent[0].TPS*1.2 {
// 			trend = "improving ↗️"
// 		} else if recent[2].TPS < recent[0].TPS*0.8 {
// 			trend = "declining ↘️"
// 		}

// 		fmt.Printf("📈 Recent Trend: %s (Last 3 windows)\n", trend)
// 	}
// 	fmt.Println("⏰ ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
// }

// // 마지막 처리된 10개 컨트랙트 출력
// func printLastContracts(stats *IngestionStats) {
// 	fmt.Println("\n📋 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
// 	fmt.Println("📋 LAST 10 PROCESSED CONTRACTS")
// 	fmt.Println("📋 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

// 	// 처리된 컨트랙트 수에 따라 출력할 개수 결정
// 	contractsToShow := int(stats.contractCount)
// 	if contractsToShow > 10 {
// 		contractsToShow = 10
// 	}

// 	if contractsToShow == 0 {
// 		fmt.Println("   📭 No contracts processed yet")
// 		fmt.Println("📋 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
// 		return
// 	}

// 	// 순환 버퍼에서 가장 최근 것부터 역순으로 출력
// 	for i := 0; i < contractsToShow; i++ {
// 		// 가장 최근 인덱스부터 역순으로 계산
// 		idx := (stats.contractIndex - 1 - i + 10) % 10
// 		contract := stats.lastContracts[idx]

// 		fmt.Printf("   📄 #%d: %s (Block: %d)\n",
// 			contractsToShow-i,
// 			contract.Address.String(),
// 			contract.CreatedAt)
// 	}

// 	fmt.Printf("   📊 Total processed contracts: %d\n", stats.contractCount)
// 	fmt.Println("📋 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
// }

// // 최종 통계 출력
// func printFinalStats(stats *IngestionStats) {
// 	totalTime := time.Since(stats.startTime)
// 	processed := atomic.LoadInt64(&stats.processedContracts)
// 	converted := atomic.LoadInt64(&stats.convertedEvents)

// 	avgTPS := float64(converted) / totalTime.Seconds()

// 	fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
// 	fmt.Println("📈 FINAL STATISTICS")
// 	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
// 	fmt.Printf("⏱️ Total Time: %s\n", totalTime.Round(time.Second))
// 	fmt.Printf("📦 Contracts Processed: %d\n", processed)
// 	fmt.Printf("🔄 TraitEvents Created: %d\n", converted)
// 	fmt.Printf("⚡ Average TPS: %.2f events/sec\n", avgTPS)
// 	fmt.Printf("📊 Total 5-min Windows: %d\n", len(stats.tpsHistory))
// 	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
// }

// func TransfromContractToTraitEvent(cont shareddomain.Contract) (domain.TraitEvent, bool) {
// 	//생성자가 zero인 경우 트레이트 마킹이 불가
// 	if cont.CreatorOrZero.IsZero() {
// 		return domain.TraitEvent{}, false
// 	}
// 	//관계= 생성된 것 & 생성자
// 	trait := roperepo.TraitCreatorAndCreatedContract

// 	contractAndRule := domain.AddressAndRule{
// 		Address: cont.Address,
// 		Rule:    roperepo.RuleCreatedContract,
// 	}
// 	creatorAndRule := domain.AddressAndRule{
// 		Address: cont.CreatorOrZero,
// 		Rule:    roperepo.RuleCreator,
// 	}
// 	txScala := domain.TxScala{
// 		TxId:        cont.CreationTxOrZero,
// 		Time:        cont.CreatedAt,
// 		ScoreInc:    1,
// 		ValueOrNull: decimal.Zero,
// 	}
// 	traitEvent := domain.NewTraitEvent(
// 		roperepo.AuthorSystem,
// 		trait,
// 		contractAndRule,
// 		creatorAndRule,
// 		txScala,
// 	)
// 	return traitEvent, true
// }
