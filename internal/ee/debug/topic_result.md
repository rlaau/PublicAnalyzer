  ⏰ Test completed by timeout
2025/07/24 22:24:48 🛑 Context cancelled: Simplified-Pipeline-Analyzer

============================================================
📊 SIMPLIFIED PIPELINE TEST RESULTS
2025/07/24 22:24:48 🔄 Shutting down: Simplified-Pipeline-Analyzer
============================================================
Generated: 768795 | Transmitted: 768795 | Runtime: 60.6s
Analyzer Success: 148794 | Healthy: true
============================================================
🧹 Cleaning up fed-tx Kafka topic...
   ✅ SaveDetectedDeposit succeeded
2025/07/24 22:24:48 🎯 DEPOSIT DETECTED #2977: From: 0xada758f4... → CEX: 0x77696bb3...
2025/07/24 22:24:48 📈 WINDOW UPDATE #148744 (with deposit): From: 0xada758f4... → To: 0x77696bb3...
2025/07/24 22:24:48 🔧 Worker 3 stopping (context)
2025/07/24 22:24:48 🔧 Worker 0 stopping (context)
2025/07/24 22:24:48 🔧 Worker 6 stopping (context)
2025/07/24 22:24:48 🔧 Worker 5 stopping (context)
2025/07/24 22:24:48 🔧 Worker 4 stopping (context)
2025/07/24 22:24:48 🔧 Worker 7 stopping (context)
2025/07/24 22:24:48 🔧 Worker 1 stopping (context)
2025/07/24 22:24:48 🔧 Worker 2 stopping (context)
2025/07/24 22:24:48 ✅ All workers stopped gracefully
2025/07/24 22:24:48 
================================================================================
2025/07/24 22:24:48 🎯 FINAL REPORT: Simplified-Pipeline-Analyzer
2025/07/24 22:24:48 ================================================================================
2025/07/24 22:24:48 📊 Performance Summary:
2025/07/24 22:24:48    Total Runtime: 1m0s
2025/07/24 22:24:48    Transactions Processed: 148813
2025/07/24 22:24:48    Success Rate: 100.00% (148813/148813)
2025/07/24 22:24:48    Processing Rate: 2480.0 tx/sec
2025/07/24 22:24:48    Errors: 0 | Dropped: 0
2025/07/24 22:24:48 
🔍 Analysis Results:
2025/07/24 22:24:48    Deposit Detections: 2977
2025/07/24 22:24:48    Graph Updates: 51
2025/07/24 22:24:48    Window Updates: 148762
2025/07/24 22:24:48 
🪟 Window Manager State:
2025/07/24 22:24:48    max_buckets: 21
2025/07/24 22:24:48    active_buckets: 21
2025/07/24 22:24:48    total_to_users: 19804
2025/07/24 22:24:48    pending_relations: 19803
2025/07/24 22:24:48    window_size_hours: 2880
2025/07/24 22:24:48    slide_interval_hours: 168
2025/07/24 22:24:48 
🗂️  Graph Database State:
2025/07/24 22:24:48    total_connections: 148
2025/07/24 22:24:48    total_edges: 74
2025/07/24 22:24:48    total_nodes: 147
2025/07/24 22:24:48 ================================================================================
2025/07/24 22:24:48 📊 [testing] Simplified-Pipeline-Analyzer Statistics:
2025/07/24 22:24:48    Uptime: 1m0s | Processed: 148813 | Success: 148813 | Errors: 0
2025/07/24 22:24:48    Deposits: 2977 | Graph: 51 | Window: 148762 | Dropped: 0
2025/07/24 22:24:48    Channel: 619894/1000000 (62.0%)
2025/07/24 22:24:48    Rate: 2477.9 tx/sec | Success Rate: 100.0%
2025/07/24 22:24:48    Buckets: 21 | Pending: 19803
2025/07/24 22:24:48    Graph: 147 nodes | 74 edges
2025/07/24 22:24:48 🧹 Cleaning up test data: /home/rlaaudgjs5638/chainAnalyzer/debug_queue_fixed
2025/07/24 22:24:48 ✅ Test data cleaned up
2025/07/24 22:24:48 ✅ Shutdown completed: Simplified-Pipeline-Analyzer
   ✅ Fed-tx topic cleaned up

🧹 Cleaning up isolated environment...
   ✅ Cleaned: /home/rlaaudgjs5638/chainAnalyzer/debug_queue_fixed
🔒 No permanent changes to system

✅ Fixed integration test completed successfully!
