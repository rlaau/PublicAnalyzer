⏰ TX #508300 time: 2034-11-16 18:50:00 (1주=1008분=약17tx, 21개 버킷=357tx에서 순환)
   🔍 DetectNewDepositAddress: 0x2a5c787b... → CEX 0x18e22645...
   📈 Existing deposit, updating count to 2
   ⏰ Test completed by timeout
2025/07/24 01:02:02 🛑 Context cancelled: Fixed-Pipeline-Analyzer
2025/07/24 01:02:02 🔄 Shutting down: Fixed-Pipeline-Analyzer
Transaction generation stopped by context
2025/07/24 01:02:02 🎯 DEPOSIT DETECTED #10389: From: 0x2a5c787b... → CEX: 0x18e22645...
2025/07/24 01:02:02 📈 WINDOW UPDATE #518716 (with deposit): From: 0x2a5c787b... → To: 0x18e22645...
2025/07/24 01:02:02 🔧 Worker 1 stopping (context)
2025/07/24 01:02:02 🔧 Worker 4 stopping (signal)
2025/07/24 01:02:02 🔧 Worker 7 stopping (context)
2025/07/24 01:02:02 🔧 Worker 3 stopping (signal)
2025/07/24 01:02:02 🔧 Worker 0 stopping (context)
2025/07/24 01:02:02 🔧 Worker 6 stopping (signal)
2025/07/24 01:02:02 🔧 Worker 2 stopping (signal)
2025/07/24 01:02:02 🔧 Worker 5 stopping (context)
2025/07/24 01:02:02 ✅ All workers stopped gracefully
2025/07/24 01:02:02 
================================================================================
2025/07/24 01:02:02 🎯 FINAL REPORT: Fixed-Pipeline-Analyzer
2025/07/24 01:02:02 ================================================================================
2025/07/24 01:02:02 📊 Performance Summary:
2025/07/24 01:02:02    Total Runtime: 10m0s
2025/07/24 01:02:02    Transactions Processed: 519415
2025/07/24 01:02:02    Success Rate: 100.00% (519415/519415)
2025/07/24 01:02:02    Processing Rate: 865.6 tx/sec
2025/07/24 01:02:02    Errors: 0 | Dropped: 1296816
2025/07/24 01:02:02 
🔍 Analysis Results:
2025/07/24 01:02:02    Deposit Detections: 10389
2025/07/24 01:02:02    Graph Updates: 683
2025/07/24 01:02:02    Window Updates: 518732
2025/07/24 01:02:02 
🪟 Window Manager State:
2025/07/24 01:02:02    max_buckets: 21
2025/07/24 01:02:02    active_buckets: 21
2025/07/24 01:02:02    total_to_users: 20425
2025/07/24 01:02:02    pending_relations: 20424
2025/07/24 01:02:02    window_size_hours: 2880
2025/07/24 01:02:02    slide_interval_hours: 168
2025/07/24 01:02:02 
🗂️  Graph Database State:
2025/07/24 01:02:02    total_connections: 1452
2025/07/24 01:02:02    total_edges: 726
2025/07/24 01:02:02    total_nodes: 1415
2025/07/24 01:02:02 ================================================================================
2025/07/24 01:02:02 📊 [testing] Fixed-Pipeline-Analyzer Statistics:
2025/07/24 01:02:02    Uptime: 10m0s | Processed: 519415 | Success: 519415 | Errors: 0
2025/07/24 01:02:02    Deposits: 10389 | Graph: 683 | Window: 518732 | Dropped: 1296816
2025/07/24 01:02:02    Channel: 999991/1000000 (100.0%)
2025/07/24 01:02:02    Rate: 865.4 tx/sec | Success Rate: 100.0%
2025/07/24 01:02:02    Buckets: 21 | Pending: 20424
2025/07/24 01:02:02    Graph: 1415 nodes | 726 edges
2025/07/24 01:02:03 🧹 Cleaning up test data: /home/rlaaudgjs5638/chainAnalyzer/debug_queue_fixed
2025/07/24 01:02:03 ✅ Test data cleaned up
2025/07/24 01:02:03 ✅ Shutdown completed: Fixed-Pipeline-Analyzer

==========================================================================================
📊 FIXED QUEUE-BASED INTEGRATION TEST RESULTS
==========================================================================================
🔢 Pipeline Stats:
   Generated:    2816222 transactions
   Transmitted:  2816222 transactions
   Processed:    1519406 transactions
   Dropped:      0 transactions
   Runtime:      600.6 seconds
   Transmission: 100.0% (2816222/2816222)
   Processing:   54.0% (1519406/2816222)
   Overall:      54.0% (1519406/2816222)
   Gen Rate:     4689.2 tx/sec
   Proc Rate:    2529.9 tx/sec

🎯 Transaction Type Analysis:
   CEX Transactions:     0 (0.0%)
   Deposit Transactions: 0 (0.0%)
   Random Transactions:  0 (0.0%)
   Processing Failures:  1296816

⚡ Analyzer Details:
   error_count         : 0
   deposit_detections  : 10389
   window_updates      : 518732
   uptime_seconds      : 600.574388079
   channel_capacity    : 1000000
   mode                : testing
   total_processed     : 519415
   success_count       : 519415
   graph_updates       : 683
   dropped_txs         : 1296816
   channel_usage       : 999991
   name                : Fixed-Pipeline-Analyzer

💚 System Health: false

🔧 Diagnostic Summary:
   ⚠️  CEX transaction ratio lower than expected (0 vs 563244 expected)
==========================================================================================

✅ Fixed integration test completed successfully!

🧹 Cleaning up isolated environment...
   ✅ Cleaned: /home/rlaaudgjs5638/chainAnalyzer/debug_queue_fixed
🔒 No permanent changes to system
