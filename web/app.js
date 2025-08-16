/**
 * ChainAnalyzer Dashboard - 실시간 모니터링 대시보드
 */
class ChainAnalyzerDashboard {
    constructor() {
        this.config = {
            updateInterval: 2000,  // 2초마다 업데이트
            maxErrors: 5,
            endpoints: {
                system: '/health',
                ee: {
                    health: '/ee/health',
                    statistics: '/ee/statistics', 
                    channel: '/ee/channel-status',
                    window: '/ee/dual-manager/window-stats',
                    graph: '/ee/graph/stats'
                }
            }
        };
        
        this.state = {
            lastUpdate: null,
            lastSuccessTime: null,
            errorCount: 0,
            isConnected: false
        };
        
        this.init();
    }

    // === 초기화 ===
    init() {
        console.log('🚀 ChainAnalyzer Dashboard 초기화');
        this.setupEventListeners();
        this.startAutoUpdate();
    }

    setupEventListeners() {
        window.addEventListener('focus', () => this.updateDashboard());
        document.addEventListener('keydown', (e) => {
            if (e.key === 'F5' || (e.ctrlKey && e.key === 'r')) {
                this.resetErrorState();
                this.updateDashboard();
            }
        });
    }

    startAutoUpdate() {
        this.updateDashboard(); // 즉시 첫 업데이트
        setInterval(() => this.updateDashboard(), this.config.updateInterval);
    }

    // === 메인 업데이트 로직 ===
    async updateDashboard() {
        try {
            console.log('📊 대시보드 업데이트 중...');
            
            await Promise.all([
                this.updateSystemStatus(),
                this.updateEEModule()
            ]);
            
            this.onUpdateSuccess();
            
        } catch (error) {
            this.onUpdateError(error);
        }
    }

    async updateSystemStatus() {
        const data = await this.fetchJSON(this.config.endpoints.system);
        
        const serverHealthy = Object.values(data.modules || {})
            .every(module => module.healthy !== false);
        
        this.updateStatusIndicator('server-health', serverHealthy);
        this.updateElement('module-count', Object.keys(data.modules || {}).length);
        
        if (data.modules?.EE?.statistics?.uptime_seconds) {
            this.updateElement('system-uptime', 
                this.formatUptime(data.modules.EE.statistics.uptime_seconds));
        }
    }

    async updateEEModule() {
        await Promise.all([
            this.updateEEBasicStatus(),
            this.updateAnalyzerStats(),
            this.updateChannelStatus(), 
            this.updateWindowStats(),
            this.updateGraphStatus()
        ]);
    }

    // === EE 모듈 업데이트 메소드들 ===
    async updateEEBasicStatus() {
        const healthData = await this.fetchJSON(this.config.endpoints.ee.health);
        const statsData = await this.fetchJSON(this.config.endpoints.ee.statistics);
        
        this.updateStatusIndicator('ee-health', healthData.healthy);
        this.updateElement('ee-processed', this.formatNumber(statsData.total_processed || 0));
        
        const successRate = this.calculateSuccessRate(statsData);
        this.updateElement('ee-success-rate', successRate + '%');
        
        const tps = this.calculateTPS(statsData);
        this.updateElement('ee-tps', tps);
    }

    async updateAnalyzerStats() {
        try {
            const stats = await this.fetchJSON(this.config.endpoints.ee.statistics);
            
            const statsItems = [
                { label: 'Mode', value: stats.mode || 'N/A' },
                { label: 'Total Processed', value: this.formatNumber(stats.total_processed || 0) },
                { label: 'Success Count', value: this.formatNumber(stats.success_count || 0) },
                { label: 'Error Count', value: this.formatNumber(stats.error_count || 0) },
                { label: 'Deposit Detections', value: this.formatNumber(stats.deposit_detections || 0) },
                { label: 'Graph Updates', value: this.formatNumber(stats.graph_updates || 0) },
                { label: 'Window Updates', value: this.formatNumber(stats.window_updates || 0) }
            ];
            
            this.renderStatsItems('analyzer-stats', statsItems);
            
        } catch (error) {
            this.showError('analyzer-stats', '분석기 통계 로드 실패');
        }
    }

    async updateChannelStatus() {
        try {
            const data = await this.fetchJSON(this.config.endpoints.ee.channel);
            const { usage = 0, capacity = 1, usage_percent = 0 } = data;
            
            const statsItems = [
                { label: 'Usage', value: `${usage} / ${capacity}` },
                { 
                    label: 'Usage %', 
                    value: `${usage_percent.toFixed(1)}%`,
                    className: this.getUsageClass(usage_percent)
                },
                { label: 'Status', value: data.status || 'normal' }
            ];
            
            this.renderStatsItems('channel-status', statsItems);
            this.updateProgressBar('channel-progress', usage_percent);
            
        } catch (error) {
            this.showError('channel-status', '채널 상태 로드 실패');
        }
    }

    async updateWindowStats() {
        try {
            const windowData = await this.fetchJSON(this.config.endpoints.ee.window);
            
            const statsItems = [
                { label: 'Active Buckets', value: windowData.active_buckets || 0 },
                { label: 'Total To Users', value: this.formatNumber(windowData.total_to_users || 0) },
                { label: 'Pending Relations', value: this.formatNumber(windowData.pending_relations || 0) },
                { label: 'Max Buckets', value: windowData.max_buckets || 21 },
                { label: 'Window Size', value: `${(windowData.window_size_hours || 0).toFixed(0)}h` },
                { label: 'Slide Interval', value: `${(windowData.slide_interval_hours || 0).toFixed(0)}h` }
            ];
            
            this.renderStatsItems('window-stats', statsItems);
            
        } catch (error) {
            this.showError('window-stats', '윈도우 통계 로드 실패');
        }
    }

    async updateGraphStatus() {
        try {
            const data = await this.fetchJSON(this.config.endpoints.ee.graph);
            
            const statsItems = [
                { 
                    label: 'Database Available', 
                    value: data.database_available ? 'Yes' : 'No',
                    className: data.database_available ? 'text-success' : 'text-error'
                },
                { 
                    label: 'Status', 
                    value: data.database_available ? 'Ready' : 'Not Ready'
                },
                {
                    label: 'Message',
                    value: data.message || 'N/A'
                }
            ];
            
            this.renderStatsItems('graph-status', statsItems);
            
        } catch (error) {
            this.showError('graph-status', '그래프 상태 로드 실패');
        }
    }

    // === 유틸리티 메소드들 ===
    async fetchJSON(url) {
        const response = await fetch(url);
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        return response.json();
    }

    calculateSuccessRate(stats) {
        const total = stats.total_processed || 0;
        const success = stats.success_count || 0;
        return total > 0 ? ((success / total) * 100).toFixed(1) : '0';
    }

    calculateTPS(stats) {
        const total = stats.total_processed || 0;
        const uptime = stats.uptime_seconds || 1;
        return uptime > 0 ? (total / uptime).toFixed(1) : '0';
    }

    getUsageClass(usagePercent) {
        if (usagePercent > 90) return 'text-error';
        if (usagePercent > 70) return 'text-warning';
        return 'text-success';
    }

    formatNumber(num) {
        if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
        if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
        return num.toString();
    }

    formatUptime(seconds) {
        const hours = Math.floor(seconds / 3600);
        const minutes = Math.floor((seconds % 3600) / 60);
        const secs = Math.floor(seconds % 60);
        
        if (hours > 0) return `${hours}h ${minutes}m ${secs}s`;
        if (minutes > 0) return `${minutes}m ${secs}s`;
        return `${secs}s`;
    }

    // === DOM 업데이트 메소드들 ===
    updateElement(id, content) {
        const element = document.getElementById(id);
        if (element) {
            element.textContent = content;
        }
    }

    updateStatusIndicator(id, isHealthy) {
        const element = document.getElementById(id);
        if (element) {
            element.textContent = isHealthy ? 'Healthy' : 'Unhealthy';
            element.className = `status-indicator ${isHealthy ? 'healthy' : 'unhealthy'}`;
        }
    }

    renderStatsItems(containerId, items) {
        const container = document.getElementById(containerId);
        if (!container) return;
        
        container.innerHTML = items.map(item => `
            <div class="stats-item">
                <span class="stats-label">${item.label}</span>
                <span class="stats-value ${item.className || ''}">${item.value}</span>
            </div>
        `).join('');
    }

    updateProgressBar(barId, percentage) {
        const progressBar = document.getElementById(barId);
        if (!progressBar) return;
        
        progressBar.style.width = percentage + '%';
        
        // 색상 변경
        if (percentage > 90) {
            progressBar.style.background = 'linear-gradient(90deg, #f56565, #e53e3e)';
        } else if (percentage > 70) {
            progressBar.style.background = 'linear-gradient(90deg, #ed8936, #dd6b20)';
        } else {
            progressBar.style.background = 'linear-gradient(90deg, #4299e1, #667eea)';
        }
    }

    showError(containerId, message) {
        const container = document.getElementById(containerId);
        if (container) {
            container.innerHTML = `<div class="loading">${message}</div>`;
        }
    }

    // === 상태 관리 ===
    onUpdateSuccess() {
        this.resetErrorState();
        this.updateServerStatus('✅ 연결됨');
        this.updateLastSuccessTime();
        this.updateLastUpdateTime();
    }

    onUpdateError(error) {
        console.error('❌ 대시보드 업데이트 실패:', error);
        this.state.errorCount++;
        this.updateLastUpdateTime(); // 에러가 발생해도 마지막 업데이트 시간은 갱신
        
        if (this.state.errorCount >= this.config.maxErrors) {
            this.updateServerStatus('❌ 에러 발생');
            console.error(`연속 ${this.config.maxErrors}회 오류 발생`);
        } else {
            this.updateServerStatus(`⚠️ 오류 (${this.state.errorCount}/${this.config.maxErrors})`);
        }
    }

    resetErrorState() {
        this.state.errorCount = 0;
        this.state.isConnected = true;
    }

    updateServerStatus(status) {
        this.updateElement('server-status', status);
    }

    updateLastSuccessTime() {
        this.state.lastSuccessTime = new Date();
        const timeString = this.state.lastSuccessTime.toLocaleTimeString();
        this.updateElement('last-success-time', `마지막 정상: ${timeString}`);
    }

    updateLastUpdateTime() {
        this.state.lastUpdate = new Date();
        const timeString = this.state.lastUpdate.toLocaleTimeString();
        this.updateElement('last-update-time', `마지막 시도: ${timeString}`);
    }
}

// 페이지 로드시 대시보드 초기화
document.addEventListener('DOMContentLoaded', () => {
    window.dashboard = new ChainAnalyzerDashboard();
    console.log('✅ 대시보드 초기화 완료');
});