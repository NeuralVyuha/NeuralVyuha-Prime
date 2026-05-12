(function () {
    'use strict';

    angular.module('neuralvyuha').controller('NvGroupListCtrl', function ($scope, $state, $interval, NvApiSrv, VersionSrv, nvSocketSrv) {
        var vm = this;

        // --- State ---
        vm.loading = true;
        vm.isLoading = true; // Support new UI
        vm.error = null;
        vm.groups = [];
        vm.total = 0;
        vm.totalGroups = 0; // Support new UI
        vm.currentPage = 0; // Support new UI
        vm.pageSize = 20;   // Support new UI
        vm.lastRefreshed = null;
        vm.autoRefreshEnabled = true;
        vm.autoRefreshSeconds = 30;
        vm.lastUpdated = new Date();

        vm.stats = {
            totalIncidents: 0,
            openIncidents: 0,
            totalAlerts: 0,
            totalCases: 0,
            trends: {
                totalIncidents: { direction: 'neutral', value: 0 },
                openIncidents: { direction: 'neutral', value: 0 },
                totalAlerts: { direction: 'neutral', value: 0 },
                totalCases: { direction: 'neutral', value: 0 }
            }
        };

        vm.chartData = {
            timeline: [],
            severity: []
        };

        vm.systemVersion = '';
        vm.socketStatus = 'connecting';
        vm.engineMetrics = {
            events_per_sec: 0,
            cpu_usage: 0,
            mem_usage: 0,
            active_threads: 0
        };

        vm.healthChecks = [
            { label: 'NeuralVyuha Engine', status: 'checking', detail: '' },
            { label: 'Query Service', status: 'checking', detail: '' },
            { label: 'Cases Database', status: 'checking', detail: '' },
            { label: 'Alert Pipeline', status: 'checking', detail: '' }
        ];

        vm.params = {
            size: 20,
            from_: 0,
            status: 'OPEN',
            severity: undefined,
            search: undefined,
            dateFrom: undefined,
            dateTo: undefined
        };

        // --- Advanced Filtering State ---
        vm.severityFilter = null;
        vm.searchQuery = '';
        vm.dateRange = { from: null, to: null };
        vm.showDateFilter = false;

        // --- Greeting ---
        vm.getGreeting = function () {
            var hour = new Date().getHours();
            if (hour < 12) return 'Good Morning';
            if (hour < 17) return 'Good Afternoon';
            return 'Good Evening';
        };
        vm.greeting = vm.getGreeting();

        // --- Uptime formatter ---
        vm.formatUptime = function (seconds) {
            if (!seconds) return '0h 0m';
            var h = Math.floor(seconds / 3600);
            var m = Math.floor((seconds % 3600) / 60);
            return h + 'h ' + m + 'm';
        };

        // --- Data Loading ---
        vm.load = function () {
            vm.loading = true;
            vm.isLoading = true;
            var query = angular.copy(vm.params);
            query.size = vm.pageSize;
            query.from_ = vm.currentPage * vm.pageSize;

            NvApiSrv.getGroups(query).then(function (data) {
                vm.groups = (data && data.items) || [];
                vm.total = (data && data.total) || 0;
                vm.totalGroups = vm.total;
                vm.lastRefreshed = new Date();
                vm.lastUpdated = new Date();
                vm.loading = false;
                vm.isLoading = false;
            }).catch(function (err) {
                vm.groups = [];
                vm.total = 0;
                vm.error = 'Unable to load incidents. The server may be unreachable.';
            }).finally(function () {
                vm.loading = false;
            });
        };

        // --- Load dashboard stats ---
        vm.loadStats = function () {
            // Helper for trend calculation
            function calcTrend(curr, prev) {
                if (!prev) return { direction: 'neutral', value: 0 };
                var diff = ((curr - prev) / prev) * 100;
                return {
                    direction: diff > 0 ? 'up' : (diff < 0 ? 'down' : 'neutral'),
                    value: Math.abs(Math.round(diff))
                };
            }

            NvApiSrv.getGroups({ size: 1, from_: 0 }).then(function (data) {
                var newTotal = (data && data.total) || 0;
                vm.stats.trends.totalIncidents = calcTrend(newTotal, vm.stats.totalIncidents);
                vm.stats.totalIncidents = newTotal;
            }).catch(function () { /* fail silently for stats */ });

            NvApiSrv.getGroups({ size: 1, from_: 0, status: 'OPEN' }).then(function (data) {
                var newOpen = (data && data.total) || 0;
                vm.stats.trends.openIncidents = calcTrend(newOpen, vm.stats.openIncidents);
                vm.stats.openIncidents = newOpen;
            }).catch(function () { });

            NvApiSrv.getAlerts({ size: 1, from_: 0 }).then(function (data) {
                var newAlerts = (data && data.total) || 0;
                vm.stats.trends.totalAlerts = calcTrend(newAlerts, vm.stats.totalAlerts);
                vm.stats.totalAlerts = newAlerts;
            }).catch(function () { });

            NvApiSrv.getCases({ limit: 1, offset: 0 }).then(function (data) {
                var newCases = (data && data.total) || 0;
                vm.stats.trends.totalCases = calcTrend(newCases, vm.stats.totalCases);
                vm.stats.totalCases = newCases;
            }).catch(function () { });

            // Load Chart Data (Mocking for Phase 1 visual demonstration)
            vm.loadChartData();

            // Enterprise: Load Engine Telemetry
            vm.loadEngineMetrics();
        };

        // --- Load engine metrics (Enterprise command center) ---
        vm.loadEngineMetrics = function () {
            NvApiSrv.getEngineMetrics().then(function (data) {
                if (data) {
                    vm.engineMetrics.events_per_sec = data.eps || 0;
                    vm.engineMetrics.cpu_usage = data.cpu_percent || 0;
                    vm.engineMetrics.mem_usage = data.memory_mb || 0;
                    vm.engineMetrics.active_threads = data.uptime_seconds || 0;
                    vm.engineMetrics.events_total = data.events_total || 0;
                    vm.engineMetrics.kafka_lag = data.kafka_lag || 0;
                }
            }).catch(function () {
                // Fail silently for metrics
            });
        };

        // --- Real-time Socket Integration ---
        function initSocket() {
            nvSocketSrv.connect();
            vm.socketStatus = 'connected';
        }

        $scope.$on('vyuha-stream-event', function (event, msg) {
            $scope.$applyAsync(function () {
                // Increment counters in real-time if valid
                if (msg.type === 'alert') {
                    vm.stats.totalAlerts++;
                    vm.stats.openIncidents++;
                }

                // Refresh charts if timeline needs update
                if (vm.chartData.timeline.length > 0) {
                    var lastPoint = vm.chartData.timeline[vm.chartData.timeline.length - 1];
                    if (msg.type === 'alert') lastPoint.alerts++;
                    else lastPoint.incidents++;
                }
            });
        });

        vm.loadChartData = function () {
            // Timeline Chart — Real data from OpenSearch date_histogram
            NvApiSrv.getAlertTimeline({ days: 14 }).then(function (data) {
                var dates = ['x'];
                var alertCounts = ['Alerts'];
                var criticalCounts = ['Critical'];

                if (data && data.timeline && data.timeline.length > 0) {
                    angular.forEach(data.timeline, function (day) {
                        dates.push(day.date);
                        alertCounts.push(day.count || 0);
                        criticalCounts.push((day.critical || 0) + (day.high || 0));
                    });
                } else {
                    // Graceful fallback: show empty chart with date axis
                    for (var i = 14; i >= 0; i--) {
                        dates.push(moment().subtract(i, 'days').format('YYYY-MM-DD'));
                        alertCounts.push(0);
                        criticalCounts.push(0);
                    }
                }

                vm.timelineChart = {
                    data: {
                        x: 'x',
                        columns: [dates, alertCounts, criticalCounts],
                        type: 'area-spline'
                    },
                    axis: {
                        x: {
                            type: 'timeseries',
                            tick: { format: '%b %d', culling: { max: 7 } }
                        },
                        y: { padding: { top: 20, bottom: 0 } }
                    },
                    point: { show: false },
                    grid: { y: { show: true } },
                    legend: { position: 'inset' }
                };
            }).catch(function () {
                // Fallback: empty chart
                var dates = ['x'];
                var alertCounts = ['Alerts'];
                for (var i = 14; i >= 0; i--) {
                    dates.push(moment().subtract(i, 'days').format('YYYY-MM-DD'));
                    alertCounts.push(0);
                }
                vm.timelineChart = {
                    data: { x: 'x', columns: [dates, alertCounts], type: 'area-spline' },
                    axis: { x: { type: 'timeseries', tick: { format: '%b %d' } } },
                    point: { show: false }, grid: { y: { show: true } }
                };
            });

            // Severity Distribution — Real data from alert stats aggregation
            NvApiSrv.getAlertStats().then(function (data) {
                var sev = (data && data.by_severity) || {};
                vm.severityChart = {
                    data: {
                        columns: [
                            ['Critical', sev['4'] || sev.critical || 0],
                            ['High', sev['3'] || sev.high || 0],
                            ['Medium', sev['2'] || sev.medium || 0],
                            ['Low', sev['1'] || sev.low || 0]
                        ],
                        type: 'donut',
                        colors: {
                            'Critical': '#f5576c',
                            'High': '#f093fb',
                            'Medium': '#fbcd35',
                            'Low': '#4facfe'
                        }
                    },
                    donut: {
                        title: 'Threat Levels',
                        width: 30,
                        label: { show: false }
                    },
                    legend: { position: 'right' }
                };
            }).catch(function () {
                vm.severityChart = {
                    data: {
                        columns: [['Critical', 0], ['High', 0], ['Medium', 0], ['Low', 0]],
                        type: 'donut',
                        colors: { 'Critical': '#f5576c', 'High': '#f093fb', 'Medium': '#fbcd35', 'Low': '#4facfe' }
                    },
                    donut: { title: 'Threat Levels', width: 30, label: { show: false } },
                    legend: { position: 'right' }
                };
            });
        };

        // --- Load system health (deep metrics) ---
        vm.loadHealth = function () {
            var t0;

            // Engine version + latency
            t0 = Date.now();
            VersionSrv.get().then(function (config) {
                var latency = Date.now() - t0;
                vm.systemVersion = (config && config.versions && config.versions.NeuralVyuha) ? config.versions.NeuralVyuha : 'NeuralVyuha Zenith';
                vm.healthChecks[0].status = 'online';
                vm.healthChecks[0].detail = 'v' + vm.systemVersion;
                vm.healthChecks[0].latency = latency;
            }).catch(function () {
                vm.healthChecks[0].status = 'offline';
                vm.healthChecks[0].detail = 'Unreachable';
                vm.healthChecks[0].latency = -1;
            });

            // Query service + latency
            t0 = Date.now();
            NvApiSrv.healthCheck().then(function (data) {
                var latency = Date.now() - t0;
                if (data) {
                    vm.healthChecks[1].status = 'online';
                    vm.healthChecks[1].detail = 'Operational';
                    vm.healthChecks[1].latency = latency;
                } else {
                    vm.healthChecks[1].status = 'offline';
                    vm.healthChecks[1].detail = 'Unreachable';
                    vm.healthChecks[1].latency = -1;
                }
            });

            // Cases database + latency
            t0 = Date.now();
            NvApiSrv.getCases({ limit: 1, offset: 0 }).then(function () {
                var latency = Date.now() - t0;
                vm.healthChecks[2].status = 'online';
                vm.healthChecks[2].detail = 'Connected';
                vm.healthChecks[2].latency = latency;
            }).catch(function () {
                vm.healthChecks[2].status = 'offline';
                vm.healthChecks[2].detail = 'Disconnected';
                vm.healthChecks[2].latency = -1;
            });

            // Alert pipeline + latency
            t0 = Date.now();
            NvApiSrv.getAlerts({ size: 1, from_: 0 }).then(function () {
                var latency = Date.now() - t0;
                vm.healthChecks[3].status = 'online';
                vm.healthChecks[3].detail = 'Active';
                vm.healthChecks[3].latency = latency;
            }).catch(function () {
                vm.healthChecks[3].status = 'offline';
                vm.healthChecks[3].detail = 'Inactive';
                vm.healthChecks[3].latency = -1;
            });
        };


        vm.isAllHealthy = function () {
            return vm.healthChecks.every(function (check) {
                return check.status === 'online';
            });
        };

        // --- Filter by status ---
        vm.filter = 'OPEN'; // Support new UI
        vm.setFilter = function (status) {
            vm.filter = status;
            vm.params.status = status === 'ALL' ? undefined : status;
            vm.currentPage = 0;
            vm.load();
        };

        // --- Severity Filter ---
        vm.setSeverityFilter = function (sev) {
            if (vm.severityFilter === sev) {
                vm.severityFilter = null;
                vm.params.severity = undefined;
            } else {
                vm.severityFilter = sev;
                vm.params.severity = sev;
            }
            vm.currentPage = 0;
            vm.load();
        };

        // --- Global Search ---
        vm.onSearch = function () {
            vm.params.search = vm.searchQuery || undefined;
            vm.currentPage = 0;
            vm.load();
        };

        vm.clearSearch = function () {
            vm.searchQuery = '';
            vm.params.search = undefined;
            vm.currentPage = 0;
            vm.load();
        };

        // --- Date Range Filter ---
        vm.toggleDateFilter = function () {
            vm.showDateFilter = !vm.showDateFilter;
        };

        vm.applyDateFilter = function () {
            vm.params.dateFrom = vm.dateRange.from ? moment(vm.dateRange.from).valueOf() : undefined;
            vm.params.dateTo = vm.dateRange.to ? moment(vm.dateRange.to).endOf('day').valueOf() : undefined;
            vm.currentPage = 0;
            vm.load();
        };

        vm.clearDateFilter = function () {
            vm.dateRange = { from: null, to: null };
            vm.params.dateFrom = undefined;
            vm.params.dateTo = undefined;
            vm.currentPage = 0;
            vm.load();
        };

        vm.filterStatus = function (status) {
            vm.params.status = status;
            vm.params.from_ = 0;
            vm.load();
        };

        // --- Pagination ---
        vm.nextPage = function () {
            if ((vm.currentPage + 1) * vm.pageSize < vm.totalGroups) {
                vm.currentPage++;
                vm.load();
            }
        };

        vm.prevPage = function () {
            if (vm.currentPage > 0) {
                vm.currentPage--;
                vm.load();
            }
        };

        vm.getCurrentPage = function () {
            return vm.currentPage + 1;
        };

        vm.getTotalPages = function () {
            return Math.max(1, Math.ceil(vm.totalGroups / vm.pageSize));
        };

        // --- Open group detail ---
        vm.viewDetail = function (group) {
            vm.openGroup(group);
        };

        vm.openGroup = function (group) {
            if (group && (group.group_id || group._id)) {
                $state.go('app.nv-group-detail', { id: group.group_id || group._id });
            }
        };

        // --- Retry after error ---
        vm.retry = function () {
            vm.error = null;
            vm.load();
            vm.loadStats();
            vm.loadHealth();
        };

        // --- Auto-refresh ---
        vm.toggleAutoRefresh = function () {
            vm.autoRefreshEnabled = !vm.autoRefreshEnabled;
            if (vm.autoRefreshEnabled) {
                startAutoRefresh();
            } else {
                stopAutoRefresh();
            }
        };

        var refreshInterval = null;
        function startAutoRefresh() {
            stopAutoRefresh();
            refreshInterval = $interval(function () {
                vm.load();
                vm.loadStats();
                vm.loadHealth();
            }, vm.autoRefreshSeconds * 1000);
        }

        function stopAutoRefresh() {
            if (refreshInterval) {
                $interval.cancel(refreshInterval);
                refreshInterval = null;
            }
        }

        // --- Severity helper ---
        vm.getSeverityLabel = function (severity) {
            if (severity >= 4) return 'CRITICAL';
            if (severity === 3) return 'HIGH';
            if (severity === 2) return 'MEDIUM';
            return 'LOW';
        };

        vm.getSeverityClass = function (severity) {
            if (severity >= 4) return 'nv-sev-critical';
            if (severity === 3) return 'nv-sev-high';
            if (severity === 2) return 'nv-sev-medium';
            return 'nv-sev-low';
        };

        // --- Fast-polling engine telemetry (3s interval) ---
        var metricsInterval = null;
        function startMetricsPolling() {
            stopMetricsPolling();
            metricsInterval = $interval(function () {
                vm.loadEngineMetrics();
            }, 3000);
        }
        function stopMetricsPolling() {
            if (metricsInterval) {
                $interval.cancel(metricsInterval);
                metricsInterval = null;
            }
        }

        // --- Cleanup on destroy ---
        $scope.$on('$destroy', function () {
            stopAutoRefresh();
            stopMetricsPolling();
        });

        // --- Initial load ---
        vm.load();
        vm.loadStats();
        vm.loadHealth();
        initSocket();
        startMetricsPolling();

        if (vm.autoRefreshEnabled) {
            startAutoRefresh();
        }
    });
})();
