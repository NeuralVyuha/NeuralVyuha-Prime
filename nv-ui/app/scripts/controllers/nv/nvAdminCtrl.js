/**
 * nvAdminCtrl.js — NeuralVyuha Admin Panel
 * Tabs: Platform | Users | System Health
 */
(function () {
    'use strict';

    angular.module('nvControllers').controller('NvAdminCtrl', function (
        $scope, $http, $timeout, $interval, AuthenticationSrv, NotificationSrv, NvApiSrv, VersionSrv
    ) {
        var vm = this;

        // ── State ──────────────────────────────────────────────────────────
        vm.activeTab = 'platform';
        vm.loading = false;
        vm.platform = {};
        vm.users = [];
        vm.health = {};
        vm.currentUser = AuthenticationSrv.currentUser || {};

        // --- Telemetry State (Relocated from Dashboard) ---
        vm.engineMetrics = {
            events_per_sec: 0,
            cpu_usage: 0,
            mem_usage: 0,
            active_threads: 0
        };
        vm.stats = {
            totalAlerts: 0
        };
        vm.healthChecks = []; // For compatibility with isAllHealthy check

        // ── Tab Navigation ─────────────────────────────────────────────────
        vm.setTab = function (tab) {
            vm.activeTab = tab;
            if (tab === 'platform') { vm.loadPlatform(); }
            if (tab === 'users') { vm.loadUsers(); }
            if (tab === 'health') { 
                vm.loadHealth(); 
                vm.loadEngineMetrics();
                vm.loadStats();
                startPolling();
            } else {
                stopPolling();
            }
            if (tab === 'ai') { vm.loadAiStatus(); }
        };

        // --- Uptime formatter ---
        vm.formatUptime = function (seconds) {
            if (!seconds) return '0h 0m';
            var h = Math.floor(seconds / 3600);
            var m = Math.floor((seconds % 3600) / 60);
            return h + 'h ' + m + 'm';
        };

        // --- Load engine metrics ---
        vm.loadEngineMetrics = function () {
            NvApiSrv.getEngineMetrics().then(function (data) {
                if (data) {
                    vm.engineMetrics.events_per_sec = data.eps || 0;
                    vm.engineMetrics.cpu_usage = data.cpu_percent || 0;
                    vm.engineMetrics.mem_usage = data.memory_mb || 0;
                    vm.engineMetrics.active_threads = data.uptime_seconds || 0;
                }
            }).catch(function () {
                // Fail silently
            });
        };

        vm.loadStats = function () {
            NvApiSrv.getAlerts({ size: 1, from_: 0 }).then(function (data) {
                vm.stats.totalAlerts = (data && data.total) || 0;
            }).catch(function () { });
        };

        vm.isAllHealthy = function () {
            if (!vm.health.services) return false;
            // Only care about core services for the global STABLE status.
            // Connectors (Cortex/MISP) might be UNKNOWN if not configured.
            var coreServices = [
                'nv-query (API Gateway)',
                'OpenSearch (Persistence)',
                'PostgreSQL (Vault)',
                'MinIO (Evidence)',
                'nv-socket (WebSocket)',
                'nv-ingest (Event Spine)',
                'Ingest Service',
                'Detective Engine',
                'Correlation Logic',
                'Case Engine',
                'Workflow Engine'
            ];
            return vm.health.services.every(function (svc) {
                if (coreServices.indexOf(svc.name) === -1) return true; // Ignore connectors
                return svc.status.toUpperCase() === 'UP' || svc.status.toUpperCase() === 'OK';
            });
        };

        // --- Fast-polling engine telemetry (3s interval) ---
        var metricsInterval = null;
        var healthInterval = null;

        function startPolling() {
            stopPolling();
            // Metrics (EPS, CPU, etc.) - every 3s
            metricsInterval = $interval(function () {
                vm.loadEngineMetrics();
                vm.loadStats();
            }, 3000);
            // Health Status (UP/DOWN/NOT_CONFIGURED) - every 5s
            healthInterval = $interval(function () {
                vm.loadHealth();
            }, 5000);
        }

        function stopPolling() {
            if (metricsInterval) {
                $interval.cancel(metricsInterval);
                metricsInterval = null;
            }
            if (healthInterval) {
                $interval.cancel(healthInterval);
                healthInterval = null;
            }
        }

        // ── AI Configuration ───────────────────────────────────────────────
        vm.aiStatus = {};
        vm.aiKeys = {};
        vm.savingAi = false;

        vm.loadAiStatus = function () {
            var providers = ['gemini', 'openai', 'anthropic', 'ollama'];
            $http.get('/node-service/nodes').then(function (res) {
                var data = res.data || {};
                var nodes = data.nodes || (Array.isArray(data) ? data : []);
                providers.forEach(function (p) {
                    var match = nodes.find(function (n) { return n.node_type === p; });
                    vm.aiStatus[p] = (match && match.status) ? match.status : 'Not Configured';
                });
            });
        };

        vm.saveAiKey = function (provider) {
            var key = vm.aiKeys[provider];
            if (!key) { return; }
            vm.savingAi = true;

            // First check if node exists
            $http.get('/node-service/nodes').then(function (res) {
                var data = res.data || {};
                var nodes = data.nodes || (Array.isArray(data) ? data : []);
                var existing = nodes.find(function (n) { return n.node_type === provider; });

                if (existing) {
                    return $http.patch('/node-service/nodes/' + existing.id, { api_key: key });
                } else {
                    return $http.post('/node-service/nodes', {
                        node_type: provider,
                        name: provider.charAt(0).toUpperCase() + provider.slice(1) + ' Engine',
                        url: '', // Use default SaaS URLs handled by backend
                        api_key: key
                    });
                }
            }).then(function () {
                NotificationSrv.success('AI Config updated for ' + provider);
                vm.aiKeys[provider] = '';
                vm.loadAiStatus();
            }).catch(function (err) {
                NotificationSrv.error('Failed to save AI key: ' + (err.data && err.data.detail || 'Internal Error'));
            }).finally(function () {
                vm.savingAi = false;
            });
        };

        // ── Platform ───────────────────────────────────────────────────────
        vm.loadPlatform = function () {
            vm.loading = true;
            $http.get('/api/status').then(function (res) {
                vm.platform = res.data || {};
            }).catch(function () {
                vm.platform = { version: 'NeuralVyuha Zenith', error: 'Could not load platform info' };
            }).finally(function () { vm.loading = false; });
        };

        // ── Users ──────────────────────────────────────────────────────────
        vm.loadUsers = function () {
            vm.loading = true;
            // Show the logged-in admin user; in a full implementation this would
            // call GET /api/v1/user/_list or similar
            $http.get('/api/v1/user/current', {
                headers: { Authorization: 'Bearer ' + localStorage.getItem('nv_token') }
            }).then(function (res) {
                var u = res.data || {};
                vm.users = [{
                    login: u.login || 'admin@neuralvyuha.local',
                    name: u.name || 'Administrator',
                    organisation: u.organisation || 'admin',
                    roles: (u.roles || ['SYSTEM_ADMIN']).join(', '),
                    status: 'Active'
                }];
            }).catch(function () {
                vm.users = [];
            }).finally(function () { vm.loading = false; });
        };

        // ── Health ─────────────────────────────────────────────────────────
        vm.loadHealth = function () {
            vm.loading = true;
            vm.health = {};
            $http.get('/api/status').then(function (res) {
                var data = res.data || {};
                var svc = data.services || {};

                // Map backend service keys → human-readable labels
                var labels = {
                    'nv-query': { name: 'nv-query (API Gateway)', detail: 'REST + JWT Auth' },
                    'opensearch': { name: 'OpenSearch (Persistence)', detail: 'Index: nv-events' },
                    'postgres': { name: 'PostgreSQL (Vault)', detail: 'nv_vault DB' },
                    'minio': { name: 'MinIO (Evidence)', detail: 'AES-256 encrypted' },
                    'redis': { name: 'nv-socket (WebSocket)', detail: 'Redis pub/sub' },
                    'redpanda': { name: 'nv-ingest (Event Spine)', detail: 'Redpanda Kafka' },
                    'nv-ingest': { name: 'Ingest Service', detail: 'Policy & Rate Limiting' },
                    'nv-dedup': { name: 'Detective Engine', detail: 'Correlation & Scoring' },
                    'nv-correlation': { name: 'Correlation Logic', detail: 'Graph Topology' },
                    'nv-case-engine': { name: 'Case Engine', detail: 'Case & Task Lifecycle' },
                    'nv-workflow': { name: 'Workflow Engine', detail: 'Autonomous Response' },
                    'socket-service': { name: 'WebSocket Service', detail: 'Real-time Fan-out' }
                };

                var services = Object.keys(labels).map(function (key) {
                    return {
                        name: labels[key].name,
                        detail: labels[key].detail,
                        status: svc[key] || 'UNKNOWN'
                    };
                });

                var conn = data.connectors || {};
                if (conn.cortex) { services.push({ name: 'Cortex', detail: 'Analyzer Engine', status: conn.cortex.status || 'UNKNOWN' }); }
                if (conn.misp) { services.push({ name: 'MISP', detail: 'Threat Intelligence', status: conn.misp.status || 'UNKNOWN' }); }

                vm.health = { version: data.version || 'N/A', services: services, probeTime: new Date() };
            }).catch(function () {
                vm.health = { error: 'Could not reach backend' };
            }).finally(function () { vm.loading = false; });
        };

        vm.statusClass = function (s) {
            if (!s) { return 'status-unknown'; }
            var u = s.toUpperCase();
            if (u === 'UP' || u === 'OK') { return 'status-up'; }
            if (u === 'DEGRADED' || u === 'WARNING') { return 'status-degraded'; }
            if (u === 'DOWN' || u === 'ERROR') { return 'status-down'; }
            if (u === 'NOT_CONFIGURED') { return 'status-unknown'; } // Use unknown/gray for not configured
            return 'status-unknown';
        };

        // ── Lifecycle ──────────────────────────────────────────────────────
        $scope.$on('$destroy', function () {
            stopPolling();
        });

        vm.loadPlatform();

    });
})();
