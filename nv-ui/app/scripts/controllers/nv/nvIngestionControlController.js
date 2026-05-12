/**
 * NeuralVyuha Ingest Command Center — NASA-Grade Engine Controller
 * Controls: Master Switch, Circuit Breaker, Dry-Run, Log Level,
 *           Source Policies, Drop Rules, Live Stream HUD, Telemetry.
 */
(function () {
    'use strict';

    angular.module('nvControllers').controller('NvIngestionControlCtrl', function ($scope, $timeout, $interval, NvApiSrv, NotificationSrv) {
        var vm = this;

        // ═══════════════════════════════════════════════════════════════════
        // State
        // ═══════════════════════════════════════════════════════════════════
        vm.loading = true;
        vm.activeTab = 'dashboard'; // dashboard | sources | rules

        // Engine state
        vm.engine = {
            paused: false,
            dry_run: false,
            log_level: 'INFO',
            circuit_breaker: false,
            uptime_human: '0h 0m',
            kafka_connected: false,
            redis_connected: false,
            postgres_connected: false,
            minio_connected: false
        };

        // Telemetry
        vm.metrics = {
            eps: 0,
            events_total: 0,
            events_dropped: 0,
            events_archived: 0,
            events_promoted: 0,
            events_behavioral_triggered: 0,
            events_rate_limited: 0,
            events_dry_run: 0,
            cpu_percent: 0,
            memory_mb: 0,
            uptime_seconds: 0
        };

        // Source policies
        vm.policies = [];
        vm.stats = { totalEps: 0, activeSources: 0 };

        // Live stream
        vm.liveStream = [];

        // Drop rules
        vm.dropRules = [];
        vm.newRule = { field_path: 'rule.level', operator: 'eq', value: '', action: 'DROP', priority: 0, severity_override: '', description: '' };

        // Dedup configs
        vm.dedupConfigs = [];
        vm.newDedup = { source: '', fields_str: '', window: 3600 };

        // Confirmation modal
        vm.confirmAction = null;
        vm.confirmMessage = '';
        vm.showConfirm = false;

        // Log levels
        vm.logLevels = ['DEBUG', 'INFO', 'WARNING', 'ERROR'];

        // Polling handles
        var metricsPoller = null;
        var streamPoller = null;

        // ═══════════════════════════════════════════════════════════════════
        // Initialization
        // ═══════════════════════════════════════════════════════════════════
        vm.init = function () {
            vm.loading = true;
            vm.loadAll();

            // Start pollers
            metricsPoller = $interval(vm.pollTelemetry, 3000);
            streamPoller = $interval(vm.pollLiveStream, 2000);

            // Cleanup on destroy
            $scope.$on('$destroy', function () {
                if (metricsPoller) $interval.cancel(metricsPoller);
                if (streamPoller) $interval.cancel(streamPoller);
            });
        };

        vm.loadAll = function () {
            NvApiSrv.getEngineStatus().then(function (data) {
                if (data) angular.extend(vm.engine, data);
            });
            NvApiSrv.getEngineMetrics().then(function (data) {
                if (data) angular.extend(vm.metrics, data);
            });
            NvApiSrv.getIngestionPolicies().then(function (data) {
                vm.policies = data || [];
                vm.updateStats();
            });
            NvApiSrv.getDropRules().then(function (data) {
                vm.dropRules = data || [];
            });
            NvApiSrv.getLiveStream().then(function (data) {
                vm.liveStream = (data && data.events) || [];
            });
            vm.loading = false;
        };

        // ═══════════════════════════════════════════════════════════════════
        // Telemetry Polling
        // ═══════════════════════════════════════════════════════════════════
        vm.pollTelemetry = function () {
            NvApiSrv.getEngineStatus().then(function (data) {
                if (data) angular.extend(vm.engine, data);
            });
            NvApiSrv.getEngineMetrics().then(function (data) {
                if (data) angular.extend(vm.metrics, data);
            });
        };

        vm.pollLiveStream = function () {
            NvApiSrv.getLiveStream().then(function (data) {
                if (data && data.events) vm.liveStream = data.events;
            });
        };

        // ═══════════════════════════════════════════════════════════════════
        // ENGINE CONTROLS
        // ═══════════════════════════════════════════════════════════════════

        // Master Switch with confirmation
        vm.toggleMasterSwitch = function () {
            if (!vm.engine.paused) {
                vm.requestConfirm('HALT ALL INGESTION? This will stop all data intake immediately.', function () {
                    NvApiSrv.pauseEngine().then(function () {
                        vm.engine.paused = true;
                        NotificationSrv.success('Engine Paused', 'All ingestion halted');
                    });
                });
            } else {
                NvApiSrv.resumeEngine().then(function () {
                    vm.engine.paused = false;
                    NotificationSrv.success('Engine Resumed', 'Ingestion is active');
                });
            }
        };

        // Circuit Breaker with confirmation
        vm.toggleCircuitBreaker = function () {
            var newState = !vm.engine.circuit_breaker;
            if (newState) {
                vm.requestConfirm('TRIP CIRCUIT BREAKER? This will block all downstream writes.', function () {
                    NvApiSrv.toggleCircuitBreaker(true).then(function () {
                        vm.engine.circuit_breaker = true;
                        NotificationSrv.success('Circuit Breaker', 'Tripped — downstream protected');
                    });
                });
            } else {
                NvApiSrv.toggleCircuitBreaker(false).then(function () {
                    vm.engine.circuit_breaker = false;
                    NotificationSrv.success('Circuit Breaker', 'Reset — normal flow');
                });
            }
        };

        // Dry Run toggle
        vm.toggleDryRun = function () {
            var newState = !vm.engine.dry_run;
            NvApiSrv.toggleDryRun(newState).then(function () {
                vm.engine.dry_run = newState;
                NotificationSrv.success('Dry Run', newState ? 'Enabled — events will NOT be saved' : 'Disabled — events are live');
            });
        };

        // Log Level
        vm.changeLogLevel = function (level) {
            NvApiSrv.setLogLevel(level).then(function () {
                vm.engine.log_level = level;
                NotificationSrv.success('Log Level', 'Changed to ' + level);
            });
        };

        // ═══════════════════════════════════════════════════════════════════
        // SOURCE CONTROLS
        // ═══════════════════════════════════════════════════════════════════
        vm.toggleSource = function (policy) {
            var original = policy.enabled;
            policy.enabled = !policy.enabled;
            NvApiSrv.updateIngestionPolicy(policy.source, { is_enabled: policy.enabled })
                .then(function () {
                    vm.updateStats();
                    NotificationSrv.success('Source Updated', policy.source + ' is now ' + (policy.enabled ? 'ACTIVE' : 'BLOCKED'));
                })
                .catch(function () {
                    policy.enabled = original;
                });
        };

        vm.updateLimit = function (policy) {
            NvApiSrv.updateIngestionPolicy(policy.source, {
                rate_limit_eps: parseInt(policy.eps),
                max_payload_kb: parseInt(policy.maxPayload)
            }).then(function () {
                vm.updateStats();
            });
        };

        vm.updateStats = function () {
            vm.stats.totalEps = 0;
            vm.stats.activeSources = 0;
            vm.policies.forEach(function (p) {
                if (p.enabled) {
                    vm.stats.totalEps += (p.eps || 0);
                    vm.stats.activeSources++;
                }
            });
        };

        // ═══════════════════════════════════════════════════════════════════
        // DROP RULES
        // ═══════════════════════════════════════════════════════════════════
        vm.addDropRule = function () {
            if (!vm.newRule.value) return;
            NvApiSrv.addDropRule(vm.newRule).then(function () {
                NotificationSrv.success('Action Rule Added', 'Rule will take effect immediately');
                vm.newRule = { field_path: 'rule.level', operator: 'eq', value: '', action: 'DROP', priority: 0, severity_override: '', description: '' };
                vm.loadDropRules();
            });
        };

        vm.deleteDropRule = function (rule) {
            vm.requestConfirm('Delete rule: ' + rule.field + ' ' + rule.operator + ' "' + rule.value + '"?', function () {
                NvApiSrv.deleteDropRule(rule.id).then(function () {
                    NotificationSrv.success('Drop Rule Deleted', 'Rule removed');
                    vm.loadDropRules();
                });
            });
        };

        vm.toggleDropRule = function (rule) {
            NvApiSrv.toggleDropRule(rule.id, !rule.active).then(function () {
                rule.active = !rule.active;
            });
        };

        vm.loadDropRules = function () {
            NvApiSrv.getDropRules().then(function (data) {
                vm.dropRules = data || [];
            });
            NvApiSrv.getDedupConfigs().then(function (data) {
                vm.dedupConfigs = data || [];
            });
        };

        // ═══════════════════════════════════════════════════════════════════
        // HELPERS
        // ═══════════════════════════════════════════════════════════════════
        vm.requestConfirm = function (message, action) {
            vm.confirmMessage = message;
            vm.confirmAction = action;
            vm.showConfirm = true;
        };

        vm.executeConfirm = function () {
            if (vm.confirmAction) vm.confirmAction();
            vm.showConfirm = false;
            vm.confirmAction = null;
        };

        vm.cancelConfirm = function () {
            vm.showConfirm = false;
            vm.confirmAction = null;
        };

        vm.getSourceIcon = function (source) {
            var icons = {
                'syslog': 'fa-terminal', 'misp': 'fa-share-alt', 'sentinel': 'fa-shield',
                'webhook': 'fa-bolt', 'sentinel-one': 'fa-crosshairs', 'crowdstrike': 'fa-dot-circle-o',
                'elastic': 'fa-database', 'splunk': 'fa-bar-chart', 'wazuh': 'fa-eye'
            };
            return icons[(source || '').toLowerCase()] || 'fa-plug';
        };

        vm.getSeverityClass = function (sev) {
            return { 1: 'info', 2: 'low', 3: 'medium', 4: 'high' }[sev] || 'info';
        };

        vm.getStatusClass = function (status) {
            return {
                'INGESTED': 'ingested', 'DRY-RUN': 'dryrun', 'DROPPED': 'dropped',
                'ARCHIVED': 'archived', 'PROMOTED': 'promoted', 'BEHAVIORAL_ALERT': 'behavioral',
                'DEDUPED': 'deduped'
            }[status] || '';
        };

        vm.formatUptime = function (seconds) {
            if (!seconds) return '0m';
            var h = Math.floor(seconds / 3600);
            var m = Math.floor((seconds % 3600) / 60);
            return h > 0 ? h + 'h ' + m + 'm' : m + 'm';
        };

        // ── Deduplication Management ────────────────────────────────────

        vm.loadDedupConfigs = function () {
            NvApiSrv.getDedupConfigs().then(function (data) {
                vm.dedupConfigs = data || [];
            });
        };

        vm.addDedupConfig = function () {
            if (!vm.newDedup.source || !vm.newDedup.fields_str) {
                NotificationSrv.error('Invalid Data', 'Source and Fields are required.');
                return;
            }

            var payload = {
                source: vm.newDedup.source.toLowerCase(),
                field_paths: vm.newDedup.fields_str.split(',').map(function (s) { return s.trim(); }),
                window_seconds: vm.newDedup.window || 3600
            };

            NvApiSrv.createDedupConfig(payload).then(function () {
                NotificationSrv.success('Deduplication Saved', 'Fingerprint rule for ' + payload.source + ' is now active.');
                vm.newDedup = { source: '', fields_str: '', window: 3600 };
                vm.loadDedupConfigs();
            });
        };

        vm.deleteDedupConfig = function (id) {
            vm.confirmMessage = 'Stop deduplication for this source? This may increase noise.';
            vm.confirmAction = function () {
                NvApiSrv.deleteDedupConfig(id).then(function () {
                    NotificationSrv.success('Deduplication Disabled', 'Noise gating removed.');
                    vm.loadDedupConfigs();
                    vm.showConfirm = false;
                });
            };
            vm.showConfirm = true;
        };

        // ── Tier 3: The Detective — Correlation Rules ──────────────────────

        vm.corrRules = [];
        vm.corrIncidents = [];
        vm.newCorr = {
            name: '', description: '', source_a: 'wazuh', source_b: 'suricata',
            match_field: 'data.srcip', time_window: 300,
            min_count_a: 1, min_count_b: 1,
            action_type: 'CREATE_CASE', severity_override: 3, mitre_tactic: 'Unknown'
        };

        vm.loadCorrelationData = function () {
            NvApiSrv.getCorrelationRules().then(function (data) {
                vm.corrRules = data || [];
            });
            NvApiSrv.getCorrelationIncidents().then(function (data) {
                vm.corrIncidents = data || [];
            });
        };

        vm.addCorrelationRule = function () {
            if (!vm.newCorr.name || !vm.newCorr.source_a || !vm.newCorr.source_b || !vm.newCorr.match_field) {
                NotificationSrv.error('Incomplete Rule', 'Name, Source A, Source B, and Match Field are all required.');
                return;
            }
            var payload = {
                name: vm.newCorr.name,
                description: vm.newCorr.description || '',
                source_a: vm.newCorr.source_a.toLowerCase(),
                source_b: vm.newCorr.source_b.toLowerCase(),
                match_field: vm.newCorr.match_field,
                time_window: parseInt(vm.newCorr.time_window) || 300,
                min_count_a: parseInt(vm.newCorr.min_count_a) || 1,
                min_count_b: parseInt(vm.newCorr.min_count_b) || 1,
                action_type: vm.newCorr.action_type || 'CREATE_CASE',
                severity_override: parseInt(vm.newCorr.severity_override) || 3,
                mitre_tactic: vm.newCorr.mitre_tactic || 'Unknown'
            };
            NvApiSrv.createCorrelationRule(payload).then(function () {
                NotificationSrv.success(
                    'Rule Created',
                    'The Detective will now watch for ' + payload.source_a + ' + ' + payload.source_b + ' correlations.'
                );
                vm.newCorr = {
                    name: '', description: '', source_a: 'wazuh', source_b: 'suricata',
                    match_field: 'data.srcip', time_window: 300,
                    min_count_a: 1, min_count_b: 1,
                    action_type: 'CREATE_CASE', severity_override: 3, mitre_tactic: 'Unknown'
                };
                vm.loadCorrelationData();
            });
        };

        vm.toggleCorrelationRule = function (rule) {
            var newState = !rule.is_active;
            NvApiSrv.toggleCorrelationRule(rule.id, newState).then(function () {
                rule.is_active = newState;
                NotificationSrv.success(
                    newState ? 'Rule Activated' : 'Rule Paused',
                    '"' + rule.name + '" is now ' + (newState ? 'active' : 'paused') + '.'
                );
            });
        };

        vm.deleteCorrelationRule = function (id) {
            var rule = vm.corrRules.find(function (r) { return r.id === id; });
            vm.confirmMessage = 'Delete correlation rule "' + (rule ? rule.name : id) + '"? This cannot be undone.';
            vm.confirmAction = function () {
                NvApiSrv.deleteCorrelationRule(id).then(function () {
                    NotificationSrv.success('Rule Deleted', 'Correlation rule removed.');
                    vm.loadCorrelationData();
                    vm.showConfirm = false;
                });
            };
            vm.showConfirm = true;
        };

        vm.init();
    });
})();
