(function () {
    'use strict';

    angular.module('nvServices').factory('NvApiSrv', function ($http, $q, NvConfig, NotificationSrv, AuthenticationSrv, UtilsSrv) {
        var service = {};
        var baseUrl = NvConfig.nvBaseUrl;

        // Helper to handle errors uniformly
        function handleError(err, context) {
            if (err.status === 403) {
                NotificationSrv.error('Permission Denied', 'You do not have permission to view ' + context);
                return $q.reject(err);
            }
            if (err.status === 404) {
                return $q.reject(err);
            }

            // For other errors (500, timeout), show warning but don't spam notifications if it's just connectivity
            console.warn('NeuralVyuha API Error [' + context + ']:', err);
            // NotificationSrv.error('Service Unavailable', 'NeuralVyuha services are currently unreachable.');
            // Suppress global error to allow fallback logic to proceed silently if needed
            return $q.reject(err);
        }

        function getHeaders() {
            var headers = {};
            if (AuthenticationSrv.currentUser && AuthenticationSrv.currentUser.token) {
                headers.Authorization = 'Bearer ' + AuthenticationSrv.currentUser.token;
            }
            return headers;
        }

        function transformCustomFields(item) {
            if (item && item.customFields && !angular.isArray(item.customFields)) {
                var fields = [];
                angular.forEach(item.customFields, function (fieldData, fieldName) {
                    var field = angular.copy(fieldData);
                    if (angular.isObject(field)) {
                        field.name = fieldName;
                    } else {
                        field = { name: fieldName, value: fieldData };
                    }
                    fields.push(field);
                });
                item.customFields = fields;
            }
            return item;
        }

        // --- Groups (Incidents) ---
        service.getGroups = function (params) {
            return $http.get(baseUrl + '/groups', {
                params: params,
                headers: getHeaders(),
                timeout: NvConfig.timeoutMs
            }).then(function (res) {
                return res.data;
            }).catch(function (err) {
                return handleError(err, 'Incidents List');
            });
        };

        service.getAlerts = function (params) {
            return $http.get(baseUrl + '/alerts', {
                params: params,
                headers: getHeaders(),
                timeout: NvConfig.timeoutMs
            }).then(function (res) {
                return res.data;
            }).catch(function (err) {
                return handleError(err, 'Alerts List');
            });
        };

        service.updateAlertStatus = function (alertId, updates) {
            return $http.patch(baseUrl + '/alerts/' + alertId, updates, {
                headers: getHeaders(),
                timeout: NvConfig.timeoutMs
            }).then(function (res) {
                return res.data;
            }).catch(function (err) {
                return handleError(err, 'Update Alert');
            });
        };

        service.bulkUpdateAlertStatus = function (ids, updates) {
            var payload = angular.extend({ ids: ids }, updates);
            return $http.post(baseUrl + '/alerts/bulk/status', payload, {
                headers: getHeaders(),
                timeout: NvConfig.timeoutMs
            }).then(function (res) {
                return res.data;
            }).catch(function (err) {
                return handleError(err, 'Bulk Update Alerts');
            });
        };

        service.deleteAlert = function (alertId) {
            return $http.delete(baseUrl + '/alerts/' + alertId, {
                headers: getHeaders(),
                timeout: NvConfig.timeoutMs
            }).then(function (res) {
                return res.data;
            }).catch(function (err) {
                return handleError(err, 'Delete Alert');
            });
        };

        service.promoteAlerts = function (alertIds, title, templateId) {
            var headers = getHeaders();
            headers['Idempotency-Key'] = UtilsSrv.guid();
            return $http.post(baseUrl + '/alerts/promote', {
                alert_ids: alertIds,
                title: title || null,
                template_id: templateId || null
            }, {
                headers: headers,
                timeout: NvConfig.timeoutMs
            }).then(function (res) {
                return res.data;
            }).catch(function (err) {
                return handleError(err, 'Promote Alerts');
            });
        };

        service.getSimilarAlerts = function (alertId) {
            return $http.get(baseUrl + '/alerts/' + alertId + '/similar', {
                headers: getHeaders(),
                timeout: NvConfig.timeoutMs
            }).then(function (res) {
                return res.data;
            }).catch(function (err) {
                return handleError(err, 'Similar Alerts');
            });
        };

        service.getAlertIocs = function (alertId) {
            return $http.get(baseUrl + '/alerts/' + alertId + '/iocs', {
                headers: getHeaders(),
                timeout: NvConfig.timeoutMs
            }).then(function (res) {
                return res.data;
            }).catch(function (err) {
                return handleError(err, 'Alert IOCs');
            });
        };

        service.getAlertStats = function () {
            return $http.get(baseUrl + '/alerts/stats', {
                headers: getHeaders(),
                timeout: NvConfig.timeoutMs
            }).then(function (res) {
                return res.data;
            }).catch(function (err) {
                return handleError(err, 'Alert Stats');
            });
        };

        service.getAlertTimeline = function (params) {
            return $http.get(baseUrl + '/alerts/timeline', {
                params: params,
                headers: getHeaders(),
                timeout: NvConfig.timeoutMs
            }).then(function (res) {
                return res.data;
            }).catch(function (err) {
                return handleError(err, 'Alert Timeline');
            });
        };

        service.correlateAlerts = function () {
            return $http.post(baseUrl + '/alerts/correlate', {}, {
                headers: getHeaders(),
                timeout: NvConfig.timeoutMs * 5 
            }).then(function (res) {
                return res.data;
            }).catch(function (err) {
                return handleError(err, 'AI Alert Correlation');
            });
        };

        service.commitCluster = function (cluster) {
            return $http.post(baseUrl + '/alerts/clusters/commit', cluster, {
                headers: getHeaders(),
                timeout: NvConfig.timeoutMs
            }).then(function (res) {
                return res.data;
            }).catch(function (err) {
                return handleError(err, 'Commit Correlated Cluster');
            });
        };

        // --- Ingestion Control Hub ---
        service.getIngestionPolicies = function () {
            return $http.get(baseUrl + '/ingest/policies', {
                headers: getHeaders(),
                timeout: NvConfig.timeoutMs
            }).then(function (res) {
                return res.data;
            }).catch(function (err) {
                return handleError(err, 'Ingestion Policies');
            });
        };

        service.updateIngestionPolicy = function (source, updates) {
            return $http.patch(baseUrl + '/ingest/policies/' + source, updates, {
                headers: getHeaders(),
                timeout: NvConfig.timeoutMs
            }).then(function (res) {
                return res.data;
            }).catch(function (err) {
                return handleError(err, 'Update Ingestion Policy');
            });
        };

        // --- Engine Control (Command Center) ---
        service.getEngineStatus = function () {
            return $http.get(baseUrl + '/ingest/engine/status', {
                headers: getHeaders(), timeout: 5000
            }).then(function (res) { return res.data; }).catch(function (err) { return handleError(err, 'Engine Status'); });
        };

        service.pauseEngine = function () {
            return $http.post(baseUrl + '/ingest/engine/pause', {}, {
                headers: getHeaders(), timeout: 5000
            }).then(function (res) { return res.data; }).catch(function (err) { return handleError(err, 'Pause Engine'); });
        };

        service.resumeEngine = function () {
            return $http.post(baseUrl + '/ingest/engine/resume', {}, {
                headers: getHeaders(), timeout: 5000
            }).then(function (res) { return res.data; }).catch(function (err) { return handleError(err, 'Resume Engine'); });
        };

        service.toggleDryRun = function (enabled) {
            return $http.post(baseUrl + '/ingest/engine/dry-run', { enabled: enabled }, {
                headers: getHeaders(), timeout: 5000
            }).then(function (res) { return res.data; }).catch(function (err) { return handleError(err, 'Toggle Dry Run'); });
        };

        service.setLogLevel = function (level) {
            return $http.post(baseUrl + '/ingest/engine/log-level', { level: level }, {
                headers: getHeaders(), timeout: 5000
            }).then(function (res) { return res.data; }).catch(function (err) { return handleError(err, 'Set Log Level'); });
        };

        service.toggleCircuitBreaker = function (tripped) {
            return $http.post(baseUrl + '/ingest/engine/circuit-breaker', { tripped: tripped }, {
                headers: getHeaders(), timeout: 5000
            }).then(function (res) { return res.data; }).catch(function (err) { return handleError(err, 'Circuit Breaker'); });
        };

        service.getEngineMetrics = function () {
            return $http.get(baseUrl + '/ingest/engine/metrics', {
                headers: getHeaders(), timeout: 5000
            }).then(function (res) { return res.data; }).catch(function (err) { return handleError(err, 'Engine Metrics'); });
        };

        service.getLiveStream = function (limit) {
            var count = limit || 50;
            return $http.get(baseUrl + '/ingest/engine/live-stream?count=' + count, {
                headers: getHeaders(), timeout: 5000
            }).then(function (res) { return res.data; }).catch(function (err) { return handleError(err, 'Live Stream'); });
        };

        // --- Drop Rules ---
        service.getDropRules = function () {
            return $http.get(baseUrl + '/ingest/drop-rules', {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; }).catch(function (err) { return handleError(err, 'Drop Rules'); });
        };

        service.addDropRule = function (rule) {
            return $http.post(baseUrl + '/ingest/drop-rules', rule, {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; }).catch(function (err) { return handleError(err, 'Add Drop Rule'); });
        };

        service.deleteDropRule = function (ruleId) {
            return $http.delete(baseUrl + '/ingest/drop-rules/' + ruleId, {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; }).catch(function (err) { return handleError(err, 'Delete Drop Rule'); });
        };

        service.toggleDropRule = function (ruleId, active) {
            return $http.patch(baseUrl + '/ingest/drop-rules/' + ruleId, { active: active }, {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; }).catch(function (err) { return handleError(err, 'Toggle Drop Rule'); });
        };

        service.getGroup = function (id) {
            return $http.get(baseUrl + '/groups/' + id, {
                headers: getHeaders(),
                timeout: NvConfig.timeoutMs
            }).then(function (res) {
                return res.data;
            }).catch(function (err) {
                return handleError(err, 'Incident Detail');
            });
        };

        service.getGroupAlerts = function (id) {
            return $http.get(baseUrl + '/groups/' + id + '/alerts', {
                headers: getHeaders(),
                timeout: NvConfig.timeoutMs
            }).then(function (res) {
                return res.data;
            }).catch(function (err) {
                return handleError(err, 'Incident Timeline');
            });
        };

        // --- Cases (Read-Path Migration) ---
        service.getCases = function (params) {
            return $http.get(baseUrl + '/cases', {
                params: params,
                headers: getHeaders(),
                timeout: NvConfig.timeoutMs
            }).then(function (res) {
                if (angular.isArray(res.data)) {
                    angular.forEach(res.data, transformCustomFields);
                }
                return res.data;
            }).catch(function (err) {
                return handleError(err, 'Case List');
            });
        };

        service.getCase = function (id) {
            return $http.get(baseUrl + '/cases/' + id, {
                headers: getHeaders(),
                timeout: NvConfig.timeoutMs
            }).then(function (res) {
                var data = res.data;
                // Mark as NV source for UI badges
                if (data) {
                    data._source = 'NV';
                    transformCustomFields(data);
                }
                return data;
            }).catch(function (err) {
                return handleError(err, 'Case Detail');
            });
        };

        service.getCaseTimeline = function (id) {
            return $http.get(baseUrl + '/cases/' + id + '/timeline', {
                headers: getHeaders(),
                timeout: NvConfig.timeoutMs
            }).then(function (res) {
                return res.data;
            }).catch(function (err) {
                return handleError(err, 'Case Timeline');
            });
        };

        // --- Write Operations (Phase E6.4) ---
        service.createCase = function (caze) {
            var headers = getHeaders();
            headers['Idempotency-Key'] = UtilsSrv.guid(); // Assuming UtilsSrv exists or I need to implement UUID helper

            return $http.post(baseUrl + '/cases', caze, {
                headers: headers,
                timeout: NvConfig.timeoutMs
            }).then(function (res) {
                return res.data;
            }).catch(function (err) {
                return handleError(err, 'Create Case');
            });
        };

        service.updateCase = function (id, updates) {
            var headers = getHeaders();
            // Idempotency key for update? Maybe optional but good practice.
            headers['Idempotency-Key'] = UtilsSrv.guid();

            return $http.patch(baseUrl + '/cases/' + id, updates, {
                headers: headers,
                timeout: NvConfig.timeoutMs
            }).then(function (res) {
                return res.data;
            }).catch(function (err) {
                return handleError(err, 'Update Case');
            });
        };

        service.linkCase = function (caseId, targetCaseId) {
            var headers = getHeaders();
            headers['Idempotency-Key'] = UtilsSrv.guid();

            return $http.post(baseUrl + '/cases/' + caseId + '/links', { target_case_id: targetCaseId }, {
                headers: headers,
                timeout: NvConfig.timeoutMs
            }).then(function (res) {
                return res.data;
            }).catch(function (err) {
                return handleError(err, 'Link Case');
            });
        };

        service.getCaseLinks = function (id) {
            return $http.get(baseUrl + '/cases/' + id + '/links', {
                headers: getHeaders(),
                timeout: NvConfig.timeoutMs
            }).then(function (res) {
                return res.data;
            }).catch(function (err) {
                return handleError(err, 'Case Links');
            });
        };

        service.createTask = function (caseId, task) {
            var headers = getHeaders();
            headers['Idempotency-Key'] = UtilsSrv.guid();

            return $http.post(baseUrl + '/case/' + caseId + '/task', task, {
                headers: headers,
                timeout: NvConfig.timeoutMs
            }).then(function (res) {
                return res.data;
            }).catch(function (err) {
                return handleError(err, 'Create Task');
            });
        };

        service.createTaskLog = function (taskId, log) {
            var headers = getHeaders();
            headers['Idempotency-Key'] = UtilsSrv.guid();

            return $http.post(baseUrl + '/tasks/' + taskId + '/logs', log, {
                headers: headers,
                timeout: NvConfig.timeoutMs
            }).then(function (res) {
                return res.data;
            }).catch(function (err) {
                return handleError(err, 'Create Task Log');
            });
        };

        // --- Evidence / Artifact Operations (Phase Z1.1) ---
        var artifactBaseUrl = NvConfig.nvArtifactBaseUrl || (baseUrl.replace('/api', '') + '/artifacts');

        service.getArtifacts = function (caseId) {
            return $http.get(artifactBaseUrl + '/case/' + caseId, {
                headers: getHeaders(),
                timeout: NvConfig.timeoutMs
            }).then(function (res) {
                return res.data;
            }).catch(function (err) {
                return handleError(err, 'Evidence List');
            });
        };

        service.uploadArtifact = function (caseId, file) {
            var fd = new FormData();
            fd.append('case_id', caseId);
            fd.append('file', file, file.name);

            var headers = getHeaders();
            headers['Content-Type'] = undefined; // Let browser set multipart boundary

            return $http.post(artifactBaseUrl + '/upload', fd, {
                headers: headers,
                transformRequest: angular.identity,
                timeout: NvConfig.timeoutMs * 5  // Allow longer for large files
            }).then(function (res) {
                return res.data;
            }).catch(function (err) {
                return handleError(err, 'Artifact Upload');
            });
        };

        service.getArtifactDownloadUrl = function (artifactId) {
            return $http.get(artifactBaseUrl + '/' + artifactId + '/download', {
                headers: getHeaders(),
                timeout: NvConfig.timeoutMs
            }).then(function (res) {
                return res.data;
            }).catch(function (err) {
                return handleError(err, 'Artifact Download');
            });
        };

        service.getSightings = function (sha256) {
            return $http.get(artifactBaseUrl + '/sightings/' + sha256, {
                headers: getHeaders(),
                timeout: NvConfig.timeoutMs
            }).then(function (res) {
                return res.data;
            }).catch(function (err) {
                return handleError(err, 'Sightings Lookup');
            });
        };

        // --- Graph (Phase E8.1 Visual Vyuha) ---
        service.getCaseGraph = function (caseId) {
            return $http.get(baseUrl + '/graph/case/' + encodeURIComponent(caseId), {
                headers: getHeaders(),
                timeout: NvConfig.timeoutMs
            }).then(function (res) {
                return res.data;
            }).catch(function (err) {
                return handleError(err, 'Case Graph');
            });
        };

        // Integration Hub — Node Service (Phase Z2.1)
        // Routed via Caddy: /node-service/* → nv-node-service:8085
        var nodeBaseUrl = '/node-service';

        service.getNodes = function (nodeType) {
            var params = nodeType ? '?node_type=' + nodeType : '';
            return $http.get(nodeBaseUrl + '/nodes' + params, {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; });
        };

        service.registerNode = function (data) {
            return $http.post(nodeBaseUrl + '/nodes', data, {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; });
        };

        service.updateNode = function (nodeId, patch) {
            return $http.patch(nodeBaseUrl + '/nodes/' + nodeId, patch, {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; });
        };

        service.testNode = function (nodeId) {
            return $http.post(nodeBaseUrl + '/nodes/' + nodeId + '/test', {}, {
                headers: getHeaders(), timeout: 20000  // Allow 20s for slow nodes
            }).then(function (res) { return res.data; });
        };

        // --- Health Check (Enterprise Dashboard) ---
        service.healthCheck = function () {
            return $http.get(baseUrl + '/status', {
                headers: getHeaders(),
                timeout: 5000
            }).then(function (res) {
                return res.data;
            }).catch(function () {
                return null;
            });
        };

        service.deleteNode = function (nodeId) {
            return $http.delete(nodeBaseUrl + '/nodes/' + nodeId, {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; });
        };

        // --- AI Case Investigations ---
        service.saveCaseInvestigation = function (caseId, data) {
            return $http.post(nodeBaseUrl + '/cases/' + caseId + '/investigations', data, {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; });
        };

        service.getCaseInvestigations = function (caseId) {
            return $http.get(nodeBaseUrl + '/cases/' + caseId + '/investigations', {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; });
        };

        service.rateInvestigation = function(invId, rating, comment) {
            return $http.put(nodeBaseUrl + '/internal/investigation/' + invId + '/feedback', {
                rating: rating,
                feedback_comment: comment
            }, {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; });
        };

        // --- Tasks ---
        service.getTasks = function (caseId) {
            return $http.get(baseUrl + '/case/' + caseId + '/task', {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; }).catch(function (err) { return handleError(err, 'Tasks List'); });
        };

        service.getTask = function (taskId) {
            return $http.get(baseUrl + '/tasks/' + taskId, {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; }).catch(function (err) { return handleError(err, 'Task Detail'); });
        };

        service.getTaskLogs = function (taskId) {
            return $http.get(baseUrl + '/tasks/' + taskId + '/logs', {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; }).catch(function (err) { return handleError(err, 'Task Logs'); });
        };

        service.deleteTask = function (caseId, taskId) {
            return $http.delete(baseUrl + '/case/' + caseId + '/task/' + taskId, {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; }).catch(function (err) { return handleError(err, 'Delete Task'); });
        };

        service.createTask = function (caseId, data) {
            return $http.post(baseUrl + '/case/' + caseId + '/task', data, {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; }).catch(function (err) { return handleError(err, 'Create Task'); });
        };

        service.addTaskLog = function (caseId, taskId, data) {
            return $http.post(baseUrl + '/case/' + caseId + '/task/' + taskId + '/log', data, {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; }).catch(function (err) { return handleError(err, 'Add Task Log'); });
        };

        service.updateTask = function (caseId, taskId, data) {
            return $http.patch(baseUrl + '/case/' + caseId + '/task/' + taskId, data, {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; }).catch(function (err) { return handleError(err, 'Update Task'); });
        };

        // --- Observables ---
        service.getObservables = function (caseId) {
            return $http.get(baseUrl + '/case/' + caseId + '/observable', {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; }).catch(function (err) { return handleError(err, 'Observables List'); });
        };

        service.createObservable = function (caseId, data) {
            return $http.post(baseUrl + '/case/' + caseId + '/observable', data, {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; }).catch(function (err) { return handleError(err, 'Create Observable'); });
        };

        service.deleteObservable = function (caseId, obsId) {
            return $http.delete(baseUrl + '/case/' + caseId + '/observable/' + obsId, {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; }).catch(function (err) { return handleError(err, 'Delete Observable'); });
        };

        service.updateObservable = function (caseId, obsId, data) {
            return $http.patch(baseUrl + '/case/' + caseId + '/observable/' + obsId, data, {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; }).catch(function (err) { return handleError(err, 'Update Observable'); });
        };

        // --- TTPs ---
        service.getTtps = function (caseId) {
            return $http.get(baseUrl + '/case/' + caseId + '/ttp', {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; }).catch(function (err) { return handleError(err, 'TTPs List'); });
        };

        service.addTtp = function (caseId, data) {
            return $http.post(baseUrl + '/case/' + caseId + '/ttp', data, {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; }).catch(function (err) { return handleError(err, 'Add TTP'); });
        };

        service.removeTtp = function (caseId, ttpId) {
            return $http.delete(baseUrl + '/case/' + caseId + '/ttp/' + ttpId, {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; }).catch(function (err) { return handleError(err, 'Remove TTP'); });
        };

        service.getPages = function (caseId) {
            return $http.get(baseUrl + '/case/' + caseId + '/page', {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; }).catch(function (err) { return handleError(err, 'Pages List'); });
        };

        service.getPage = function (caseId, pageId) {
            return $http.get(baseUrl + '/case/' + caseId + '/page/' + pageId, {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; }).catch(function (err) { return handleError(err, 'Page Detail'); });
        };

        service.createPage = function (caseId, data) {
            return $http.post(baseUrl + '/case/' + caseId + '/page', data, {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; }).catch(function (err) { return handleError(err, 'Create Page'); });
        };

        service.updatePage = function (caseId, pageId, data) {
            return $http.patch(baseUrl + '/case/' + caseId + '/page/' + pageId, data, {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; }).catch(function (err) { return handleError(err, 'Update Page'); });
        };

        service.deletePage = function (caseId, pageId) {
            return $http.delete(baseUrl + '/case/' + caseId + '/page/' + pageId, {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; }).catch(function (err) { return handleError(err, 'Delete Page'); });
        };

        // Deduplication (Tier 2 Architect)
        service.getDedupConfigs = function () {
            return $http.get(baseUrl + '/ingest/dedup-configs', {
                headers: getHeaders(),
                timeout: NvConfig.timeoutMs
            }).then(function (res) {
                return res.data;
            }).catch(function (err) {
                return handleError(err, 'Dedup Configs');
            });
        };

        service.createDedupConfig = function (config) {
            return $http.post(baseUrl + '/ingest/dedup-configs', config, {
                headers: getHeaders(),
                timeout: NvConfig.timeoutMs
            }).then(function (res) {
                return res.data;
            }).catch(function (err) {
                return handleError(err, 'Create Dedup Config');
            });
        };

        service.deleteDedupConfig = function (id) {
            return $http.delete(baseUrl + '/ingest/dedup-configs/' + id, {
                headers: getHeaders(),
                timeout: NvConfig.timeoutMs
            }).then(function (res) {
                return res.data;
            }).catch(function (err) {
                return handleError(err, 'Delete Dedup Config');
            });
        };

        // ── Tier 3: The Detective — Correlation Rules (Cross-Source) ──────────

        service.getCorrelationRules = function () {
            return $http.get(baseUrl + '/ingest/correlation-rules', {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; })
                .catch(function (err) { return handleError(err, 'Correlation Rules'); });
        };

        service.createCorrelationRule = function (rule) {
            return $http.post(baseUrl + '/ingest/correlation-rules', rule, {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; })
                .catch(function (err) { return handleError(err, 'Create Correlation Rule'); });
        };

        service.toggleCorrelationRule = function (id, active) {
            return $http.patch(baseUrl + '/ingest/correlation-rules/' + id, { active: active }, {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; })
                .catch(function (err) { return handleError(err, 'Toggle Correlation Rule'); });
        };

        service.deleteCorrelationRule = function (id) {
            return $http.delete(baseUrl + '/ingest/correlation-rules/' + id, {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; })
                .catch(function (err) { return handleError(err, 'Delete Correlation Rule'); });
        };

        service.getCorrelationIncidents = function (limit) {
            var params = limit ? '?limit=' + limit : '';
            return $http.get(baseUrl + '/ingest/correlation-incidents' + params, {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; })
                .catch(function (err) { return handleError(err, 'Correlation Incidents'); });
        };

        // ── Tier 5: The Hunter — Threat Hunting API ──────────────────────

        service.huntSearch = function (params) {
            var qs = '?q=' + encodeURIComponent(params.q || '*')
                + '&size=' + (params.size || 25)
                + '&from=' + (params.from || 0)
                + '&sort_field=' + (params.sort_field || 'timestamp')
                + '&sort_order=' + (params.sort_order || 'desc');
            if (params.time_from) qs += '&time_from=' + params.time_from;
            if (params.time_to) qs += '&time_to=' + params.time_to;
            if (params.source) qs += '&source=' + encodeURIComponent(params.source);
            return $http.get(baseUrl + '/hunt/search' + qs, {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; })
                .catch(function (err) { return handleError(err, 'Hunt Search'); });
        };

        service.huntHistogram = function (params) {
            var qs = '?q=' + encodeURIComponent(params.q || '*')
                + '&interval=' + (params.interval || 'auto');
            if (params.time_from) qs += '&time_from=' + params.time_from;
            if (params.time_to) qs += '&time_to=' + params.time_to;
            return $http.get(baseUrl + '/hunt/histogram' + qs, {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; })
                .catch(function (err) { return handleError(err, 'Hunt Histogram'); });
        };

        service.huntFields = function (params) {
            var qs = '?field=' + encodeURIComponent(params.field)
                + '&q=' + encodeURIComponent(params.q || '*')
                + '&top_n=' + (params.top_n || 10);
            if (params.time_from) qs += '&time_from=' + params.time_from;
            if (params.time_to) qs += '&time_to=' + params.time_to;
            return $http.get(baseUrl + '/hunt/fields' + qs, {
                headers: getHeaders(), timeout: NvConfig.timeoutMs
            }).then(function (res) { return res.data; })
                .catch(function (err) { return handleError(err, 'Hunt Fields'); });
        };

        return service;
    });
})();
