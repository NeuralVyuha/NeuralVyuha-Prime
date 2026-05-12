(function () {
    'use strict';

    angular.module('neuralvyuha').controller('NvAlertListCtrl', function (
        $scope, $rootScope, $timeout, $q, $uibModal, NvApiSrv, NotificationSrv, nvSocketSrv
    ) {
        var vm = this;

        // ── State ──
        vm.alerts = [];
        vm.loading = true;
        vm.error = null;

        // Stats
        vm.stats = { total: 0, new: 0, updated: 0, imported: 0, ignored: 0, by_severity: {} };

        // Pagination
        vm.page = 1;
        vm.pageSize = 25;
        vm.totalAlerts = 0;
        vm.totalPages = 1;

        // Filtering
        vm.filterParams = {
            status: null,       // null = all, 'New', 'Updated', 'Imported', 'Ignored'
            severity: null,     // null = all, 1-4
            source: '',
            q: '',
            sort: '-timestamp', // default sort
            dateFrom: null,
            dateTo: null
        };
        vm.statusPresets = ['New', 'Updated', 'Imported', 'Ignored', 'FalsePositive'];
        vm.severityLevels = [
            { value: 1, label: 'Low', cls: 'nv-sev-low' },
            { value: 2, label: 'Medium', cls: 'nv-sev-medium' },
            { value: 3, label: 'High', cls: 'nv-sev-high' },
            { value: 4, label: 'Critical', cls: 'nv-sev-critical' }
        ];

        // Sort
        vm.sortField = 'timestamp';
        vm.sortDir = 'desc';
        vm.sortOptions = [
            { field: 'timestamp', label: 'Date' },
            { field: 'payload.severity', label: 'Severity' },
            { field: 'payload.source', label: 'Source' },
            { field: 'payload.type', label: 'Type' },
            { field: 'status', label: 'Status' }
        ];

        // Bulk Selection
        vm.selectedIds = {};
        vm.selectedCount = 0;
        vm.selectAllChecked = false;

        // Quick View Sidebar
        vm.sidebarOpen = false;
        vm.selectedAlert = null;
        vm.selectedIocs = null;
        vm.selectedSimilar = null;
        vm.sidebarLoading = false;
        vm.sidebarTab = 'details'; // 'details', 'iocs', 'similar', 'raw'

        // Live Stream
        vm.liveEnabled = true;
        vm.newAlertCount = 0;

        // ── Data Loading ──
        vm.loadAlerts = function () {
            vm.loading = true;
            vm.error = null;

            var params = {
                size: vm.pageSize,
                from_: (vm.page - 1) * vm.pageSize
            };

            if (vm.filterParams.status) { params.status = vm.filterParams.status; }
            if (vm.filterParams.severity) { params.severity = vm.filterParams.severity; }
            if (vm.filterParams.source) { params.source = vm.filterParams.source; }
            if (vm.filterParams.q) { params.q = vm.filterParams.q; }
            if (vm.filterParams.sort) { params.sort = vm.filterParams.sort; }

            NvApiSrv.getAlerts(params).then(function (data) {
                if (data && data.hits) {
                    vm.alerts = data.hits.map(function (alert) {
                        alert._id = alert._id || alert.event_id || alert.id;
                        alert._selected = !!vm.selectedIds[alert._id];
                        return alert;
                    });
                    vm.totalAlerts = data.total || 0;
                    vm.totalPages = Math.ceil(vm.totalAlerts / vm.pageSize) || 1;
                } else if (angular.isArray(data)) {
                    vm.alerts = data;
                    vm.totalAlerts = data.length;
                    vm.totalPages = 1;
                } else {
                    vm.alerts = [];
                    vm.totalAlerts = 0;
                    vm.totalPages = 1;
                }
                vm.loading = false;
                vm.newAlertCount = 0;
            }).catch(function (err) {
                vm.error = 'Failed to load alerts. Please try again.';
                vm.loading = false;
                vm.alerts = [];
            });
        };

        vm.loadStats = function () {
            NvApiSrv.getAlertStats().then(function (data) {
                if (data) {
                    vm.stats = data;
                }
            });
        };

        vm.refresh = function () {
            vm.loadAlerts();
            vm.loadStats();
        };

        // ── Filtering ──
        vm.setStatusFilter = function (status) {
            vm.filterParams.status = vm.filterParams.status === status ? null : status;
            vm.page = 1;
            vm.loadAlerts();
        };

        vm.setSeverityFilter = function (severity) {
            vm.filterParams.severity = vm.filterParams.severity === severity ? null : severity;
            vm.page = 1;
            vm.loadAlerts();
        };

        vm.clearFilters = function () {
            vm.filterParams = { status: null, severity: null, source: '', q: '', sort: '-timestamp', dateFrom: null, dateTo: null };
            vm.page = 1;
            vm.loadAlerts();
        };

        var searchTimeout;
        vm.onSearchChange = function () {
            if (searchTimeout) { $timeout.cancel(searchTimeout); }
            searchTimeout = $timeout(function () {
                vm.page = 1;
                vm.loadAlerts();
            }, 400);
        };

        // ── Sorting ──
        vm.sortBy = function (field) {
            if (vm.sortField === field) {
                vm.sortDir = vm.sortDir === 'asc' ? 'desc' : 'asc';
            } else {
                vm.sortField = field;
                vm.sortDir = 'desc';
            }
            vm.filterParams.sort = (vm.sortDir === 'asc' ? '+' : '-') + vm.sortField;
            vm.loadAlerts();
        };

        vm.getSortIcon = function (field) {
            if (vm.sortField !== field) { return 'fa-sort'; }
            return vm.sortDir === 'asc' ? 'fa-caret-up' : 'fa-caret-down';
        };

        // ── Pagination ──
        vm.prevPage = function () {
            if (vm.page > 1) { vm.page--; vm.loadAlerts(); }
        };

        vm.nextPage = function () {
            if (vm.page < vm.totalPages) { vm.page++; vm.loadAlerts(); }
        };

        vm.goToPage = function (p) {
            if (p >= 1 && p <= vm.totalPages) { vm.page = p; vm.loadAlerts(); }
        };

        // ── Bulk Selection ──
        vm.toggleSelect = function (alert) {
            var id = alert._id || alert.event_id;
            if (vm.selectedIds[id]) {
                delete vm.selectedIds[id];
            } else {
                vm.selectedIds[id] = true;
            }
            alert._selected = !!vm.selectedIds[id];
            vm.selectedCount = Object.keys(vm.selectedIds).length;
            vm.selectAllChecked = vm.selectedCount === vm.alerts.length && vm.alerts.length > 0;
        };

        vm.toggleSelectAll = function () {
            vm.selectAllChecked = !vm.selectAllChecked;
            vm.alerts.forEach(function (alert) {
                var id = alert._id || alert.event_id;
                if (vm.selectAllChecked) {
                    vm.selectedIds[id] = true;
                } else {
                    delete vm.selectedIds[id];
                }
                alert._selected = vm.selectAllChecked;
            });
            vm.selectedCount = Object.keys(vm.selectedIds).length;
        };

        vm.clearSelection = function () {
            vm.selectedIds = {};
            vm.selectedCount = 0;
            vm.selectAllChecked = false;
            vm.alerts.forEach(function (a) { a._selected = false; });
        };

        vm.getSelectedIds = function () {
            return Object.keys(vm.selectedIds);
        };

        // ── Bulk Actions ──
        vm.bulkMarkRead = function () {
            var ids = vm.getSelectedIds();
            if (!ids.length) { return; }
            NvApiSrv.bulkUpdateAlertStatus(ids, { read: true }).then(function () {
                NotificationSrv.success(ids.length + ' alert(s) marked as read');
                vm.clearSelection();
                vm.refresh();
            });
        };

        vm.bulkIgnore = function () {
            var ids = vm.getSelectedIds();
            if (!ids.length) { return; }
            NvApiSrv.bulkUpdateAlertStatus(ids, { status: 'Ignored' }).then(function () {
                NotificationSrv.success(ids.length + ' alert(s) ignored');
                vm.clearSelection();
                vm.refresh();
            });
        };

        vm.bulkFalsePositive = function () {
            var ids = vm.getSelectedIds();
            if (!ids.length) { return; }
            NvApiSrv.bulkUpdateAlertStatus(ids, { status: 'FalsePositive' }).then(function () {
                NotificationSrv.success(ids.length + ' alert(s) marked as false positive');
                vm.clearSelection();
                vm.refresh();
            });
        };

        vm.bulkDelete = function () {
            var ids = vm.getSelectedIds();
            if (!ids.length) { return; }
            if (!confirm('Delete ' + ids.length + ' alert(s)? This action cannot be undone.')) { return; }
            NvApiSrv.bulkUpdateAlertStatus(ids, { status: 'Deleted' }).then(function () {
                NotificationSrv.success(ids.length + ' alert(s) deleted');
                vm.clearSelection();
                vm.refresh();
            });
        };

        vm.bulkPromote = function () {
            var ids = vm.getSelectedIds();
            if (!ids.length) { return; }
            NvApiSrv.promoteAlerts(ids).then(function (resp) {
                NotificationSrv.success('Created case from ' + ids.length + ' alert(s)');
                vm.clearSelection();
                vm.refresh();
            });
        };

        // ── Per-Row Actions ──
        vm.toggleRead = function (alert) {
            var id = alert._id || alert.event_id;
            var newRead = !alert.read;
            NvApiSrv.updateAlertStatus(id, { read: newRead }).then(function () {
                alert.read = newRead;
            });
        };

        vm.toggleFollow = function (alert) {
            var id = alert._id || alert.event_id;
            var newFollow = !alert.follow;
            NvApiSrv.updateAlertStatus(id, { follow: newFollow }).then(function () {
                alert.follow = newFollow;
            });
        };

        vm.deleteAlert = function (alert) {
            var id = alert._id || alert.event_id;
            NvApiSrv.deleteAlert(id).then(function () {
                NotificationSrv.success('Alert deleted');
                vm.refresh();
            });
        };

        vm.promoteAlert = function (alert) {
            var id = alert._id || alert.event_id;
            var title = (alert.payload && alert.payload.title) || alert.title || 'Alert Case';
            NvApiSrv.promoteAlerts([id], title).then(function (resp) {
                NotificationSrv.success('Alert promoted to case');
                vm.refresh();
            });
        };

        vm.investigateAlert = function (alert) {
            var context = {
                type: 'alert',
                alertId: alert._id || alert.id || alert.event_id,
                title: vm.getAlertTitle(alert),
                severity: vm.getAlertSeverity(alert),
                payload: alert.payload || alert
            };
            $rootScope.$broadcast('nv:toggleCopilot', context);
        };

        // ── Quick View Sidebar ──
        vm.openSidebar = function (alert) {
            vm.selectedAlert = alert;
            vm.sidebarOpen = true;
            vm.sidebarLoading = true;
            vm.sidebarTab = 'details';
            vm.selectedIocs = null;
            vm.selectedSimilar = null;

            var id = alert._id || alert.event_id;

            $q.all({
                iocs: NvApiSrv.getAlertIocs(id),
                similar: NvApiSrv.getSimilarAlerts(id)
            }).then(function (results) {
                vm.selectedIocs = results.iocs;
                vm.selectedSimilar = results.similar;
                vm.sidebarLoading = false;
            }).catch(function () {
                vm.sidebarLoading = false;
            });
        };

        vm.closeSidebar = function () {
            vm.sidebarOpen = false;
            vm.selectedAlert = null;
        };

        vm.setSidebarTab = function (tab) {
            vm.sidebarTab = tab;
        };

        // ── Helpers ──
        vm.getSeverityLabel = function (sev) {
            var map = { 1: 'Low', 2: 'Medium', 3: 'High', 4: 'Critical' };
            return map[sev] || 'Unknown';
        };

        vm.getSeverityClass = function (sev) {
            var map = { 1: 'nv-sev-low', 2: 'nv-sev-medium', 3: 'nv-sev-high', 4: 'nv-sev-critical' };
            return map[sev] || '';
        };

        vm.getStatusClass = function (status) {
            var map = {
                'New': 'nv-status-open',
                'Updated': 'nv-status-open',
                'Imported': 'nv-status-merged',
                'Ignored': 'nv-status-closed',
                'FalsePositive': 'nv-status-closed',
                'Deleted': 'nv-status-closed'
            };
            return map[status] || 'nv-status-open';
        };

        vm.getAlertTitle = function (alert) {
            return (alert.payload && alert.payload.title) || alert.title || alert.sourceRef || 'Untitled Alert';
        };

        vm.getAlertSource = function (alert) {
            return (alert.payload && alert.payload.source) || alert.source || '-';
        };

        vm.getAlertType = function (alert) {
            return (alert.payload && alert.payload.type) || alert.type || '-';
        };

        vm.getAlertSeverity = function (alert) {
            return (alert.payload && alert.payload.severity) || alert.severity || 2;
        };

        vm.getAlertDate = function (alert) {
            return alert.timestamp || alert.date || (alert.payload && alert.payload.date) || alert._createdAt;
        };

        vm.getAlertRef = function (alert) {
            return (alert.payload && alert.payload.sourceRef) || alert.sourceRef || '';
        };

        vm.getAlertStatus = function (alert) {
            return alert.status || 'New';
        };

        vm.getAlertRawJson = function () {
            if (!vm.selectedAlert) { return '{}'; }
            try {
                return JSON.stringify(vm.selectedAlert, null, 2);
            } catch (e) {
                return '{}';
            }
        };

        vm.getIocCount = function (alert) {
            // Quick count from payload fields
            var count = 0;
            var p = alert.payload || alert;
            if (p.src_ip) count++;
            if (p.dst_ip) count++;
            if (p.file_hash) count++;
            if (p.domain) count++;
            if (p.url) count++;
            return count;
        };

        // ── Real-time WebSocket ──
        var streamUnsubscribe = $scope.$on('vyuha-stream-event', function (event, msg) {
            if (!vm.liveEnabled) { return; }

            if (msg.type === 'NEW_ALERT' || msg.type === 'AlertAccepted') {
                $scope.$apply(function () {
                    var newAlert = msg.payload || msg;
                    newAlert._id = newAlert._id || newAlert.event_id || newAlert.id;
                    newAlert._isNew = true;

                    // Only prepend if on first page with no status filter or 'New' filter
                    if (vm.page === 1 && (!vm.filterParams.status || vm.filterParams.status === 'New')) {
                        vm.alerts.unshift(newAlert);
                        if (vm.alerts.length > vm.pageSize) {
                            vm.alerts.pop();
                        }
                        vm.totalAlerts++;
                    }

                    vm.newAlertCount++;
                    vm.stats.total++;
                    vm.stats.new++;

                    // Remove animation class after delay
                    $timeout(function () {
                        newAlert._isNew = false;
                    }, 3000);
                });
            }

            if (msg.type === 'ALERT_STATUS_CHANGED') {
                $scope.$apply(function () {
                    vm.loadStats();
                });
            }
        });

        vm.toggleLive = function () {
            vm.liveEnabled = !vm.liveEnabled;
        };

        // ── AI Correlation ──
        vm.correlating = false;
        
        vm.runAiCorrelation = function() {
            vm.correlating = true;
            NvApiSrv.correlateAlerts().then(function(res) {
                vm.correlating = false;
                if (res && res.clusters && res.clusters.length > 0) {
                    vm.showCorrelationModal(res.clusters);
                } else {
                    NotificationSrv.info('No candidate cases could be identified from the current unassigned alerts.');
                }
            }).catch(function(err) {
                vm.correlating = false;
                NotificationSrv.error('AI Correlation Failed', 'Unable to cluster alerts at this time.');
            });
        };

        vm.showCorrelationModal = function(clusters) {
            var modalInstance = $uibModal.open({
                templateUrl: 'correlationModal.html',
                controller: function($scope, $uibModalInstance, clusters, NvApiSrv, NotificationSrv) {
                    $scope.clusters = clusters;
                    $scope.committing = false;

                    $scope.close = function() {
                        $uibModalInstance.dismiss('cancel');
                    };

                    $scope.commitCluster = function(cluster) {
                        $scope.committing = true;
                        NvApiSrv.commitCluster(cluster).then(function(res) {
                            $scope.committing = false;
                            NotificationSrv.success('Candidate Case Promoted: ' + cluster.suggested_title);
                            cluster._committed = true;
                        }).catch(function() {
                            $scope.committing = false;
                        });
                    };
                },
                size: 'lg',
                resolve: {
                    clusters: function() { return clusters; }
                }
            });

            modalInstance.result.finally(function() {
                vm.refresh();
            });
        };

        // ── Cleanup ──
        $scope.$on('$destroy', function () {
            if (streamUnsubscribe) { streamUnsubscribe(); }
            if (searchTimeout) { $timeout.cancel(searchTimeout); }
        });

        // ── Init ──
        vm.refresh();
    });
})();
