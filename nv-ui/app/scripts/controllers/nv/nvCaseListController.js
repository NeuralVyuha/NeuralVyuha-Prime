(function () {
    'use strict';

    angular.module('neuralvyuha').controller('NvCaseListCtrl', function ($scope, $rootScope, $state, $uibModal, NvApiSrv) {
        var vm = this;

        vm.createCase = function () {
            var modal = $uibModal.open({
                templateUrl: 'views/partials/case/case.creation.html',
                controller: 'CaseCreationCtrl',
                size: 'lg',
                resolve: {
                    template: function () { return {}; }
                }
            });
            modal.result.then(function (data) {
                if (data && (data.id || data.case_id)) {
                    $state.go('app.case', { caseId: data.id || data.case_id });
                } else {
                    vm.load();
                }
            }).catch(angular.noop);
        };

        vm.loading = true;
        vm.error = null;
        vm.cases = [];
        vm.total = 0;

        vm.params = {
            limit: 20,
            offset: 0,
            status: 'Open'
        };

        vm.load = function () {
            vm.loading = true;
            vm.error = null;

            // Map UI status to NV Engine status
            var apiParams = angular.copy(vm.params);
            if (apiParams.status === 'Resolved' || apiParams.status === 'Closed') {
                apiParams.status = 'CLOSED';
            } else if (apiParams.status === 'Open' || apiParams.status === 'InProgress') {
                apiParams.status = 'OPEN';
            } else if (apiParams.status === 'All') {
                delete apiParams.status; // No filter — fetch all
            }

            NvApiSrv.getCases(apiParams).then(function (data) {
                if (angular.isArray(data)) {
                    vm.cases = data;
                    vm.total = data.length;
                } else if (data) {
                    vm.cases = data.data || data.cases || data.hits || [];
                    vm.total = data.total || 0;
                } else {
                    vm.cases = [];
                    vm.total = 0;
                }
            }).catch(function (err) {
                vm.cases = [];
                vm.total = 0;
                vm.error = 'Unable to load cases. The server may be unreachable.';
            }).finally(function () {
                vm.loading = false;
            });
        };

        vm.filterStatus = function (status) {
            vm.params.status = status;
            vm.params.offset = 0;
            vm.load();
        };

        vm.nextPage = function () {
            if (vm.params.offset + vm.params.limit < vm.total) {
                vm.params.offset += vm.params.limit;
                vm.load();
            }
        };

        vm.prevPage = function () {
            if (vm.params.offset > 0) {
                vm.params.offset = Math.max(0, vm.params.offset - vm.params.limit);
                vm.load();
            }
        };

        vm.getCurrentPage = function () {
            return Math.floor(vm.params.offset / vm.params.limit) + 1;
        };

        vm.getTotalPages = function () {
            return Math.max(1, Math.ceil(vm.total / vm.params.limit));
        };

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

        vm.getTlpClass = function (tlp) {
            switch (tlp) {
                case 0: return 'label-default'; // White
                case 1: return 'label-success'; // Green
                case 2: return 'label-warning'; // Amber
                case 3: return 'label-danger'; // Red
                default: return 'label-default';
            }
        };

        vm.getTlpLabel = function (tlp) {
            switch (tlp) {
                case 0: return 'TLP:WHITE';
                case 1: return 'TLP:GREEN';
                case 2: return 'TLP:AMBER';
                case 3: return 'TLP:RED';
                default: return 'TLP:UNKNOWN';
            }
        };

        vm.openCase = function (c) {
            if (c && c.id) {
                $state.go('app.case', { caseId: c.id });
            } else if (c && c._id) {
                $state.go('app.case', { caseId: c._id });
            }
        };

        vm.retry = function () {
            vm.load();
        };

        vm.investigateCase = function (caseObj) {
            var context = {
                type: 'case',
                caseId: caseObj.caseId || caseObj.id || caseObj._id,
                title: caseObj.title,
                severity: caseObj.severity,
                payload: caseObj
            };
            $rootScope.$broadcast('nv:toggleCopilot', context);
        };

        vm.load();
    });
})();
