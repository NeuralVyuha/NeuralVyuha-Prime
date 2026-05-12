(function () {
    'use strict';

    /* @ngInject */
    function CaseTtpsCtrl($scope, $stateParams, $state, CaseTabsSrv, NvApiSrv, NotificationSrv) {
        CaseTabsSrv.activateTab($state.current.data.tab);

        var caseId = $stateParams.caseId;
        $scope.caseId = caseId;
        $scope.ttps = [];
        $scope.isLoading = true;
        $scope.showAddForm = false;

        // MITRE ATT&CK Tactics reference
        $scope.tactics = [
            'Reconnaissance', 'Resource Development', 'Initial Access', 'Execution',
            'Persistence', 'Privilege Escalation', 'Defense Evasion', 'Credential Access',
            'Discovery', 'Lateral Movement', 'Collection', 'Command and Control',
            'Exfiltration', 'Impact'
        ];

        $scope.newTtp = {
            tactic: '',
            techniqueId: '',
            techniqueName: ''
        };

        // Load TTPs
        function loadTtps() {
            $scope.isLoading = true;
            NvApiSrv.getTtps(caseId).then(function (data) {
                $scope.ttps = data || [];
                // Group by tactic for display
                $scope.groupedTtps = {};
                angular.forEach($scope.ttps, function (ttp) {
                    if (!$scope.groupedTtps[ttp.tactic]) {
                        $scope.groupedTtps[ttp.tactic] = [];
                    }
                    $scope.groupedTtps[ttp.tactic].push(ttp);
                });
            }).catch(function () {
                $scope.ttps = [];
                $scope.groupedTtps = {};
            }).finally(function () {
                $scope.isLoading = false;
            });
        }

        // Add TTP
        $scope.addTtp = function () {
            if (!$scope.newTtp.tactic || !$scope.newTtp.techniqueId || !$scope.newTtp.techniqueName) {
                NotificationSrv.error('CaseTtpsCtrl', 'All fields are required', 400);
                return;
            }

            NvApiSrv.addTtp(caseId, $scope.newTtp).then(function () {
                NotificationSrv.success('TTP added successfully');
                $scope.showAddForm = false;
                $scope.newTtp = { tactic: '', techniqueId: '', techniqueName: '' };
                loadTtps();
            }).catch(function (err) {
                NotificationSrv.error('CaseTtpsCtrl', 'Failed to add TTP', err.status);
            });
        };

        // Remove TTP
        $scope.removeTtp = function (ttp) {
            if (!confirm('Remove TTP ' + ttp.techniqueId + ': ' + ttp.techniqueName + '?')) return;

            NvApiSrv.removeTtp(caseId, ttp._id).then(function () {
                NotificationSrv.success('TTP removed');
                loadTtps();
            }).catch(function (err) {
                NotificationSrv.error('CaseTtpsCtrl', 'Failed to remove TTP', err.status);
            });
        };

        $scope.getTacticKeys = function () {
            return Object.keys($scope.groupedTtps || {});
        };

        // Init
        loadTtps();
    }

    angular.module('nvControllers').controller('CaseTtpsCtrl', CaseTtpsCtrl);
})();
