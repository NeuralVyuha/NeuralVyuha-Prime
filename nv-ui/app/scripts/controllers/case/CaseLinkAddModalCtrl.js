(function () {
    'use strict';
    angular.module('nvControllers').controller('CaseLinkAddModalCtrl',
        function ($scope, $uibModalInstance, NvApiSrv, NotificationSrv, caseId) {
            $scope.currentCaseId = caseId;
            $scope.searchQuery = '';
            $scope.results = [];
            $scope.searching = false;
            $scope.linking = false;
            $scope.searched = false;

            $scope.search = function () {
                if (!$scope.searchQuery) return;
                $scope.searching = true;
                $scope.searched = true;

                NvApiSrv.getCases({ title: $scope.searchQuery, limit: 10 })
                    .then(function (data) {
                        $scope.results = data.cases || [];
                    })
                    .finally(function () {
                        $scope.searching = false;
                    });
            };

            $scope.link = function (targetCaseId) {
                $scope.linking = true;
                NvApiSrv.linkCase($scope.currentCaseId, targetCaseId)
                    .then(function () {
                        NotificationSrv.log('Cases linked successfully', 'success');
                        $uibModalInstance.close();
                    })
                    .catch(function (err) {
                        NotificationSrv.error('CaseLinkAddModal', 'Failed to link cases: ' + (err.data.detail || err.statusText));
                    })
                    .finally(function () {
                        $scope.linking = false;
                    });
            };

            $scope.cancel = function () {
                $uibModalInstance.dismiss();
            };
        }
    );
})();
