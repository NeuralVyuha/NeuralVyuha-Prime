(function () {
    'use strict';

    angular.module('nvCore').controller('NvGroupDetailCtrl', function ($scope, $stateParams, NvApiSrv) {
        var vm = this;
        vm.loading = true;
        vm.group = {};
        vm.alerts = [];
        vm.groupId = $stateParams.id;

        vm.load = function () {
            vm.loading = true;
            NvApiSrv.getGroup(vm.groupId).then(function (data) {
                vm.group = data;
                return NvApiSrv.getGroupAlerts(vm.groupId);
            }).then(function (alertData) {
                vm.alerts = alertData.hits || [];
            }).finally(function () {
                vm.loading = false;
            });
        };

        // Initial load
        vm.load();
    });
})();
