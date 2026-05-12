(function () {
    'use strict';

    angular.module('nvControllers').controller('NvHelpCtrl', function ($scope, $http) {
        var vm = this;
        vm.currentSection = 'intro';
        vm.searchText = '';
        vm.manualContent = '';
        vm.loadingManual = false;

        // Load full manual only when requested
        $scope.$watch('vm.currentSection', function (newVal) {
            if (newVal === 'manual' && !vm.manualContent) {
                vm.loadingManual = true;
                // Attempt to load generated manual JSON or MD.
                // Since we are in browser, we can't read local files directly unless served.
                // We assume 'docs/USER_MANUAL.md' is served or we embedded it.
                // For this phase, we'll simulate fetching a static resource if available, or show a placeholder.

                // In a real build, we'd have a task to copy USER_MANUAL.md to 'assets/manual.md'
                // For now, let's just put a placeholder message or try to fetch from a relative path if served.
                vm.manualContent = "The full User Manual is available in the 'docs/' folder of the project repository. Please refer to [User Manual](../../docs/USER_MANUAL.md) for detailed technical specifications.";
                vm.loadingManual = false;
            }
        });
    });
})();
