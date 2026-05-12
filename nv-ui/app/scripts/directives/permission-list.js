(function() {
    'use strict';
    angular.module('nvDirectives')
        .directive('permissionList', function() {
            return {
                restrict: 'E',
                templateUrl: 'views/directives/permission-list.html',
                scope: {
                    permissions: '=',
                    showLabel: '=',
                    label: '='
                }
            };
        });
})();
