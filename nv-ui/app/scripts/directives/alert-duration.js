(function() {
    'use strict';
    angular.module('nvDirectives').directive('alertDuration', function() {
        return {
            restrict: 'E',
            scope: {
                start: '=',
                end: '=',
                icon: '@',
                indicator: '='
            },
            templateUrl: 'views/directives/alert-duration.html'
        };
    });
})();
