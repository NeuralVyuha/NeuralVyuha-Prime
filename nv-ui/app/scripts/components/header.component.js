(function () {
    'use strict';
    angular.module('nvControllers')
        .directive('header', function () {
            return {
                restrict: 'E',
                templateUrl: 'views/components/header.component.html?v=' + new Date().getTime()
            };
        });
})();
