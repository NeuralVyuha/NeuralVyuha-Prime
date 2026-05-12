(function() {
    'use strict';
    angular.module('nvControllers')
        .directive('appContainer', function() {
            return {
                restrict: 'E',
                templateUrl: 'views/components/app-container.component.html'
            };
        });
})();
