(function() {
    'use strict';
    angular.module('nvControllers')
        .directive('mainSidebar', function() {
            return {
                restrict: 'E',
                templateUrl: 'views/components/main-sidebar.component.html'
            };
        });
})();
