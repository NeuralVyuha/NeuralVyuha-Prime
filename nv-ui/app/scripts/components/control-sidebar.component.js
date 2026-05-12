(function() {
    'use strict';
    angular.module('nvControllers')
        .directive('controlSidebar', function() {
            return {
                restrict: 'E',
                templateUrl: 'views/components/control-sidebar.component.html'
            };
        });
})();
