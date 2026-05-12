(function() {
    'use strict';
    angular.module('nvDirectives').directive('tagList', function() {
        return {
            restrict: 'E',
            scope: {
                data: '='
            },
            templateUrl: 'views/directives/tag-list.html'
        };
    });

})();
