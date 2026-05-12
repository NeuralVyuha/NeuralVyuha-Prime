(function() {
    'use strict';
    angular.module('nvFilters').filter('offset', function() {
        return function(input, start) {
            if (!input) {
                return;
            }
            start = parseInt(start, 10);
            return input.slice(start);
        };
    });
})();
