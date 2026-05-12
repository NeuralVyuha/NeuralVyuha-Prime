(function() {
    'use strict';

    angular.module('nvFilters').filter('escape', function() {
        return window.encodeURIComponent;
    });
})();
