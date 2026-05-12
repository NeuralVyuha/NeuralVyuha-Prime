(function() {
    'use strict';
    angular.module('nvFilters').filter('fang', function(UtilsSrv) {
        return function(value) {
            if(!value) {
                return '';
            }

            return UtilsSrv.fangValue(value);
        };
    });
})();
