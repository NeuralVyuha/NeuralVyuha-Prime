(function() {
    'use strict';
    angular.module('nvDirectives')
        .directive('updatable', function(UtilsSrv) {
            return {
                'restrict': 'E',
                'link': UtilsSrv.updatableLink,
                'transclude': true,
                'templateUrl': 'views/directives/updatable.html',
                'scope': {
                    'value': '=?',
                    'onUpdate': '&',
                    'active': '=?'
                }
            };
        });
})();
