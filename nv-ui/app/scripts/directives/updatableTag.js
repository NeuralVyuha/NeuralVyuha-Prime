(function() {
    'use strict';
    angular.module('nvDirectives')
        .directive('updatableTag', function(UtilsSrv) {
            return {
                'restrict': 'E',
                'link': UtilsSrv.updatableLink,
                'templateUrl': 'views/directives/updatable-tag.html',
                'scope': {
                    'value': '=?',
                    'colour': '<?',
                    'onUpdate': '&'
                }
            };
        });
})();
