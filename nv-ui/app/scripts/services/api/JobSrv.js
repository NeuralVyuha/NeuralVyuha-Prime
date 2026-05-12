(function() {
    'use strict';
    angular.module('nvServices')
        .factory('JobSrv', function($resource) {
            return $resource('./api/case/artifact/:artifactId/job/:analyzerId', {}, {}, {});
        });
})();
