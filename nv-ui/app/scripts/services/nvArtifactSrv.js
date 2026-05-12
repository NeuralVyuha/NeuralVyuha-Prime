'use strict';

angular.module('nvServices').factory('nvArtifactSrv', function ($http, $q, NvConfig, NotificationSrv) {
    var service = {};

    // Mock functionality to fetch a presigned URL from the new v5 artifact store (MinIO/S3)
    service.getPresignedUrl = function (artifactId) {
        if (!NvConfig.useNvQueryReads) {
            // Fallback to legacy v4 stream endpoint if disabled
            return $q.resolve('/api/connector/cortex/job/' + artifactId + '/report');
        }

        // NeuralVyuha nv-artifact-service route
        return $http.get('/api/artifacts/' + artifactId + '/download-url', {
            timeout: NvConfig.timeoutMs
        }).then(function (res) {
            return res.data.url;
        }).catch(function (err) {
            NotificationSrv.error('Download Failed', 'Could not generate download link for artifact.');
            return $q.reject(err);
        });
    };

    return service;
});
