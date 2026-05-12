(function() {
    'use strict';
    angular.module('nvServices').factory('TaskLogSrv', function(FileResource, NvConfig, NvApiSrv, $q) {
        var resource = FileResource('./api/case/task/:taskId/log/:logId', {}, {
            update: { method: 'PATCH' }
        });

        // --- Phase E6.4: Write-Path Override (Create Task Log/Note) ---
        var originalSave = resource.save;
        resource.save = function(params, data, success, error) {
            if (arguments.length === 3 && angular.isFunction(data)) {
                error = success; success = data; data = params; params = {};
            }

            if (NvConfig.MASTER_WRITE_TARGET === 'NV') {
                var taskId = params.taskId || data.taskId;
                return NvApiSrv.createTaskLog(taskId, data).then(function(res) {
                    var ret = angular.extend({}, data, { _id: res.log_id, id: res.log_id });
                    if (angular.isFunction(success)) success(ret);
                    return ret;
                }, function(err) {
                    if (angular.isFunction(error)) error(err);
                    return $q.reject(err);
                });
            } else {
                return originalSave(params, data, success, error);
            }
        };

        return resource;
    });
})();
