(function() {
    'use strict';
    angular.module('nvServices')
        .service('CaseTaskSrv', function($resource, $http, $q, QuerySrv, ModalSrv, NvConfig, NvApiSrv) {
            var resource = $resource('./api/case/:caseId/task/:taskId', {}, {
                update: { method: 'PATCH' }
            });

            this.get = resource.get;
            this.update = resource.update;
            this.query = resource.query;

            // --- Phase E6.4: Write-Path Override (Create Task) ---
            var originalSave = resource.save;
            this.save = function(params, data, success, error) {
                if (arguments.length === 3 && angular.isFunction(data)) {
                    error = success; success = data; data = params; params = {};
                }

                if (NvConfig.MASTER_WRITE_TARGET === 'NV') {
                    // NV Path
                    var caseId = params.caseId || data.caseId;
                    return NvApiSrv.createTask(caseId, data).then(function(res) {
                        var ret = angular.extend({}, data, { _id: res.task_id, id: res.task_id });
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

            this.getById = function(id) {
                var defer = $q.defer();
                QuerySrv.call('v1', [{ _name: 'getTask', idOrName: id }], {
                    name: 'get-task-' + id,
                    page: { from: 0, to: 1, extraData: ['actionRequired', 'actionRequiredMap'] }
                }).then(function(response) { defer.resolve(response[0]); }).catch(function(err){ defer.reject(err); });
                return defer.promise;
            };

            this.getActionRequiredMap = function(taskId) { return $http.get('./api/v1/task/' + taskId + '/actionRequired'); };
            this.markAsDone = function(taskId, org) { return $http.put('./api/v1/task/' + taskId + '/actionDone/' + org); };
            this.markAsActionRequired = function(taskId, org) { return $http.put('./api/v1/task/' + taskId + '/actionRequired/' + org); };
            this.getShares = function(caseId, taskId) { return $http.get('./api/case/' + caseId + '/task/' + taskId + '/shares'); };
            this.addShares = function(taskId, organisations) { return $http.post('./api/case/task/' + taskId + '/shares', { organisations: organisations }); };
            this.bulkUpdate = function(ids, update) { return $http.patch('./api/v1/task/_bulk', _.extend({ids: ids}, update)); };
            this.removeShare = function(id, share) { return $http.delete('./api/task/'+id+'/shares', { data: { organisations: [share.organisationName] }, headers: { 'Content-Type': 'application/json' } }); };

            this.promtForActionRequired = function(title, prompt) {
               var defer = $q.defer();
               var confirmModal = ModalSrv.confirm(title, prompt, { okText: 'Yes, add log', actions: [{ flavor: 'default', text: 'Proceed without log', dismiss: 'skip-log' }] });
               confirmModal.result.then(function() { defer.resolve('add-log'); }).catch(function(err) { if(err === 'skip-log') { defer.resolve(err); } else { defer.reject(err); } });
               return defer.promise;
           };
        });
})();
