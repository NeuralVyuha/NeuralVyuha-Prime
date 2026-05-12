(function () {
    'use strict';
    angular.module('nvServices')
        .service('CaseSrv', function ($q, $http, $resource, QuerySrv, NvApiSrv, NvConfig, NvRouter, PaginatedQuerySrv) {

            var resource = $resource('./api/case/:caseId', {}, {
                update: { method: 'PATCH' },
                links: { method: 'GET', url: './api/case/:caseId/links', isArray: true },
                forceRemove: { method: 'DELETE', url: './api/case/:caseId/force', params: { caseId: '@caseId' } },
                query: { method: 'POST', url: './api/case/_search', isArray: true }
            });

            this.get = resource.get;
            this.alerts = resource.alerts;
            this.forceRemove = resource.forceRemove;
            this.links = function (params, success, error) {
                if (NvConfig.useNvQueryReads) {
                    return NvApiSrv.getCaseLinks(params.caseId).then(function (res) {
                        if (angular.isFunction(success)) success(res);
                        return res;
                    }, function (err) {
                        if (angular.isFunction(error)) error(err);
                        return $q.reject(err);
                    });
                }
                return resource.links(params, success, error);
            };
            var originalUpdate = resource.update;
            this.update = function (params, data, success, error) {
                if (NvConfig.MASTER_WRITE_TARGET === 'NV') {
                    // --- Phase E6.4: Update Mapping ---
                    var nvData = angular.copy(data);

                    // Map field names
                    if (nvData.owner) {
                        nvData.assigned_to = nvData.owner;
                        delete nvData.owner;
                    }

                    // Map status values
                    if (nvData.status) {
                        if (nvData.status === 'Resolved' || nvData.status === 'Closed') {
                            nvData.status = 'CLOSED';
                        } else if (nvData.status === 'InProgress') {
                            nvData.status = 'IN_PROGRESS';
                        } else if (nvData.status === 'Open') {
                            nvData.status = 'OPEN';
                        }
                    }

                    // Ensure numeric types for metadata
                    if (nvData.severity) nvData.severity = parseInt(nvData.severity);
                    if (nvData.tlp) nvData.tlp = parseInt(nvData.tlp);
                    if (nvData.pap) nvData.pap = parseInt(nvData.pap);

                    return NvApiSrv.updateCase(params.caseId, nvData).then(function (res) {
                        if (angular.isFunction(success)) success(res);
                        return res;
                    }, function (err) {
                        if (angular.isFunction(error)) error(err);
                        return $q.reject(err);
                    });
                } else {
                    return originalUpdate(params, data, success, error);
                }
            };

            this.merge = resource.merge;
            this.query = resource.query;

            // --- Phase E6.4: Write-Path Override (Create Case) ---
            var originalSave = resource.save;
            this.save = function (params, data, success, error) {
                // Handle optional params
                if (arguments.length === 3 && angular.isFunction(data)) {
                    error = success;
                    success = data;
                    data = params;
                    params = {};
                }

                if (NvConfig.MASTER_WRITE_TARGET === 'NV') {
                    // NV Path: Call NvApiSrv.createCase
                    // We must return a promise object that mimics $resource promise if possible
                    // However, controller usually relies on callback or promise chain.
                    return NvApiSrv.createCase(data).then(function (res) {
                        var ret = angular.extend({}, data, { _id: res.case_id, id: res.case_id });
                        if (angular.isFunction(success)) success(ret);
                        return ret;
                    }, function (err) {
                        if (angular.isFunction(error)) error(err);
                        return $q.reject(err);
                    });
                } else {
                    return originalSave(params, data, success, error);
                }
            };

            // --- Phase E6.3: NeuralVyuha List Adapter ---
            var NvCaseListAdapter = function (config, callback) {
                var self = this;
                this.values = [];
                this.total = 0;
                this.loading = false;
                this.config = config;

                this.update = function () {
                    self.loading = true;
                    var params = {
                        limit: config.pageSize || 15,
                        offset: (config.currentPage && config.currentPage > 1) ? (config.currentPage - 1) * (config.pageSize || 15) : 0
                    };

                    // Extract filters from config
                    if (config.filtering && config.filtering.context && config.filtering.context.filters) {
                        var statusFilter = _.find(config.filtering.context.filters, { field: 'status' });
                        if (statusFilter && statusFilter.value && statusFilter.value.list && statusFilter.value.list.length > 0) {
                            var uiStatus = statusFilter.value.list[0].text;
                            // Map legacy UI statuses to NV Engine statuses
                            if (uiStatus === 'Resolved' || uiStatus === 'Closed') {
                                params.status = 'CLOSED';
                            } else if (uiStatus === 'Open' || uiStatus === 'InProgress') {
                                params.status = 'OPEN';
                            }
                        }
                    }

                    NvApiSrv.getCases(params).then(function (data) {
                        var mapped = _.map(data.cases || [], function (c) {
                            return {
                                _id: c.case_id,
                                id: c.case_id,
                                number: c.case_number,
                                title: c.title,
                                description: c.description,
                                severity: c.severity,
                                status: c.status === 'CLOSED' ? 'Resolved' : (c.status === 'IN_PROGRESS' ? 'InProgress' : (c.status === 'OPEN' ? 'Open' : c.status)),
                                owner: c.assigned_to,
                                startDate: c.created_at,
                                endDate: c.closed_at,
                                _source: 'NV'
                            };
                        });

                        self.values = mapped;
                        self.total = data.total;
                        if (angular.isFunction(config.onUpdate)) config.onUpdate(self.values);

                    }).catch(function (err) {
                        console.error("v5 Case List failed:", err);
                        self.values = [];
                        self.total = 0;
                        if (angular.isFunction(config.onUpdate)) config.onUpdate([]);
                    }).finally(function () {
                        self.loading = false;
                    });
                };

                this.update();
            };

            this.list = function (config, callback) {
                if (NvConfig.useNvQueryReads) {
                    return new NvCaseListAdapter(config, callback);
                }
                return new PaginatedQuerySrv(config, callback);
            };

            this.getById = function (id, withStats) {
                var defer = $q.defer();
                var fetchLegacy = function () {
                    QuerySrv.call('v1', [{ '_name': 'getCase', 'idOrName': id }], {
                        name: 'get-case-' + id,
                        page: { from: 0, to: 1, extraData: withStats ? ["observableStats", "taskStats", "alerts", "isOwner", "shareCount", "permissions"] : [] }
                    }).then(function (response) { defer.resolve(response[0]); }).catch(function (err) { defer.reject(err); });
                };

                if (NvConfig.useNvQueryReads) {
                    NvApiSrv.getCase(id).then(function (nvCase) {
                        var legacyCase = {
                            _id: nvCase.case_id,
                            title: nvCase.title,
                            description: nvCase.description,
                            severity: nvCase.severity,
                            tlp: nvCase.tlp,
                            pap: nvCase.pap,
                            status: nvCase.status === 'CLOSED' ? 'Resolved' : (nvCase.status === 'IN_PROGRESS' ? 'InProgress' : (nvCase.status === 'OPEN' ? 'Open' : nvCase.status)),
                            owner: nvCase.assigned_to,
                            assignee: nvCase.assigned_to,
                            startDate: nvCase.created_at,
                            endDate: nvCase.closed_at,
                            tags: nvCase.tags || [],
                            customFields: nvCase.customFields || [],
                            _source: 'NV',
                            number: nvCase.case_number,
                            extraData: { permissions: ['manageCase'], alerts: [] },
                            flag: nvCase.flag || false,
                            resolutionStatus: nvCase.resolution_status || null,
                            impactStatus: nvCase.impact_status || null,
                            summary: nvCase.summary || null
                        };
                        defer.resolve(legacyCase);
                    }).catch(function (err) {
                        console.error("v5 Get Case failed:", err);
                        defer.reject(err);
                    });
                } else { fetchLegacy(); }
                return defer.promise;
            };

            this.alerts = function (id) {
                var defer = $q.defer();
                QuerySrv.call('v1', [{ '_name': 'getCase', 'idOrName': id }, { '_name': 'alerts' }], { name: 'get-case-alerts' + id })
                    .then(function (response) { defer.resolve(response); })
                    .catch(function (err) { defer.reject(err); });
                return defer.promise;
            };

            this.merge = function (ids) { return $http.post('./api/v1/case/_merge/' + ids.join(',')); };
            this.bulkUpdate = function (ids, update) { return $http.patch('./api/case/_bulk', _.extend({ ids: ids }, update)); };
            this.getShares = function (id) { return $http.get('./api/case/' + id + '/shares'); };
            this.setShares = function (id, shares) { return $http.post('./api/case/' + id + '/shares', { "shares": shares }); };
            this.updateShare = function (org, patch) { return $http.patch('./api/case/share/' + org, patch); };
            this.removeShare = function (id, share) { return $http.delete('./api/case/' + id + '/shares', { data: { organisations: [share.organisationName] }, headers: { 'Content-Type': 'application/json' } }); };
            this.removeCustomField = function (caseId, customFieldValueId) {
                if (typeof customFieldValueId === 'undefined') {
                    customFieldValueId = caseId;
                    caseId = undefined;
                }
                if (NvConfig.MASTER_WRITE_TARGET === 'NV') {
                    if (!caseId) {
                        return $q.reject("caseId is required for NV mode custom field deletion.");
                    }
                    var updateObj = {};
                    updateObj['customFields.' + customFieldValueId] = null;
                    return NvApiSrv.updateCase(caseId, updateObj);
                }
                return $http.delete('./api/v1/case/customField/' + customFieldValueId);
            };
        });
})();
