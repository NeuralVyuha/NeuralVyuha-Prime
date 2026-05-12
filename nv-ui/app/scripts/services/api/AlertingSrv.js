(function () {
    'use strict';
    angular.module('nvServices')
        .factory('AlertingSrv', function ($q, $http, $rootScope, StatSrv, StreamSrv, PSearchSrv, PaginatedQuerySrv, NvRouter, NvConfig) {

            var baseUrl = './api/alert';

            var similarityFilters = {
                // ... (filters preserved) ...
                'none': { label: 'None', filters: [] },
                'open-cases': { label: 'Open Cases', filters: [{ field: 'status', type: 'enumeration', value: { list: [{ text: 'Open', label: 'Open' }] } }] },
                'open-cases-last-7days': { label: 'Open Cases in the last 7 days', filters: [{ field: 'status', type: 'enumeration', value: { list: [{ text: 'Open', label: 'Open' }] } }, { field: '_createdAt', type: 'date', value: { operator: 'last7days', from: null, to: null } }] },
                'open-cases-last-30days': { label: 'Open Cases in the last 30 days', filters: [{ field: 'status', type: 'enumeration', value: { list: [{ text: 'Open', label: 'Open' }] } }, { field: '_createdAt', type: 'date', value: { operator: 'last30days', from: null, to: null } }] },
                'open-cases-last-3months': { label: 'Open Cases in the last 3 months', filters: [{ field: 'status', type: 'enumeration', value: { list: [{ text: 'Open', label: 'Open' }] } }, { field: '_createdAt', type: 'date', value: { operator: 'last3months', from: null, to: null } }] },
                'open-cases-last-year': { label: 'Open Cases in the last year', filters: [{ field: 'status', type: 'enumeration', value: { list: [{ text: 'Open', label: 'Open' }] } }, { field: '_createdAt', type: 'date', value: { operator: 'lastyear', from: null, to: null } }] },
                'resolved-cases': { label: 'Resolved cases', filters: [{ field: 'status', type: 'enumeration', value: { list: [{ text: 'Resolved', label: 'Resolved' }] } }] }
            };

            // V5 Adapter for PaginatedQuerySrv interface
            var V5ListAdapter = function(config, callback) {
                var self = this;
                this.values = [];
                this.total = 0;
                this.loading = false;
                this.config = config;

                this.update = function() {
                    self.loading = true;
                    // Map config to v5 params
                    var params = {
                        limit: config.pageSize || 10,
                        sort: config.sort,
                        filter: config.filter
                    };

                    NvRouter.getAlerts(params).then(function(data) {
                        self.values = data; // Assuming data is array
                        self.total = data.length; // Simplification if v5 doesn't return total in list
                        // In real v5, response might be { results: [], total: N }
                        if (data.results) {
                            self.values = data.results;
                            self.total = data.total;
                        }
                        if (angular.isFunction(callback)) {
                            callback(self.values);
                        }
                    }).catch(function(err) {
                        // Fallback logic inside NvRouter handles the switch,
                        // but if we are here, it means even fallback failed or 403.
                        // If fallback was triggered inside NvRouter, it would return v4 data.
                        // So we just handle error here.
                        console.error("V5 List Error", err);
                    }).finally(function() {
                        self.loading = false;
                    });
                };

                // Initial load
                this.update();
            };

            var factory = {
                getSimilarityFilters: function () {
                    return similarityFilters;
                },
                getSimilarityFilter: function (name) {
                    return (similarityFilters[name] || {}).filters;
                },
                list: function (config, callback) {
                    // E6.1: V5 Routing for Alerts List
                    if (NvConfig.useNvQueryReads) {
                        // We must return an object compatible with the controller's expectations of PaginatedQuerySrv
                        // But controllers expect specific methods.
                        // For Phase E6.1, we use the Adapter.
                        return new V5ListAdapter(config, callback);
                    }

                    return new PaginatedQuerySrv({
                        name: 'alerts',
                        root: undefined,
                        objectType: 'alert',
                        version: 'v1',
                        scope: config.scope,
                        sort: config.sort || ['-date'],
                        loadAll: config.loadAll || false,
                        pageSize: config.pageSize || 10,
                        filter: config.filter || undefined,
                        onUpdate: callback || undefined,
                        limitedCount: config.limitedCount || false,
                        operations: [
                            { '_name': 'listAlert' }
                        ],
                        extraData: ['importDate', 'caseNumber']
                    });
                },

                get: function (alertId) {
                    // E6.1: Route via NvRouter with Fallback
                    return NvRouter.getAlert(alertId);
                },

                create: function (alertId, data) {
                    return $http.post(baseUrl + '/' + alertId + '/createCase', data || {});
                },

                update: function (alertId, updates) {
                    return $http.patch(baseUrl + '/' + alertId, updates);
                },

                mergeInto: function (alertId, caseId) {
                    return $http.post(baseUrl + '/' + alertId + '/merge/' + caseId);
                },

                bulkMergeInto: function (alertIds, caseId) {
                    return $http.post(baseUrl + '/merge/_bulk', {
                        caseId: caseId,
                        alertIds: alertIds
                    });
                },

                canMarkAsRead: function (event) {
                    return !!!event.read;
                },

                canMarkAsUnread: function (event) {
                    return !!event.read;
                },

                markAsRead: function (alertId) {
                    return $http.post(baseUrl + '/' + alertId + '/markAsRead');
                },

                markAsUnread: function (alertId) {
                    return $http.post(baseUrl + '/' + alertId + '/markAsUnread');
                },

                follow: function (alertId) {
                    return $http.post(baseUrl + '/' + alertId + '/follow');
                },

                unfollow: function (alertId) {
                    return $http.post(baseUrl + '/' + alertId + '/unfollow');
                },

                forceRemove: function (alertId) {
                    return $http.delete(baseUrl + '/' + alertId, {
                        params: {
                            force: 1
                        }
                    });
                },

                bulkRemove: function (alertIds) {
                    return $http.post(baseUrl + '/delete/_bulk', {
                        ids: alertIds
                    }, {
                        params: {
                            force: 1
                        }
                    });
                },

                stats: function (scope) {
                    var field = 'status',
                        result = {},
                        statConfig = {
                            query: {},
                            objectType: 'alert',
                            field: field,
                            result: result
                        };

                    StreamSrv.addListener({
                        rootId: 'any',
                        objectType: 'alert',
                        scope: scope,
                        callback: function () {
                            StatSrv.get(statConfig);
                        }
                    });

                    return StatSrv.get(statConfig);
                }

            };

            return factory;
        });

})();
