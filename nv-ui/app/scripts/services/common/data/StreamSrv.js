(function () {
    'use strict';
    angular.module('nvServices').factory('StreamSrv', function ($q, $rootScope, $http, $timeout, UserSrv, AuthenticationSrv, AfkSrv, NotificationSrv, VersionSrv) {

        var self = {
            isPolling: false,
            streamId: null,
            httpRequestCanceller: $q.defer(),
            disabled: true,
            retryCount: 0,
            maxRetries: 3,

            init: function () {
                if (self.retryCount >= self.maxRetries) {
                    console.warn('StreamSrv: Max retries reached, streaming disabled');
                    self.disabled = true;
                    return;
                }
                self.streamId = null;
                self.disabled = false;
                self.requestStream();
            },

            runCallbacks: function (id, objectType, message) {
                $rootScope.$broadcast('stream:' + id + '-' + objectType, message);
            },

            handleStreamResponse: function (data) {
                if (!data || data.length === 0) {
                    return;
                }

                var byRootIds = {};
                var byObjectTypes = {};
                var byRootIdsWithObjectTypes = {};
                var bySecondaryObjectTypes = {};

                angular.forEach(data, function (message) {
                    var rootId = message.base.rootId;
                    var objectType = message.base.objectType;
                    var rootIdWithObjectType = rootId + '|' + objectType;
                    var secondaryObjectTypes = message.summary ? _.without(_.keys(message.summary), objectType) : [];

                    if (rootId in byRootIds) {
                        byRootIds[rootId].push(message);
                    } else {
                        byRootIds[rootId] = [message];
                    }

                    if (objectType in byObjectTypes) {
                        byObjectTypes[objectType].push(message);
                    } else {
                        byObjectTypes[objectType] = [message];
                    }

                    if (rootIdWithObjectType in byRootIdsWithObjectTypes) {
                        byRootIdsWithObjectTypes[rootIdWithObjectType].push(message);
                    } else {
                        byRootIdsWithObjectTypes[rootIdWithObjectType] = [message];
                    }

                    _.each(secondaryObjectTypes, function (type) {
                        if (type in bySecondaryObjectTypes) {
                            bySecondaryObjectTypes[type].push(message);
                        } else {
                            bySecondaryObjectTypes[type] = [message];
                        }
                    });

                });

                angular.forEach(byRootIds, function (messages, rootId) {
                    self.runCallbacks(rootId, 'any', messages);
                });
                angular.forEach(byObjectTypes, function (messages, objectType) {
                    self.runCallbacks('any', objectType, messages);
                });

                // Trigger strem event for sub object types
                angular.forEach(bySecondaryObjectTypes, function (messages, objectType) {
                    self.runCallbacks('any', objectType, messages);
                });

                angular.forEach(byRootIdsWithObjectTypes, function (messages, rootIdWithObjectType) {
                    var temp = rootIdWithObjectType.split('|', 2),
                        rootId = temp[0],
                        objectType = temp[1];

                    self.runCallbacks(rootId, objectType, messages);
                });

                self.runCallbacks('any', 'any', data);
            },

            cancelPoll: function () {
                if (self.httpRequestCanceller) {
                    self.httpRequestCanceller.resolve('cancel');
                }

                self.disabled = true;
            },

            poll: function () {
                // Feature Disabled: NeuralVyuha uses WebSockets now, not long polling
                // Stop 404 errors by instantly returning.
                self.isPolling = false;
                self.disabled = true;
                return;
            },


            requestStream: function () {
                // Feature Disabled: NeutralVyuha uses WebSockets
                self.streamId = 'dummy-stream-id';
                return;
            },

            /**
             * @param config {Object} This configuration object has the following attributes
             * <li>rootId</li>
             * <li>objectType {String}</li>
             * <li>scope {Object}</li>
             * <li>callback {Function}</li>
             */
            addListener: function (config) {
                if (!config.scope) {
                    console.error('No scope provided, use the old listen method', config);
                    self.listen(config.rootId, config.objectType, config.callback);
                    return;
                }

                var eventName = 'stream:' + config.rootId + '-' + config.objectType;
                config.scope.$on(eventName, function (event, data) {
                    if (!self.disabled) {
                        config.callback(data);
                    }
                });
            }
        };

        return self;
    });
})();
