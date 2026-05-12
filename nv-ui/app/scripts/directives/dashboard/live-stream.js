(function () {
    'use strict';
    angular.module('nvDirectives').directive('dashboardLiveStream', function ($http, $rootScope, $timeout, nvSocketSrv, NvApiSrv) {
        return {
            restrict: 'E',
            replace: true,
            scope: {
                options: '=',
                autoload: '=',
                mode: '='
            },
            templateUrl: 'views/directives/dashboard/live-stream.html',
            link: function (scope) {
                scope.events = [];
                scope.loading = false;
                scope.connected = false;

                // 1. Load Initial Buffer
                scope.loadBuffer = function () {
                    scope.loading = true;
                    var limit = (scope.options && scope.options.limit) ? scope.options.limit : 50;
                    NvApiSrv.getLiveStream(limit)
                        .then(function (data) {
                            scope.events = data.events || [];
                            scope.loading = false;
                        })
                        .catch(function () {
                            scope.loading = false;
                        });
                };

                // 2. Handle Real-time Updates
                var streamUnbind = $rootScope.$on('vyuha-stream-event', function (event, msg) {
                    if (msg.type === 'NEW_ALERT') {
                        $timeout(function () {
                            // Prepend new alert
                            scope.events.unshift({
                                event_id: msg.id,
                                title: msg.title,
                                severity: msg.severity,
                                source: msg.source,
                                timestamp: msg.timestamp,
                                status: 'LIVE'
                            });

                            // Trim list to limit
                            if (scope.events.length > (scope.options.limit || 100)) {
                                scope.events.pop();
                            }
                        });
                    }
                });

                // 3. Connect to WebSocket if not already
                nvSocketSrv.connect();

                if (scope.autoload) {
                    scope.loadBuffer();
                }

                scope.$on('$destroy', function () {
                    streamUnbind();
                });

                scope.getSeverityClass = function (sev) {
                    switch (parseInt(sev)) {
                        case 4: return 'sev-critical';
                        case 3: return 'sev-high';
                        case 2: return 'sev-medium';
                        default: return 'sev-low';
                    }
                };
            }
        };
    });
})();
