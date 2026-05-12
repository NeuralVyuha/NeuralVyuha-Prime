'use strict';

angular.module('nvServices').factory('nvSocketSrv', function ($rootScope, $window, AuthenticationSrv, NotificationSrv) {
    var service = {};
    var socket = null;
    var reconnectTimer = null;

    service.connect = function () {
        if (socket) return;

        if (!AuthenticationSrv.currentUser || !AuthenticationSrv.currentUser.token) {
            console.warn('Cannot connect to Vyuha-Stream: No authenticated user token.');
            return;
        }

        var tenantId = AuthenticationSrv.currentUser.tenant_id || AuthenticationSrv.currentUser.organisation || 'admin';
        var token = AuthenticationSrv.currentUser.token;

        var protocol = $window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        var wsUrl = protocol + '//' + $window.location.host + '/ws/' + tenantId + '?token=' + token;

        console.log('Connecting to Vyuha-Stream at ' + wsUrl + '...');

        try {
            socket = new WebSocket(wsUrl);

            socket.onopen = function () {
                console.log('Vyuha-Stream connected (Tenant: ' + tenantId + ').');
                if (reconnectTimer) {
                    clearTimeout(reconnectTimer);
                    reconnectTimer = null;
                }
            };

            socket.onmessage = function (event) {
                var msg = JSON.parse(event.data);

                // Handle Heartbeat
                if (msg.type === 'PING') {
                    socket.send(JSON.stringify({ type: 'PONG', timestamp: Date.now() }));
                } else if (msg.type === 'CONNECTED') {
                    // Connection acknowledged by backend
                } else {
                    // Broadcast real-time security events to Angular controllers globally
                    $rootScope.$broadcast('vyuha-stream-event', msg);
                }
            };

            socket.onclose = function (event) {
                console.log('Vyuha-Stream disconnected. Reconnecting in 5s...');
                socket = null;
                reconnectTimer = setTimeout(function () {
                    service.connect();
                }, 5000);
            };

            socket.onerror = function (err) {
                console.error('Vyuha-Stream Connection Error');
            };
        } catch (e) {
            console.error('WebSocket creation failed', e);
        }
    };

    service.disconnect = function () {
        if (socket) {
            socket.onclose = function () { }; // prevent auto-reconnect
            socket.close();
            socket = null;
            console.log('Disconnected from Vyuha-Stream.');
        }
        if (reconnectTimer) {
            clearTimeout(reconnectTimer);
            reconnectTimer = null;
        }
    };

    return service;
});
