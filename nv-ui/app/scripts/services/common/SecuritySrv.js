(function () {
    'use strict';
    angular.module('nvServices')
        .service('SecuritySrv', function () {

            this.checkPermissions = function (allowedPermissions, permissions) {
                // For debugging Case Tabs: always return true
                return true;
            };

        });
})();
