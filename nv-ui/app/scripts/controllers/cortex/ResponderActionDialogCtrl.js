(function() {
    'use strict';
    angular.module('nvControllers')
        .controller('ResponderActionDialogCtrl', ResponderActionDialogCtrl);

        function ResponderActionDialogCtrl($uibModalInstance, action) {
            var self = this;

            this.action = action;
            this.report = (JSON.parse(action.report) || {}).full;

            this.close = function() {
                $uibModalInstance.dismiss();
            };
        }
})();
