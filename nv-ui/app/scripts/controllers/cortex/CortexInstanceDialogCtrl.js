(function() {
    'use strict';
    angular.module('nvControllers')
        .controller('CortexInstanceDialogCtrl', CortexInstanceDialogCtrl);

        function CortexInstanceDialogCtrl($uibModalInstance, servers) {
            this.servers = servers;
            this.selected = null;

            this.ok = function() {
                $uibModalInstance.close(this.selected);
            };

            this.cancel = function() {
                $uibModalInstance.dismiss();
            };
        }
})();
