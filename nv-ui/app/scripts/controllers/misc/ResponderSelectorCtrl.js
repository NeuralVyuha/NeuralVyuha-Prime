(function() {
    'use strict';

    angular.module('nvControllers')
        .controller('ResponderSelectorCtrl', function($uibModalInstance, responders) {
            this.responders = responders || [];
            this.selectAll = false;
            this.state = {
                filter: ''
            };

            this.next = function(responder) {
                $uibModalInstance.close(responder);
            };

            this.cancel = function() {
                $uibModalInstance.dismiss();
            };
        });
})();
