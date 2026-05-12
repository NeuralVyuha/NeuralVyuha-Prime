(function () {
    'use strict';

    angular.module('nvComponents')
        .component('datalistHeader', {
            controller: function () { },
            controllerAs: '$ctrl',
            templateUrl: 'views/components/common/datalist-header.component.html',
            bindings: {
                title: '@',
                list: '<',
                total: '<'
            }
        });
})();
