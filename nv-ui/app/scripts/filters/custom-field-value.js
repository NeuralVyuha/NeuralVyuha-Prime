(function() {
    'use strict';
    angular.module('nvFilters').filter('customFieldValue', function(UiSettingsSrv) {
        return function(customField) {
            if(!customField) {
                return '';
            }

            var format = UiSettingsSrv.defaultDateFormat()

            switch(customField.type) {
                case 'date':
                    return moment(customField.value).format(format);
                default:
                    return customField.value;
            }
        };
    });
})();
