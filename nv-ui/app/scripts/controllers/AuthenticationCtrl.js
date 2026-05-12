/**
 * Controller for login modal page2
 */
(function () {
    'use strict';
    angular.module('nvControllers')
        .controller('AuthenticationCtrl', function ($rootScope, $scope, $state, $location, $uibModalStack, $stateParams, AuthenticationSrv, NotificationSrv, UtilsSrv, appConfig) {
            $scope.appConfig = appConfig;

            // Safeguard against missing versions block in payload
            if (appConfig && appConfig.versions && appConfig.versions.NeuralVyuha) {
                $scope.version = appConfig.versions.NeuralVyuha;
            } else {
                $scope.version = 'NeuralVyuha';
            }

            $scope.params = {
                requireMfa: false
            };

            $uibModalStack.dismissAll();

            $scope.ssoEnabled = function () {
                return appConfig && appConfig.config && appConfig.config.authType && appConfig.config.authType.indexOf("oauth2") !== -1;
            };


            $scope.login = function () {
                $scope.params.username = $scope.params.username.toLowerCase();
                AuthenticationSrv.login($scope.params.username, $scope.params.password, $scope.params.mfaCode)
                    .then(function () {
                        // Populate currentUser and homeState before navigating
                        return AuthenticationSrv.current();
                    })
                    .then(function () {
                        $location.search('error', null);
                        $state.go(AuthenticationSrv.currentUser.homeState || 'app.index');
                    })
                    .catch(function (err) {
                        if (err.status === 520) {
                            NotificationSrv.error('AuthenticationCtrl', err.data.message, err.status);
                        } else if (err.status === 402) {
                            $scope.params.requireMfa = true;
                        } else {
                            NotificationSrv.log(err.data.message, 'error');
                        }
                    });
            };

            var error = $location.search().error;
            if (!_.isEmpty(error)) {
                $scope.ssoError = window.decodeURIComponent(error).replace(/\+/gi, ' ', '');
            }
        });
})();
