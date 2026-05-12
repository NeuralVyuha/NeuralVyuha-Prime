/**
 * nvProfileCtrl.js — User Profile & Settings Page
 */
(function () {
    'use strict';

    angular.module('nvControllers').controller('NvProfileCtrl', function (
        $scope, $http, $timeout, AuthenticationSrv, NotificationSrv, $state
    ) {
        var vm = this;

        // ── State ──────────────────────────────────────────────────────────
        vm.user = {};
        vm.loading = true;
        vm.saving = false;
        vm.pwForm = { current: '', newPw: '', confirm: '' };
        vm.pwError = '';
        vm.pwSuccess = false;
        vm.tokenInfo = {};

        // ── Load Profile ───────────────────────────────────────────────────
        vm.loadProfile = function () {
            vm.loading = true;
            var token = localStorage.getItem('nv_token');
            var headers = {};
            if (token) { headers.Authorization = 'Bearer ' + token; }

            $http.get('/api/v1/user/current', { headers: headers })
                .then(function (res) {
                    vm.user = res.data || {};
                    vm.user.displayName = vm.user.name || '';

                    // Decode token to show expiry
                    if (token) {
                        try {
                            var parts = token.split('.');
                            var payload = JSON.parse(atob(parts[1]));
                            vm.tokenInfo = {
                                exp: payload.exp ? new Date(payload.exp * 1000) : null,
                                iat: payload.iat ? new Date(payload.iat * 1000) : null,
                                sub: payload.sub,
                                tenant: payload.tenant_id,
                                roles: payload.roles || []
                            };
                        } catch (e) {
                            vm.tokenInfo = { error: 'Could not decode token' };
                        }
                    }
                })
                .catch(function () {
                    vm.user = AuthenticationSrv.currentUser || {};
                })
                .finally(function () { vm.loading = false; });
        };

        // ── Change Password ────────────────────────────────────────────────
        vm.changePassword = function () {
            vm.pwError = '';
            vm.pwSuccess = false;

            if (!vm.pwForm.newPw || vm.pwForm.newPw.length < 8) {
                vm.pwError = 'New password must be at least 8 characters.';
                return;
            }
            if (vm.pwForm.newPw !== vm.pwForm.confirm) {
                vm.pwError = 'New password and confirmation do not match.';
                return;
            }

            vm.saving = true;
            var token = localStorage.getItem('nv_token');
            $http.post('/api/v1/user/' + (vm.user.login || 'admin') + '/password/change', {
                currentPassword: vm.pwForm.current,
                password: vm.pwForm.newPw
            }, { headers: { Authorization: 'Bearer ' + token } })
                .then(function () {
                    vm.pwSuccess = true;
                    vm.pwForm = { current: '', newPw: '', confirm: '' };
                    $timeout(function () { vm.pwSuccess = false; }, 4000);
                })
                .catch(function (err) {
                    vm.pwError = (err && err.data && err.data.detail) || 'Password change failed. Ensure current password is correct.';
                })
                .finally(function () { vm.saving = false; });
        };

        // ── Logout ─────────────────────────────────────────────────────────
        vm.logout = function () {
            AuthenticationSrv.logout(function () {
                $state.go('login');
            });
        };

        // ── Lifecycle ──────────────────────────────────────────────────────
        vm.loadProfile();
    });
})();
