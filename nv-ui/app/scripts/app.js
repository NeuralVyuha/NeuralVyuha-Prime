'use strict';

// Define sub-modules first to ensure they exist for other scripts
angular.module('nvControllers', ['nvServices']);
angular.module('nvServices', []);
angular.module('nvDirectives', []);
angular.module('nvFilters', []);
angular.module('nvComponents', []);
angular.module('nvCore', []);

angular.module('neuralvyuha', [
    'ngAnimate',
    'ngCookies',
    'ngResource',
    'ngSanitize',
    'ngTouch',
    'ngMessages',
    'ui.bootstrap',
    'ui.router',
    'ui-notification',
    'LocalStorageModule',
    'angularMoment',
    'timer',
    'ngTagsInput',
    'ngFileUpload',
    'angular-clipboard',
    'hc.marked',
    'hljs',
    'angular-markdown-editor',
    'ui.ace',
    'angular-page-loader',
    'images-resizer',
    'naif.base64',
    'ui.sortable',
    'duScroll',
    'dndLists',
    'colorpicker.module',
    'btorfs.multiselect',
    'ja.qr',
    'nvControllers',
    'nvServices',
    'nvDirectives',
    'nvFilters',
    'nvComponents',
    'nvCore'
])
    .factory('UrlParser', function () {
        // Dummy factory injected for legacy v4 compatibility
        return {
            parse: function (url) { return url; }
        };
    })
    .factory('appConfig', function (VersionSrv, $q) {
        // Global appConfig service - wraps VersionSrv.get() with a safe fallback
        var defaultConfig = {
            config: {
                capabilities: [],
                pollingDuration: 3000,
                authType: [],
                ssoAutoLogin: false,
                protectDownloadsWith: null,
                freeTagDefaultColour: '#000000'
            },
            connectors: {
                cortex: { enabled: false, servers: [] },
                misp: { enabled: false, servers: [] }
            },
            versions: { NeuralVyuha: 'NeuralVyuha' },
            schemaStatus: 'OK'
        };

        return VersionSrv.get().then(function (config) {
            return config;
        }, function () {
            // If backend is unavailable, return safe defaults
            return defaultConfig;
        });
    })
    .config(function ($stateProvider, $urlRouterProvider, $httpProvider, NotificationProvider) {
        // Default route
        $urlRouterProvider.otherwise('/nv/dashboard');

        // Ensure parent case route cleanly redirects to details regardless of the UUID format
        $urlRouterProvider.when(/^\/case\/([a-zA-Z0-9_\-]+)$/i, '/case/$1/details');

        NotificationProvider.setOptions({
            delay: 5000,
            startTop: 20,
            startRight: 10,
            verticalSpacing: 20,
            horizontalSpacing: 20,
            positionX: 'right',
            positionY: 'top'
        });

        $stateProvider
            .state('login', {
                url: '/login',
                templateUrl: 'views/login.html',
                controller: 'AuthenticationCtrl',
                resolve: {
                    appConfig: function (appConfig) {
                        return appConfig;
                    }
                }
            })
            .state('app', {
                abstract: true,
                templateUrl: 'views/app.html',
                controller: 'RootCtrl',
                resolve: {
                    currentUser: function (AuthenticationSrv, $q) {
                        return AuthenticationSrv.current().catch(function (err) {
                            return $q.reject(err);
                        });
                    },
                    appConfig: function (appConfig) {
                        return appConfig;
                    }
                }
            })
            .state('app.index', {
                url: '/nv/dashboard',
                templateUrl: 'views/nv/nvGroupListV2.html',
                controller: 'NvGroupListCtrl',
                controllerAs: 'vm'
            })
            // Phase 3 — Enterprise Upgrade Restorations
            .state('app.alerts', {
                url: '/nv/alerts',
                templateUrl: 'views/nv/nvAlerts.html',
                controller: 'NvAlertListCtrl',
                controllerAs: 'vm'
            })
            .state('app.ingest-control', {
                url: '/nv/ingest-control',
                templateUrl: 'views/nv/ingestion-control.html',
                controller: 'NvIngestionControlCtrl',
                controllerAs: 'vm'
            })
            .state('app.threat-hunter', {
                url: '/nv/threat-hunter',
                templateUrl: 'views/nv/threat-hunter.html',
                controller: 'NvThreatHunterCtrl',
                controllerAs: 'vm'
            })
            .state('app.cases', {
                url: '/nv/cases',
                templateUrl: 'views/nv/nvCasesV2.html',
                controller: 'NvCaseListCtrl',
                controllerAs: 'vm'
            })
            .state('app.nvcase', {
                url: '/nv/cases/:caseId',
                templateUrl: 'views/app.case.html',
                controller: 'CaseMainCtrl',
                resolve: {
                    caze: function (CaseSrv, $stateParams) {
                        return CaseSrv.getById($stateParams.caseId, true);
                    }
                }
            })
            .state('app.nvcase.details', {
                url: '/details',
                templateUrl: 'views/partials/case/case.details.html',
                controller: 'CaseDetailsCtrl',
                data: { tab: 'details' }
            })
            .state('app.nvcase.pages', {
                url: '/pages',
                templateUrl: 'views/partials/case/case.pages.html',
                controller: 'CasePagesCtrl',
                data: { tab: 'pages' }
            })
            .state('app.nvcase.tasks', {
                url: '/tasks',
                templateUrl: 'views/partials/case/case.tasks.html',
                controller: 'CaseTasksCtrl',
                data: { tab: 'tasks' }
            })
            .state('app.nvcase.tasks-item', {
                url: '/tasks/:itemId',
                templateUrl: 'views/partials/case/case.tasks.item.html',
                controller: 'CaseTasksItemCtrl',
                data: { tab: 'tasks' },
                resolve: {
                    task: function ($http, $stateParams) {
                        return $http.get('./api/tasks/' + $stateParams.itemId)
                            .then(function (res) {
                                var t = res.data;
                                if (!t.extraData) t.extraData = {};
                                if (!t.extraData.actionRequiredMap) t.extraData.actionRequiredMap = {};
                                return t;
                            })
                            .catch(function () {
                                return {
                                    _id: $stateParams.itemId,
                                    id: $stateParams.itemId,
                                    title: 'Task',
                                    status: 'Waiting',
                                    flag: false,
                                    extraData: { shareCount: 0, actionRequired: false, actionRequiredMap: {} }
                                };
                            });
                    }
                }
            })
            .state('app.nvcase.observables', {
                url: '/observables',
                templateUrl: 'views/partials/case/case.observables.html',
                controller: 'CaseObservablesCtrl',
                data: { tab: 'observables' }
            })
            .state('app.nvcase.observables-item', {
                url: '/observables/:itemId',
                templateUrl: 'views/partials/case/case.observables.item.html',
                controller: 'CaseObservablesItemCtrl',
                data: { tab: 'observables' },
                resolve: {
                    artifact: function ($stateParams, CaseArtifactSrv) {
                        return CaseArtifactSrv.api().get({ artifactId: $stateParams.itemId }).$promise
                            .catch(function (err) {
                                return { _id: $stateParams.itemId, id: $stateParams.itemId, dataType: 'unknown', data: 'Error Loading Observable' };
                            });
                    }
                }
            })
            .state('app.nvcase.sharing', {
                url: '/sharing',
                templateUrl: 'views/partials/case/case.sharing.html',
                controller: 'CaseSharingCtrl',
                data: { tab: 'sharing' },
                resolve: {
                    organisations: function () { return []; },
                    profiles: function () { return []; },
                    shares: function ($http, $stateParams) {
                        return $http.get('./api/case/' + $stateParams.caseId + '/shares')
                            .then(function (r) { return r.data; })
                            .catch(function () { return []; });
                    }
                }
            })
            .state('app.nvcase.procedures', {
                url: '/procedures',
                templateUrl: 'views/partials/case/case.ttps.html',
                controller: 'CaseTtpsCtrl',
                data: { tab: 'procedures' }
            })
            .state('app.nvcase.ai-investigation', {
                url: '/ai-investigation',
                templateUrl: 'views/partials/case/case.ai-investigation.html',
                controller: 'CaseAiInvestigationCtrl',
                data: { tab: 'ai-investigation' }
            })
            // Legacy / Active Direct Links
            .state('app.case', {
                url: '/case/:caseId',
                templateUrl: 'views/app.case.html',
                controller: 'CaseMainCtrl',
                resolve: {
                    caze: function (CaseSrv, $stateParams) {
                        return CaseSrv.getById($stateParams.caseId, true);
                    }
                }
            })
            .state('app.case.details', {
                url: '/details',
                templateUrl: 'views/partials/case/case.details.html',
                controller: 'CaseDetailsCtrl',
                data: { tab: 'details' }
            })
            .state('app.case.pages', {
                url: '/pages',
                templateUrl: 'views/partials/case/case.pages.html',
                controller: 'CasePagesCtrl',
                data: { tab: 'pages' }
            })
            .state('app.case.tasks', {
                url: '/tasks',
                templateUrl: 'views/partials/case/case.tasks.html',
                controller: 'CaseTasksCtrl',
                data: { tab: 'tasks' }
            })
            .state('app.case.tasks-item', {
                url: '/tasks/:itemId',
                templateUrl: 'views/partials/case/case.tasks.item.html',
                controller: 'CaseTasksItemCtrl',
                data: { tab: 'tasks' },
                resolve: {
                    task: function ($http, $stateParams) {
                        return $http.get('./api/tasks/' + $stateParams.itemId)
                            .then(function (res) {
                                var t = res.data;
                                if (!t.extraData) t.extraData = {};
                                if (!t.extraData.actionRequiredMap) t.extraData.actionRequiredMap = {};
                                return t;
                            })
                            .catch(function () {
                                return {
                                    _id: $stateParams.itemId,
                                    id: $stateParams.itemId,
                                    title: 'Task',
                                    status: 'Waiting',
                                    flag: false,
                                    extraData: { shareCount: 0, actionRequired: false, actionRequiredMap: {} }
                                };
                            });
                    }
                }
            })
            .state('app.case.observables', {
                url: '/observables',
                templateUrl: 'views/partials/case/case.observables.html',
                controller: 'CaseObservablesCtrl',
                data: { tab: 'observables' }
            })
            .state('app.case.observables-item', {
                url: '/observables/:itemId',
                templateUrl: 'views/partials/case/case.observables.item.html',
                controller: 'CaseObservablesItemCtrl',
                data: { tab: 'observables' },
                resolve: {
                    artifact: function ($stateParams, CaseArtifactSrv) {
                        return CaseArtifactSrv.api().get({ artifactId: $stateParams.itemId }).$promise
                            .catch(function (err) {
                                return { _id: $stateParams.itemId, id: $stateParams.itemId, dataType: 'unknown', data: 'Error Loading Observable' };
                            });
                    }
                }
            })
            .state('app.case.sharing', {
                url: '/sharing',
                templateUrl: 'views/partials/case/case.sharing.html',
                controller: 'CaseSharingCtrl',
                data: { tab: 'sharing' },
                resolve: {
                    organisations: function () { return []; },
                    profiles: function () { return []; },
                    shares: function ($http, $stateParams) {
                        return $http.get('./api/case/' + $stateParams.caseId + '/shares')
                            .then(function (r) { return r.data; })
                            .catch(function () { return []; });
                    }
                }
            })
            .state('app.case.procedures', {
                url: '/procedures',
                templateUrl: 'views/partials/case/case.ttps.html',
                controller: 'CaseTtpsCtrl',
                data: { tab: 'procedures' }
            })
            .state('app.case.ai-investigation', {
                url: '/ai-investigation',
                templateUrl: 'views/partials/case/case.ai-investigation-v4.html',
                controller: 'CaseAiInvestigationCtrl',
                data: { tab: 'ai-investigation' }
            })
            .state('app.case.ai-chat', {
                url: '/ai-chat',
                templateUrl: 'views/partials/case/case.ai-chat.html?v=1003',
                controller: 'CaseAiChatCtrl',
                data: { tab: 'ai-chat' }
            })
            // Phase U1 — Zenith: Visual Vyuha Route
            .state('app.graph', {
                url: '/nv/graph/:caseId',
                templateUrl: 'views/nv/visual-vyuha.html',
                controller: 'nvVisualVyuhaCtrl',
                controllerAs: 'vm'
            })
            .state('app.admin', {
                url: '/nv/admin',
                templateUrl: 'views/nv/nvAdminV2.html',
                controller: 'NvAdminCtrl',
                controllerAs: 'vm'
            })
            .state('app.profile', {
                url: '/nv/profile',
                templateUrl: 'views/nv/nvProfile.html',
                controller: 'NvProfileCtrl',
                controllerAs: 'vm'
            })
            .state('app.integrations', {
                url: '/nv/integrations',
                templateUrl: 'views/nv/integrations.html',
                controller: 'NvIntegrationsCtrl',
                controllerAs: 'vm'
            })
            .state('app.vyuha-graph', {
                url: '/nv/vyuha-graph',
                templateUrl: 'views/nv/vyuha-graph.html',
                controller: 'NvVyuhaGraphCtrl',
                controllerAs: 'vm'
            })
            .state('app.nv-help', {
                url: '/nv/help',
                templateUrl: 'views/nv/help.html',
                controller: 'NvHelpCtrl',
                controllerAs: 'vm'
            });
    })
    .run(function ($rootScope, $state, AuthenticationSrv, NvConfig) {
        // Initialization logic
        $rootScope.$on('$stateChangeStart', function (event, toState) {
            // Allow transition to proceed. If AuthenticationSrv.currentUser is missing,
            // the 'app' state's resolve block for 'currentUser' will catch it,
            // call AuthenticationSrv.current(), and if that fails, RootCtrl will redirect to login.
            if (toState.name === 'login' && AuthenticationSrv.currentUser) {
                event.preventDefault();
                $state.go('app.index');
            }
        });
    });
