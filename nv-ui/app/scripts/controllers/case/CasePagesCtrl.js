(function () {
    'use strict';

    /* @ngInject */
    function CasePagesCtrl($scope, $stateParams, NvApiSrv, NotificationSrv, caze) {
        var caseId = $stateParams.caseId;
        $scope.caze = caze;
        $scope.pages = [];
        $scope.isLoading = true;
        $scope.isEditing = false;
        $scope.currentEditingPage = null;

        $scope.newPage = {
            title: '',
            content: ''
        };

        // Fetch Case Pages
        function loadPages() {
            $scope.isLoading = true;
            NvApiSrv.getPages(caseId)
                .then(function (data) {
                    $scope.pages = data;
                })
                .catch(function (error) {
                    NotificationSrv.error('CasePagesCtrl', 'Failed to load pages');
                })
                .finally(function () {
                    $scope.isLoading = false;
                });
        }

        $scope.startAdd = function () {
            $scope.isEditing = true;
            $scope.currentEditingPage = null;
            $scope.newPage = { title: '', content: '' };
        };

        $scope.startEdit = function (page) {
            $scope.isEditing = true;
            $scope.currentEditingPage = angular.copy(page);
        };

        $scope.cancelEdit = function () {
            $scope.isEditing = false;
            $scope.currentEditingPage = null;
        };

        // Create or Update a Page
        $scope.savePage = function () {
            var pageData = $scope.currentEditingPage || $scope.newPage;

            if (!pageData.title || !pageData.content) {
                NotificationSrv.error('CasePagesCtrl', 'Title and content are required');
                return;
            }

            if ($scope.currentEditingPage && $scope.currentEditingPage.id) {
                // Update
                NvApiSrv.updatePage(caseId, $scope.currentEditingPage.id, {
                    title: $scope.currentEditingPage.title,
                    content: $scope.currentEditingPage.content
                }).then(function () {
                    NotificationSrv.success('Page updated successfully');
                    $scope.isEditing = false;
                    loadPages();
                });
            } else {
                // Create
                NvApiSrv.createPage(caseId, $scope.newPage)
                    .then(function () {
                        NotificationSrv.success('Page created successfully');
                        $scope.isEditing = false;
                        $scope.newPage = { title: '', content: '' };
                        loadPages();
                    });
            }
        };

        // Delete a Page
        $scope.deletePage = function (pageId) {
            if (!confirm("Are you sure you want to delete this page?")) return;

            NvApiSrv.deletePage(caseId, pageId)
                .then(function () {
                    NotificationSrv.success('Page deleted successfully');
                    loadPages();
                });
        };

        // Init
        loadPages();
    }


    angular.module('nvControllers').controller('CasePagesCtrl', CasePagesCtrl);
})();
