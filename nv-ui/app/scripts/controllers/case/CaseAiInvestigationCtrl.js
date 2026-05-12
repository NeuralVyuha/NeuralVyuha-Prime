(function () {
    'use strict';
    angular.module('nvControllers').controller('CaseAiInvestigationCtrl', function ($scope, $rootScope, NvApiSrv, NotificationSrv, CaseTabsSrv, $state) {
        
        CaseTabsSrv.activateTab('ai-investigation');
        
        $scope.investigations = [];
        $scope.loading = true;

        $scope.loadInvestigations = function() {
            $scope.loading = true;
            NvApiSrv.getCaseInvestigations($scope.caseId).then(function(data) {
                $scope.investigations = data;
                $scope.loading = false;
            }).catch(function(err) {
                NotificationSrv.error('Error', 'Failed to load AI investigations');
                $scope.loading = false;
            });
        };

        $scope.rateInvestigation = function(inv, rating, $event) {
            if ($event) {
                $event.preventDefault();
                $event.stopPropagation();
            }
            
            // Toggle off if clicking the same rating
            var newRating = (inv.rating === rating) ? null : rating;
            
            NvApiSrv.rateInvestigation(inv.id, newRating, inv.feedback_comment).then(function() {
                inv.rating = newRating;
                NotificationSrv.success('Feedback Saved', 'Thank you for rating this investigation.');
            }).catch(function(err) {
                NotificationSrv.error('Error', 'Failed to save feedback.');
            });
        };

        $scope.runNewInvestigation = function() {
            var context = {
                type: 'case',
                caseId: $scope.caseId,
                title: ($scope.$parent && $scope.$parent.caze) ? $scope.$parent.caze.title : 'Investigation',
                payload: ($scope.$parent && $scope.$parent.caze) ? $scope.$parent.caze : {}
            };
            $rootScope.$broadcast('nv:toggleCopilot', context);
        };

        $scope.loadInvestigations();
    });
})();
