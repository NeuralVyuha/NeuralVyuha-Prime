(function () {
    'use strict';
    angular.module('nvControllers')
        .controller('CaseTasksCtrl', CaseTasksCtrl);

    function CaseTasksCtrl($scope, $state, $stateParams, $q, AuthenticationSrv, SecuritySrv, ModalUtilsSrv, FilteringSrv, CaseTabsSrv, PaginatedQuerySrv, CaseTaskSrv, NotificationSrv, CortexSrv, AppLayoutSrv, NvApiSrv) {

        CaseTabsSrv.activateTab($state.current.data.tab);

        $scope.caseId = $stateParams.caseId;
        $scope.state = {
            isNewTask: false,
            showGrouped: !!AppLayoutSrv.layout.groupTasks
        };
        $scope.priorityOpen = {};
        $scope.newTask = {
            status: 'Waiting',
            priority: 2,
            description: ''
        };
        $scope.taskResponders = null;
        $scope.userPermissions = ['manageTask', 'manageAction', 'manageAnalyse'];
        $scope.canEdit = true;
        $scope.collapseOptions = {};

        $scope.selection = [];
        $scope.menu = {
            selectAll: false
        };

        this.$onInit = function () {
            $scope.filtering = new FilteringSrv('task', 'task.list', {
                version: 'v1',
                defaults: {
                    showFilters: true,
                    showStats: false,
                    pageSize: 15,
                    sort: ['-flag', '+order', '+startDate', '+title'],
                },
                defaultFilter: []
            });

            $scope.filtering.initContext($scope.caseId)
                .then(function () {
                    $scope.load();

                    $scope.$watch('list.pageSize', function (newValue) {
                        $scope.filtering.setPageSize(newValue);
                    });
                });
        };

        $scope.getAssignableUsers = function (taskId) {
            return [
                { _name: 'getTask', idOrName: taskId },
                { _name: 'assignableUsers' }
            ];
        };

        $scope.load = function () {
            $scope.list = $scope.list || { values: [], total: 0 };
            NvApiSrv.getTasks($scope.caseId).then(function (tasks) {
                var filtered = _.filter(tasks || [], function (t) { return t.status !== 'Cancel'; });
                $scope.list.values = filtered;
                $scope.list.total = filtered.length;
                $scope.tasksCount = $scope.list.total;
                $scope.resetSelection();
            });
        };

        $scope.addTask = function () {
            if (!$scope.newTask.title) {
                return;
            }
            var task = angular.copy($scope.newTask);

            // Clean up for API
            if (task.group === 'default' || !task.group) {
                delete task.group;
            }

            NvApiSrv.createTask($scope.caseId, task).then(function () {
                NotificationSrv.success('Task has been successfully added');
                $scope.newTask = { status: 'Waiting', priority: 2, description: '' };
                $scope.state.isNewTask = false;
                $scope.load();
            }).catch(function (err) {
                NotificationSrv.error('Failed to create task', err.data);
            });
        };

        $scope.removeTask = function (task) {
            ModalUtilsSrv.confirm('Delete Task', 'Are you sure you want to delete this task? This action cannot be undone.', {
                okText: 'Yes, Delete',
                flavor: 'danger'
            })
                .then(function () {
                    return NvApiSrv.deleteTask($scope.caseId, task._id);
                })
                .then(function () {
                    NotificationSrv.success('Task has been successfully removed');
                    $scope.load();
                }).catch(function (err) {
                    if (err) {
                        NotificationSrv.error('Failed to remove task', err.data);
                    }
                });
        };

        $scope.updateField = function (field, value, task) {
            var patch = {};
            patch[field] = value;
            NvApiSrv.updateTask($scope.caseId, task._id, patch).then(function () {
                $scope.load();
            });
        };

        $scope.startTask = function (task) {
            $scope.updateField('status', 'InProgress', task);
        };

        $scope.closeTask = function (task) {
            $scope.updateField('status', 'Completed', task);
        };

        $scope.openTask = function (task) {
            $scope.updateTaskStatus(task._id, 'InProgress');
        };

        $scope.updateTaskStatus = function (taskId, newStatus) {
            NvApiSrv.updateTask($scope.caseId, taskId, { status: newStatus }).then(function () {
                NotificationSrv.success('Task status updated to ' + newStatus);
                $scope.load();
            });
        };

        $scope.priorityOptions = [
            { id: 1, label: 'Low', color: 'text-info' },
            { id: 2, label: 'Medium', color: 'text-primary' },
            { id: 3, label: 'High', color: 'text-warning' },
            { id: 4, label: 'Critical', color: 'text-danger' }
        ];

        $scope.getPriorityLabel = function (id) {
            var opt = _.find($scope.priorityOptions, { id: id });
            return opt ? opt.label : 'Medium';
        };

        $scope.getPriorityColor = function (id) {
            var opt = _.find($scope.priorityOptions, { id: id });
            return opt ? opt.color : 'text-primary';
        };

        $scope.resetSelection = function () {
            if ($scope.menu.selectAll) {
                $scope.selectAll();
            } else {
                $scope.selection = [];
                $scope.menu.selectAll = false;
                $scope.updateMenu();
            }
        };

        $scope.updateMenu = function () {
            // Handle flag/unflag menu items
            var temp = _.uniq(_.pluck($scope.selection, 'flag'));
            $scope.menu.unflag = temp.length === 1 && temp[0] === true;
            $scope.menu.flag = temp.length === 1 && temp[0] === false;

            // Handle start menu item
            temp = _.uniq(_.pluck($scope.selection, 'status'));
            $scope.menu.start = temp.length === 1 && temp[0] === 'Waiting';

            // Handle close menu item
            temp = _.uniq(_.pluck($scope.selection, 'status'));
            $scope.menu.close = temp.indexOf('Completed') === -1;

            // Handle reopen menu item
            temp = _.uniq(_.pluck($scope.selection, 'status'));
            $scope.menu.reopen = temp.length === 1 && temp[0] === 'Completed';

            $scope.menu.delete = $scope.selection.length > 0;
        };

        $scope.select = function (task) {
            if (task.selected) {
                $scope.selection.push(task);
            } else {
                $scope.selection = _.reject($scope.selection, function (item) {
                    return item._id === task._id;
                });
            }
            $scope.updateMenu();
        };

        $scope.selectAll = function () {
            var selected = $scope.menu.selectAll;

            _.each($scope.list.values, function (item) {
                // if(SecuritySrv.checkPermissions(['manageCase'], item.extraData.permissions)) {
                item.selected = selected;
                // }
            });

            if (selected) {
                $scope.selection = _.filter($scope.list.values, function (item) {
                    return !!item.selected;
                });
            } else {
                $scope.selection = [];
            }

            $scope.updateMenu();
        };

        $scope.toggleStats = function () {
            $scope.filtering.toggleStats();
        };

        $scope.toggleFilters = function () {
            $scope.filtering.toggleFilters();
        };

        $scope.filter = function () {
            $scope.filtering.filter().then($scope.applyFilters);
        };

        $scope.clearFilters = function () {
            $scope.filtering.clearFilters()
                .then($scope.search);
        };

        $scope.removeFilter = function (index) {
            $scope.filtering.removeFilter(index)
                .then($scope.search);
        };

        $scope.search = function () {
            $scope.load();
            $scope.filtering.storeContext();
        };
        $scope.addFilterValue = function (field, value) {
            $scope.filtering.addFilterValue(field, value);
            $scope.search();
        };

        $scope.filterBy = function (field, value) {
            $scope.filtering.clearFilters()
                .then(function () {
                    $scope.addFilterValue(field, value);
                });
        };

        $scope.filterMyTasks = function () {
            $scope.filtering.clearFilters()
                .then(function () {
                    var currentUser = AuthenticationSrv.currentUser;
                    $scope.filtering.addFilter({
                        field: 'assignee',
                        type: 'user',
                        value: {
                            list: [{
                                text: currentUser.login,
                                label: currentUser.name
                            }]
                        }
                    });
                    $scope.search();
                });
        };

        $scope.toggleGroupedView = function () {
            $scope.state.showGrouped = !$scope.state.showGrouped;

            AppLayoutSrv.groupTasks($scope.state.showGrouped);
        };

        $scope.buildTaskGroups = function (tasks) {
            // Sort tasks by order
            var orderedTasks = _.sortBy(_.map(tasks, function (t) {
                return _.pick(t, 'group', 'order');
            }), 'order');
            var groups = [];

            // Get group names by keeping the group orders
            _.each(orderedTasks, function (task) {
                if (groups.indexOf(task.group) === -1) {
                    groups.push(task.group);
                }
            });

            var groupedTasks = [];
            _.each(groups, function (group) {
                groupedTasks.push({
                    group: group,
                    tasks: _.filter(tasks, function (t) {
                        return t.group === group;
                    })
                });
            });

            $scope.groups = groups;
            $scope.groupedTasks = groupedTasks;
        };

        $scope.showTask = function (taskId) {
            $state.go('app.case.tasks-item', {
                itemId: taskId
            });
        };

        $scope.updateField = function (fieldName, newValue, task) {
            var field = {};
            field[fieldName] = newValue;
            return NvApiSrv.updateTask($scope.caseId, task._id, field).then(function () {
                task[fieldName] = newValue;
            }).catch(function (response) {
                NotificationSrv.error('taskList', response.data, response.status);
            });
        };



        $scope.bulkUpdate = function (ids, patch) {
            return CaseTaskSrv.bulkUpdate(ids, patch)
                .then(function (/*responses*/) {
                    NotificationSrv.log('Selected tasks have been updated successfully', 'success');
                })
                .catch(function (err) {
                    NotificationSrv.error('Bulk update tasks', err.data, err.status);
                });
        };

        $scope.bulkFlag = function (flag) {
            var ids = _.pluck($scope.selection, '_id');

            return $scope.bulkUpdate(ids, { flag: flag });
        };

        $scope.bulkStatus = function (status) {
            var ids = _.pluck($scope.selection, '_id');

            return $scope.bulkUpdate(ids, { status: status });
        };

        $scope.bulkRemove = function () {
            var ids = _.pluck($scope.selection, '_id');

            ModalUtilsSrv.confirm('Delete selected tasks', 'Are you sure you want to delete the selected tasks?', {
                okText: 'Yes, proceed',
                flavor: 'danger'
            }).then(function () {
                return CaseTaskSrv.bulkUpdate(ids, { status: 'Cancel' })
                    .then(function (/*responses*/) {
                        NotificationSrv.log('Selected tasks have been successfully removed', 'success');

                        _.each($scope.selection, function (task) {
                            $scope.$emit('tasks:task-removed', task);
                        });
                    })
                    .catch(function (err) {
                        NotificationSrv.error('Bulk remove tasks', err.data, err.status);
                    });
            }).catch(function (err) {
                if (err && !_.isString(err)) {
                    NotificationSrv.error('Bulk remove tasks', err.data, err.status);
                }
            });
        };

        // open task tab with its details
        $scope.startTask = function (task) {
            var taskId = task._id;

            if (task.status === 'Waiting') {
                $scope.updateTaskStatus(taskId, 'InProgress')
                    .then(function (/*response*/) {
                        $scope.showTask(taskId);
                    });
            } else {
                $scope.showTask(taskId);
            }
        };

        $scope.openTask = function (task) {
            if (task.status === 'Completed') {
                $scope.updateTaskStatus(task._id, 'InProgress')
                    .then(function (/*response*/) {
                        $scope.showTask(task._id);
                    });
            }
        };

        $scope.closeTask = function (task) {
            if (task.status === 'InProgress') {
                $scope.updateTaskStatus(task._id, 'Completed')
                    .then(function () {
                        NotificationSrv.success('Task has been successfully closed');
                    });
            }
        };

        $scope.updateTaskStatus = function (task, status) {
            var taskId = _.isString(task) ? task : task._id;
            return NvApiSrv.updateTask($scope.caseId, taskId, { status: status }).then(function (data) {
                $scope.load();
                return data;
            }).catch(function (response) {
                NotificationSrv.error('taskList', response.data, response.status);
                return $q.reject(response);
            });
        };

        $scope.getTaskResponders = function (task, force) {
            if (!force && $scope.taskResponders !== null) {
                return;
            }

            $scope.taskResponders = null;
            CortexSrv.getResponders('case_task', task._id)
                .then(function (responders) {
                    $scope.taskResponders = responders;
                    return CortexSrv.promntForResponder(responders);
                })
                .then(function (response) {
                    if (response && _.isString(response)) {
                        NotificationSrv.log(response, 'warning');
                    } else {
                        return CortexSrv.runResponder(response.id, response.name, 'case_task', _.pick(task, '_id'));
                    }
                })
                .then(function (response) {
                    NotificationSrv.success(['Responder', response.data.responderName, 'started successfully on task', task.title].join(' '));
                })
                .catch(function (err) {
                    if (err && !_.isString(err)) {
                        NotificationSrv.error('taskList', err.data, err.status);
                    }
                });
        };

        // $scope.runResponder = function(responderId, responderName, task) {
        //     CortexSrv.runResponder(responderId, responderName, 'case_task', _.pick(task, '_id'))
        //       .then(function(response) {
        //           NotificationSrv.success(['Responder', response.data.responderName, 'started successfully on task', task.title].join(' '));
        //       })
        //       .catch(function(response) {
        //           if(response && !_.isString(response)) {
        //               NotificationSrv.error('taskList', response.data, response.status);
        //           }
        //       });
        // };
    }
}());
