'use strict';

angular.module('nvControllers').controller('nvVisualVyuhaCtrl', function ($scope, $element, $timeout, $stateParams, NvApiSrv) {
    var vm = this;
    vm.caseId = $stateParams.caseId;
    vm.selectedNode = null;
    vm.loading = true;
    vm.error = null;
    var cy = null;

    vm.initGraph = function () {
        var container = document.getElementById('cy-container');
        if (!container) return; // Prevent init if container isn't ready

        NvApiSrv.getCaseGraph(vm.caseId).then(function (res) {
            vm.loading = false;
            var elements = [];

            if (res.data && res.data.nodes) {
                res.data.nodes.forEach(function (n) {
                    var size = n.node_type === 'case' ? 60 : 40;
                    elements.push({
                        data: {
                            id: n.id,
                            name: n.label || n.id,
                            type: n.node_type,
                            size: size,
                            status: n.status,
                            severity: n.severity
                        }
                    });
                });
            }

            if (res.data && res.data.edges) {
                var edgeCount = 0;
                res.data.edges.forEach(function (e) {
                    elements.push({
                        data: {
                            id: 'e' + edgeCount++,
                            source: e.source,
                            target: e.target,
                            label: e.label || ''
                        }
                    });
                });
            }

            cy = cytoscape({
                container: container,
                elements: elements,
                style: [
                    {
                        selector: 'node',
                        style: {
                            'background-color': '#3c8dbc',
                            'label': 'data(name)',
                            'color': '#fff',
                            'text-outline-color': '#222',
                            'text-outline-width': '1px',
                            'font-size': '12px',
                            'text-valign': 'bottom',
                            'text-halign': 'center',
                            'text-margin-y': '5px',
                            'width': 'data(size)',
                            'height': 'data(size)'
                        }
                    },
                    {
                        selector: 'node[type="case"]',
                        style: {
                            'background-color': '#f39c12',
                            'shape': 'hexagon'
                        }
                    },
                    {
                        selector: 'node[type="file"], node[type="hash"]',
                        style: {
                            'background-color': '#00a65a',
                            'shape': 'round-rectangle'
                        }
                    },
                    {
                        selector: 'node[type="ip"], node[type="domain"]',
                        style: {
                            'background-color': '#00c0ef',
                            'shape': 'ellipse'
                        }
                    },
                    {
                        selector: 'node[type="user"]',
                        style: {
                            'background-color': '#e08e0b',
                            'shape': 'triangle'
                        }
                    },
                    {
                        selector: 'node[type="cross_tenant"]',
                        style: {
                            'background-color': '#dd4b39',
                            'shape': 'star'
                        }
                    },
                    {
                        selector: 'node:selected',
                        style: {
                            'border-width': '4px',
                            'border-color': '#fff',
                            'background-color': '#00a65a'
                        }
                    },
                    {
                        selector: 'edge',
                        style: {
                            'width': 2,
                            'line-color': '#666',
                            'target-arrow-color': '#666',
                            'target-arrow-shape': 'triangle',
                            'curve-style': 'bezier',
                            'label': 'data(label)',
                            'font-size': '10px',
                            'color': '#aaa',
                            'text-rotation': 'autorotate'
                        }
                    }
                ],
                layout: {
                    name: 'cose',
                    padding: 50,
                    animate: true
                }
            });

            // Setup event listeners
            cy.on('tap', 'node', function (evt) {
                var node = evt.target;
                $scope.$apply(function () {
                    vm.selectedNode = node.json();
                });
            });

            cy.on('tap', function (evt) {
                if (evt.target === cy) {
                    $scope.$apply(function () {
                        vm.selectedNode = null;
                    });
                }
            });

        }).catch(function (err) {
            vm.loading = false;
            vm.error = "Error loading graph: " + (err.data ? err.data.detail : err.statusText);
        });
    };

    // Initialize graph after view renders
    $timeout(function () {
        vm.initGraph();
    }, 100);

    // Cleanup on destroy
    $scope.$on('$destroy', function () {
        if (cy) {
            cy.destroy();
        }
    });
});
