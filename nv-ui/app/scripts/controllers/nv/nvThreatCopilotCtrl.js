(function () {
    'use strict';

    angular.module('nvControllers')
        .controller('nvThreatCopilotCtrl', ['$scope', '$http', '$timeout', 'NvApiSrv', 'NotificationSrv', function ($scope, $http, $timeout, NvApiSrv, NotificationSrv) {
        var vm = this;

        vm.visible = false;
        vm.loading = false;
        vm.isSaving = false;
        vm.messages = [];
        vm.inputMessage = '';
        vm.modelProvider = 'gemini'; 
        vm.modelName = 'gemini-2.5-flash';
        vm.context = null;

        // Mapping of providers to their available models (as provided by the user)
        vm.availableModels = {
            'gemini': ['gemini-2.5-flash', 'gemini-1.5-pro'],
            'ollama': [
                'qwen2.5-coder:7b',
                'qwen2.5:7b-instruct',
                'deepseek-r1:8b',
                'mistral:latest',
                'kimi-k2.5:cloud',
                'gpt-oss:120b-cloud'
            ],
            'moonshot': ['kimi-k2.5', 'kimi-v1'],
            'deepseek': ['deepseek-chat', 'deepseek-reasoner'],
            'openai': ['gpt-4o', 'gpt-4-turbo', 'gpt-3.5-turbo'],
            'claude': ['claude-3-5-sonnet-20240620', 'claude-3-opus-20240229']
        };

        // Update model name when provider changes
        $scope.$watch('copilot.modelProvider', function(newProvider) {
            if (newProvider && vm.availableModels[newProvider]) {
                vm.modelName = vm.availableModels[newProvider][0];
            }
        });

        $scope.$on('nv:toggleCopilot', function (event, contextPayload) {
            vm.visible = !vm.visible;
            if (vm.visible && contextPayload) {
                vm.context = contextPayload;
                // Auto-trigger investigation
                var type = contextPayload.type === 'alert' ? 'Alert' : 'Case';
                var id = contextPayload.alertId || contextPayload.caseId || 'N/A';
                vm.inputMessage = "Investigate this " + type + " [" + id + "]: " + contextPayload.title + ". Check logs and correlate data.";
                $timeout(function() {
                    vm.sendMessage();
                }, 500);
            }
        });

        vm.toggle = function () {
            vm.visible = !vm.visible;
        };

        vm.close = function () {
            vm.visible = false;
        };

        vm.sendMessage = function () {
            if (!vm.inputMessage.trim()) return;

            var userMsg = vm.inputMessage.trim();
            var assistantMsg = { role: 'assistant', content: '', thoughts: [], isStreaming: true };
            
            vm.messages.push({ role: 'user', content: userMsg });
            vm.messages.push(assistantMsg);
            vm.inputMessage = '';
            vm.loading = true; 
            console.log('Sending message:', payload);
            
            $timeout(function() {
                _scrollToBottom();
            }, 100);

            var payload = {
                message: userMsg,
                model_provider: vm.modelProvider,
                model_name: vm.modelName,
                context: vm.context || {}
            };

            var headers = {
                'Content-Type': 'application/json',
                'Accept': 'text/event-stream'
            };
            if ($http.defaults.headers.common.Authorization) {
                headers['Authorization'] = $http.defaults.headers.common.Authorization;
            }

            fetch('/mcp-service/copilot/chat', {
                method: 'POST',
                headers: headers,
                body: JSON.stringify(payload)
            }).then(function(response) {
                console.log('Fetch response received:', response.status);
                if (!response.ok) {
                    throw new Error('HTTP ' + response.status);
                }
                vm.loading = false; 
                var reader = response.body.getReader();
                var decoder = new TextDecoder('utf-8');
                
                function readStream() {
                    reader.read().then(function(result) {
                        if (result.done) {
                            $scope.$apply(function() {
                                assistantMsg.isStreaming = false;
                            });
                            return;
                        }
                        
                        var chunk = decoder.decode(result.value, {stream: true});
                        var lines = chunk.split('\n');
                        
                        $scope.$apply(function() {
                            lines.forEach(function(line) {
                                if (line.startsWith('data: ')) {
                                    var dataStr = line.replace('data: ', '').trim();
                                    if (dataStr === '[DONE]') {
                                        assistantMsg.isStreaming = false;
                                    } else if (dataStr) {
                                        try {
                                            var data = JSON.parse(dataStr);
                                            console.log('MCP Copilot Data:', data);
                                            if (data.type === 'thought') {
                                                assistantMsg.thoughts.push(data.content);
                                            } else if (data.type === 'message') {
                                                assistantMsg.content += data.content;
                                            } else if (data.type === 'error') {
                                                assistantMsg.content += '\n\n**Error**: ' + data.content;
                                            }
                                        } catch(e) {}
                                    }
                                }
                            });
                            _scrollToBottom();
                        });
                        readStream();
                    }).catch(function(err) {
                        $scope.$apply(function() {
                            assistantMsg.content = "Stream interrupted.";
                            assistantMsg.isStreaming = false;
                            vm.loading = false;
                        });
                    });
                }
                readStream();
            }).catch(function(err) {
                $scope.$apply(function() {
                    vm.messages.push({ role: 'system', content: 'Connection Error: ' + err.message });
                    vm.loading = false;
                    _scrollToBottom();
                });
            });
        };
        
        vm.saveToCase = function () {
            if (!vm.messages || vm.messages.length <= 1) {
                NotificationSrv.warning('Empty', 'No investigation data to save.');
                return;
            }
            if (!vm.context || !vm.context.caseId) {
                NotificationSrv.error('Error', 'No case context found to save investigation.');
                return;
            }

            vm.isSaving = true;
            var payload = {
                investigation_data: {
                    messages: vm.messages.filter(function(m) { return m.role !== 'system'; })
                },
                model_provider: vm.modelProvider,
                model_name: vm.modelName
            };

            NvApiSrv.saveCaseInvestigation(vm.context.caseId, payload).then(function (res) {
                NotificationSrv.success('Success', 'AI Investigation saved to case tab.');
                vm.isSaving = false;
            }).catch(function (err) {
                NotificationSrv.error('Error', 'Failed to save investigation.');
                vm.isSaving = false;
            });
        };

        function _scrollToBottom() {
            $timeout(function () {
                var el = document.getElementById('copilot-message-list');
                if (el) {
                    el.scrollTop = el.scrollHeight;
                }
            }, 100);
        }
    }]);
})();
