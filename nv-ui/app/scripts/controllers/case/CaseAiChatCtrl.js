(function () {
    'use strict';

    angular.module('nvControllers').controller('CaseAiChatCtrl', function ($scope, $rootScope, $stateParams, $http, $timeout, NvApiSrv, NotificationSrv, CaseTabsSrv) {
        
        // Register Tab
        CaseTabsSrv.activateTab('ai-chat');

        $scope.caseId = $stateParams.caseId;
        $scope.displayId = $scope.caseId; // Default to UUID

        // Watch for parent caze object to get the friendly number (e.g. 8024)
        $scope.$watch('caze.number', function(newVal) {
            if (newVal) {
                $scope.displayId = newVal;
            }
        });
        
        $scope.loading = false;
        $scope.isSaving = false;
        $scope.messages = [];
        $scope.inputMessage = '';
        $scope.modelProvider = 'gemini'; 
        $scope.modelName = 'gemini-2.5-flash';

        $scope.availableModels = {
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

        $scope.$watch('modelProvider', function(newProvider) {
            if (newProvider && $scope.availableModels[newProvider]) {
                $scope.modelName = $scope.availableModels[newProvider][0];
            }
        });

        // Initialize Context from Parent Scope (CaseMainCtrl provides $scope.caze)
        function getCaseContext() {
            var ctx = {
                type: 'case',
                caseId: $scope.caseId
            };
            if ($scope.caze) {
                ctx.title = $scope.caze.title;
                ctx.description = $scope.caze.description;
                ctx.severity = $scope.caze.severity;
                ctx.tags = $scope.caze.tags;
                ctx.status = $scope.caze.status;
            }
            return ctx;
        }

        $scope.clearChat = function() {
            $scope.messages = [];
        };

        $scope.sendMessage = function () {
            if (!$scope.inputMessage.trim()) return;

            var userMsg = $scope.inputMessage.trim();
            var assistantMsg = { role: 'assistant', content: '', thoughts: [], isStreaming: true };
            
            $scope.messages.push({ role: 'user', content: userMsg });
            $scope.messages.push(assistantMsg);
            $scope.inputMessage = '';
            $scope.loading = true; 
            
            $timeout(function() {
                _scrollToBottom();
            }, 100);

            var payload = {
                message: userMsg,
                model_provider: $scope.modelProvider,
                model_name: $scope.modelName,
                context: getCaseContext()
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
                if (!response.ok) {
                    throw new Error('HTTP ' + response.status);
                }
                $scope.$apply(function() {
                    $scope.loading = false;
                });
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
                            $scope.loading = false;
                        });
                    });
                }
                readStream();
            }).catch(function(err) {
                $scope.$apply(function() {
                    $scope.messages.push({ role: 'system', content: 'Connection Error: ' + err.message });
                    $scope.loading = false;
                    _scrollToBottom();
                });
            });
        };
        
        $scope.saveToCase = function () {
            if (!$scope.messages || $scope.messages.length <= 1) {
                NotificationSrv.warning('Empty', 'No conversation to save.');
                return;
            }

            $scope.isSaving = true;
            var payload = {
                investigation_data: {
                    messages: $scope.messages.filter(function(m) { return m.role !== 'system'; })
                },
                model_provider: $scope.modelProvider,
                model_name: $scope.modelName
            };

            NvApiSrv.saveCaseInvestigation($scope.caseId, payload).then(function (res) {
                NotificationSrv.success('Success', 'Chat transcript saved to AI Investigation history.');
                $scope.isSaving = false;
                // Optionally navigate to AI Investigation tab
                // $state.go('app.case.ai-investigation', { caseId: $scope.caseId });
            }).catch(function (err) {
                NotificationSrv.error('Error', 'Failed to save chat transcript.');
                $scope.isSaving = false;
            });
        };

        function _scrollToBottom() {
            $timeout(function () {
                var el = document.getElementById('case-ai-chat-messages');
                if (el) {
                    el.scrollTop = el.scrollHeight;
                }
            }, 100);
        }
    });

})();
