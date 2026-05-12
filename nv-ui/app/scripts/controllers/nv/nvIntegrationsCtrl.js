/**
 * nvIntegrationsCtrl.js — Full-Scale Integration Hub Controller
 *
 * Works entirely client-side using localStorage when the backend
 * (nv-node-service) is unavailable.  When the backend IS reachable
 * it syncs automatically.
 *
 * Features:
 *  - 12-type integration catalog
 *  - Add / Edit / Delete / Test nodes
 *  - Category filter + text search
 *  - Export / Import config as JSON
 *  - Browser-side connection testing (CORS-safe ping)
 *  - Stats bar (UP / DOWN / DEGRADED counts)
 *  - Toast notification system
 */
(function () {
    'use strict';

    angular.module('nvControllers').controller('NvIntegrationsCtrl', function (
        $scope, $http, $interval, $timeout
    ) {

        var vm = this;
        var STORAGE_KEY = 'nv_integration_nodes';
        var _pollHandle = null;

        // ================================================================
        //  CATALOG — all supported integration types
        // ================================================================
        vm.catalog = [
            { id: 'cortex',       name: 'Cortex',          icon: '🧠', color: '#9b77e8', category: 'analyzer',  requiresKey: true,  tags: ['ip','domain','url','hash'],            desc: 'Elastic analyzer engine for threat intelligence enrichment' },
            { id: 'misp',         name: 'MISP',            icon: '🔴', color: '#e05c6a', category: 'intel',     requiresKey: true,  tags: ['ip','domain','url','hash'],            desc: 'Open-source threat intelligence sharing platform' },
            { id: 'virustotal',   name: 'VirusTotal',      icon: '🦠', color: '#394EFF', category: 'analyzer',  requiresKey: true,  tags: ['ip','domain','url','hash'],            desc: 'Multi-engine malware scanner and URL/IP reputation. 500 lookups/day free' },
            { id: 'abuseipdb',    name: 'AbuseIPDB',       icon: '🚫', color: '#e05c6a', category: 'intel',     requiresKey: true,  tags: ['ip'],                                  desc: 'IP abuse & malicious activity reports. 1,000 checks/day free' },
            { id: 'alienvault',   name: 'AlienVault OTX',  icon: '👽', color: '#3ecf8e', category: 'intel',     requiresKey: true,  tags: ['ip','domain','url','hash'],            desc: 'Open Threat Exchange — unlimited free community threat intel' },
            { id: 'shodan',       name: 'Shodan',          icon: '🔭', color: '#e05c6a', category: 'recon',     requiresKey: true,  tags: ['ip'],                                  desc: 'Internet-wide scanner. Free: 100 results/month' },
            { id: 'urlhaus',      name: 'URLhaus',         icon: '🌐', color: '#3ecf8e', category: 'intel',     requiresKey: false, tags: ['url','domain','hash'],                 desc: 'Abuse.ch — malicious URL tracking. No API key needed' },
            { id: 'malwarebazaar',name: 'MalwareBazaar',   icon: '💾', color: '#f5c842', category: 'intel',     requiresKey: false, tags: ['hash'],                                desc: 'Abuse.ch — malware hash lookup. No API key needed' },
            { id: 'threatfox',    name: 'ThreatFox',       icon: '🦊', color: '#e05c6a', category: 'intel',     requiresKey: false, tags: ['ip','domain','url','hash'],            desc: 'Abuse.ch — IOC sharing. No API key needed' },
            { id: 'greynoise',    name: 'GreyNoise',       icon: '📡', color: '#4a9eff', category: 'recon',     requiresKey: true,  tags: ['ip'],                                  desc: 'Internet scan context — distinguishes noise from targeted attacks. Community tier free' },
            { id: 'ipinfo',       name: 'IPInfo',          icon: '🌍', color: '#3ecf8e', category: 'recon',     requiresKey: true,  tags: ['ip'],                                  desc: 'IP geolocation, ASN, org data. 50k lookups/month free' },
            { id: 'phishtank',    name: 'PhishTank',       icon: '🎣', color: '#f5c842', category: 'intel',     requiresKey: false, tags: ['url'],                                 desc: 'Community phishing URL database. No API key needed' },
            { id: 'gemini',       name: 'Google Gemini',   icon: '♊', color: '#4a9eff', category: 'ai',        requiresKey: true,  tags: ['llm','agentic'],                       desc: 'Google Next-gen AI. Critical for Threat Copilot reasoning.' },
            { id: 'openai',       name: 'OpenAI',          icon: '🤖', color: '#3ecf8e', category: 'ai',        requiresKey: true,  tags: ['llm','gpt4'],                          desc: 'GPT-4o/o1 models for advanced cybersecurity analysis.' },
            { id: 'anthropic',    name: 'Anthropic Claude',icon: '👤', color: '#f5c842', category: 'ai',        requiresKey: true,  tags: ['llm','claude'],                        desc: 'Claude 3.5 Sonnet — high-precision forensic reasoning.' },
            { id: 'moonshot',     name: 'Moonshot AI',    icon: '🌙', color: '#4a9eff', category: 'ai',        requiresKey: true,  tags: ['llm','kimi'],                          desc: 'Moonshot (Kimi) model for high-speed reasoning.' },
            { id: 'deepseek',     name: 'DeepSeek',       icon: '🧠', color: '#9b77e8', category: 'ai',        requiresKey: true,  tags: ['llm','r1'],                            desc: 'DeepSeek-V3/R1 models. Cutting-edge open-weights reasoning.' },
            { id: 'ollama',       name: 'Ollama (Local)',  icon: '🦙', color: '#9b77e8', category: 'ai',        requiresKey: false, tags: ['llm','local'],                         desc: 'Run Llama3 or Mistral locally. No API key required.' }
        ];

        vm.categories = [
            { id: 'all',      label: 'All' },
            { id: 'analyzer', label: 'Analyzers' },
            { id: 'intel',    label: 'Threat Intel' },
            { id: 'recon',    label: 'Recon' },
            { id: 'ai',       label: 'AI Analyst' }
        ];

        // ================================================================
        //  STATE
        // ================================================================
        vm.nodes           = [];
        vm.loading         = false;
        vm.saving          = false;
        vm.showModal       = false;
        vm.showCatalog     = false;
        vm.editingNode     = null;
        vm.form            = _emptyForm();
        vm.toasts          = [];
        vm.testResult      = null;
        vm.testInProgress  = false;
        vm.filterCategory  = 'all';
        vm.searchQuery     = '';
        vm.backendStatus   = 'checking';

        // ================================================================
        //  PERSISTENCE HELPERS (localStorage <-> backend)
        // ================================================================
        function _readLocal() {
            try { return JSON.parse(localStorage.getItem(STORAGE_KEY)) || []; }
            catch (e) { return []; }
        }
        function _writeLocal(nodes) {
            localStorage.setItem(STORAGE_KEY, JSON.stringify(nodes));
        }
        function _genId() {
            return 'node_' + Date.now() + '_' + Math.random().toString(36).substr(2, 6);
        }

        // ================================================================
        //  BACKEND PROBE — determine if nv-node-service is reachable
        // ================================================================
        function _probeBackend() {
            $http.get('/node-service/healthz', { timeout: 3000 })
                .then(function () { vm.backendStatus = 'online'; })
                .catch(function () { vm.backendStatus = 'offline'; });
        }
        _probeBackend();

        // ================================================================
        //  LOAD NODES — try backend first, fall back to localStorage
        // ================================================================
        vm.loadNodes = function () {
            vm.loading = true;

            $http.get('/node-service/nodes', { timeout: 5000 }).then(function (res) {
                var backendNodes = (res.data && res.data.nodes) ? res.data.nodes : (angular.isArray(res.data) ? res.data : []);
                backendNodes.forEach(function (n) { n._source = 'backend'; });
                vm.backendStatus = 'online';

                // Merge with localStorage nodes that aren't on the backend
                var localNodes = _readLocal().filter(function (ln) {
                    return !backendNodes.some(function (bn) { return bn.id === ln.id; });
                });
                localNodes.forEach(function (n) { n._source = 'local'; });

                vm.nodes = backendNodes.concat(localNodes);
                _writeLocal(vm.nodes);
            }).catch(function () {
                vm.backendStatus = 'offline';
                vm.nodes = _readLocal();
                vm.nodes.forEach(function (n) { n._source = 'local'; });
            }).finally(function () {
                vm.loading = false;
            });
        };

        // ================================================================
        //  CATALOG HELPERS
        // ================================================================
        function _catalogFor(typeId) {
            for (var i = 0; i < vm.catalog.length; i++) {
                if (vm.catalog[i].id === typeId) return vm.catalog[i];
            }
            return null;
        }

        vm.toggleCatalog = function () { vm.showCatalog = !vm.showCatalog; };

        vm.getFilteredCatalog = function () {
            return vm.catalog.filter(function (c) {
                if (vm.filterCategory !== 'all' && c.category !== vm.filterCategory) return false;
                if (vm.searchQuery) {
                    var q = vm.searchQuery.toLowerCase();
                    return c.name.toLowerCase().indexOf(q) !== -1 || c.desc.toLowerCase().indexOf(q) !== -1;
                }
                return true;
            });
        };

        vm.getRegisteredCount = function (typeId) {
            return vm.nodes.filter(function (n) { return n.node_type === typeId; }).length;
        };

        // ================================================================
        //  NODE DISPLAY HELPERS
        // ================================================================
        vm.getNodeIcon = function (node) {
            var cat = node ? _catalogFor(node.node_type) : null;
            return cat ? cat.icon : '⬡';
        };
        vm.getNodeColor = function (node) {
            var cat = node ? _catalogFor(node.node_type) : null;
            return cat ? cat.color : '#4a9eff';
        };
        vm.getNodeTypeName = function (node) {
            var cat = node ? _catalogFor(node.node_type) : null;
            return cat ? cat.name : (node ? node.node_type : 'Unknown');
        };

        vm.getActiveTypes = function () {
            var seen = {};
            var types = [];
            var filtered = vm.getFilteredNodes();
            for (var i = 0; i < filtered.length; i++) {
                var t = filtered[i].node_type;
                if (!seen[t]) { seen[t] = true; types.push(t); }
            }
            return types;
        };

        vm.getNodesByType = function (type) {
            return vm.getFilteredNodes().filter(function (n) { return n.node_type === type; });
        };

        vm.getFilteredNodes = function () {
            return vm.nodes.filter(function (n) {
                if (vm.filterCategory !== 'all') {
                    var cat = _catalogFor(n.node_type);
                    if (!cat || cat.category !== vm.filterCategory) return false;
                }
                if (vm.searchQuery) {
                    var q = vm.searchQuery.toLowerCase();
                    return (n.name || '').toLowerCase().indexOf(q) !== -1 ||
                           (n.url || '').toLowerCase().indexOf(q) !== -1 ||
                           (n.node_type || '').toLowerCase().indexOf(q) !== -1;
                }
                return true;
            });
        };

        // ================================================================
        //  STATS
        // ================================================================
        vm.getUpCount = function () {
            return vm.nodes.filter(function (n) { return n.status === 'UP'; }).length;
        };
        vm.getDownCount = function () {
            return vm.nodes.filter(function (n) { return n.status === 'DOWN'; }).length;
        };
        vm.getDegradedCount = function () {
            return vm.nodes.filter(function (n) { return n.status === 'DEGRADED'; }).length;
        };

        // ================================================================
        //  MODAL — Add / Edit
        // ================================================================
        vm.openAddModal = function (catalogItem) {
            vm.editingNode = null;
            vm.form = _emptyForm();
            if (catalogItem) {
                vm.form.node_type = catalogItem.id;
            }
            vm.testResult = null;
            vm.showModal = true;
        };

        vm.openEditModal = function (node) {
            vm.editingNode = node;
            vm.form = {
                node_type:   node.node_type,
                name:        node.name,
                url:         node.url,
                api_key:     '',
                description: node.description || '',
                tls_verify:  node.tls_verify !== false
            };
            vm.testResult = null;
            vm.showModal = true;
        };

        vm.closeModal = function () {
            vm.showModal = false;
            vm.editingNode = null;
            vm.testResult = null;
            vm.saving = false;
        };

        // ================================================================
        //  SAVE NODE — backend-first, localStorage fallback
        // ================================================================
        vm.saveNode = function () {
            if (!vm.form.node_type || !vm.form.name || !vm.form.url) return;
            vm.saving = true;

            if (vm.editingNode) {
                // ── UPDATE ──
                var patch = {
                    name: vm.form.name,
                    url: vm.form.url,
                    description: vm.form.description,
                    tls_verify: vm.form.tls_verify
                };
                if (vm.form.api_key && vm.form.api_key.trim()) {
                    patch.api_key = vm.form.api_key.trim();
                }

                if (vm.backendStatus === 'online') {
                    $http.patch('/node-service/nodes/' + vm.editingNode.id, patch, { timeout: 5000 })
                        .then(function () { _afterSave('Node updated successfully'); })
                        .catch(function () { _updateLocal(vm.editingNode.id, patch); _afterSave('Updated locally (backend offline)'); });
                } else {
                    _updateLocal(vm.editingNode.id, patch);
                    _afterSave('Updated in local storage');
                }
            } else {
                // ── CREATE ──
                var newNode = {
                    id:          _genId(),
                    node_type:   vm.form.node_type,
                    name:        vm.form.name,
                    url:         vm.form.url,
                    api_key:     vm.form.api_key || '',
                    api_key_masked: vm.form.api_key ? ('••••' + vm.form.api_key.slice(-4)) : '',
                    description: vm.form.description || '',
                    tls_verify:  vm.form.tls_verify,
                    status:      'UNKNOWN',
                    latency_ms:  null,
                    probe_count: 0,
                    last_error:  null,
                    last_checked: null,
                    created_at:  new Date().toISOString(),
                    _source:     'local'
                };

                if (vm.backendStatus === 'online') {
                    $http.post('/node-service/nodes', {
                        node_type: newNode.node_type, name: newNode.name,
                        url: newNode.url, api_key: newNode.api_key, tls_verify: newNode.tls_verify
                    }, { timeout: 5000 }).then(function (res) {
                        // Use server-generated ID if available
                        if (res.data && res.data.id) newNode.id = res.data.id;
                        newNode._source = 'backend';
                        vm.nodes.push(newNode);
                        _writeLocal(vm.nodes);
                        _afterSave('Node registered — first probe within 60s');
                    }).catch(function () {
                        vm.nodes.push(newNode);
                        _writeLocal(vm.nodes);
                        _afterSave('Saved locally (backend offline)');
                    });
                } else {
                    vm.nodes.push(newNode);
                    _writeLocal(vm.nodes);
                    _afterSave('Saved to local storage');
                }
            }
        };

        function _updateLocal(nodeId, patch) {
            for (var i = 0; i < vm.nodes.length; i++) {
                if (vm.nodes[i].id === nodeId) {
                    angular.extend(vm.nodes[i], patch);
                    if (patch.api_key) {
                        vm.nodes[i].api_key_masked = '••••' + patch.api_key.slice(-4);
                    }
                    break;
                }
            }
            _writeLocal(vm.nodes);
        }

        function _afterSave(msg) {
            _toast(msg, 'success');
            vm.closeModal();
        }

        // ================================================================
        //  DELETE NODE
        // ================================================================
        vm.deleteNode = function (node) {
            if (!window.confirm('Remove "' + node.name + '"? This cannot be undone.')) return;

            if (vm.backendStatus === 'online' && node._source === 'backend') {
                $http.delete('/node-service/nodes/' + node.id, { timeout: 5000 })
                    .then(function () { _removeLocal(node.id); _toast('"' + node.name + '" removed', 'success'); })
                    .catch(function () { _removeLocal(node.id); _toast('Removed locally', 'info'); });
            } else {
                _removeLocal(node.id);
                _toast('"' + node.name + '" removed', 'success');
            }
        };

        function _removeLocal(nodeId) {
            vm.nodes = vm.nodes.filter(function (n) { return n.id !== nodeId; });
            _writeLocal(vm.nodes);
        }

        // ================================================================
        //  TEST CONNECTION — Real Backend API Verification
        // ================================================================
        vm.testConnection = function () {
            if (!vm.form.url) return;
            vm.testResult = null;
            vm.testInProgress = true;

            var payload = {
                node_type: vm.form.node_type || 'cortex',
                name: vm.form.name || 'Test Node',
                url: vm.form.url,
                api_key: vm.form.api_key || 'na',
                tls_verify: vm.form.tls_verify !== false
            };

            $http.post('/node-service/nodes/test-connection', payload, { timeout: 15000 })
                .then(function (res) {
                    vm.testResult = res.data;
                })
                .catch(function (err) {
                    if (err.status === -1) {
                         vm.testResult = { ok: false, error: 'Backend service unreachable. Cannot verify API.' };
                    } else {
                         vm.testResult = err.data || { ok: false, error: 'Test failed (' + err.status + ')' };
                    }
                })
                .finally(function () {
                    vm.testInProgress = false;
                });
        };

        vm.quickTest = function (node) {
            node._testing = true;

            var isAiNode = ['gemini', 'openai', 'anthropic', 'ollama'].indexOf(node.node_type) !== -1;

            // Try backend test first
            if (vm.backendStatus === 'online' && node._source === 'backend') {
                $http.post('/node-service/nodes/' + node.id + '/test', {}, { timeout: 20000 })
                    .then(function (res) {
                        var r = res.data;
                        node.status = r.status || (r.ok ? 'UP' : 'DOWN');
                        node.latency_ms = r.latency_ms;
                        node.last_error = r.error || null;
                        node.last_checked = new Date();
                        node.probe_count = (node.probe_count || 0) + 1;
                        _writeLocal(vm.nodes);
                        _toast((r.ok ? '✓ ' : '✗ ') + node.name + ' — ' + (r.latency_ms || 0) + 'ms', r.ok ? 'success' : 'error');
                    })
                    .catch(function () { 
                        if (!isAiNode) _fallbackBrowserTest(node); 
                    })
                    .finally(function () { node._testing = false; });
            } else if (!isAiNode) {
                _fallbackBrowserTest(node);
            } else {
                node._testing = false;
                _toast('AI Nodes must be registered to test connection', 'warning');
            }
        };

        function _fallbackBrowserTest(node) {
            _browserPing(node.url).then(function (result) {
                node.status = result.ok ? 'UP' : 'DOWN';
                node.latency_ms = result.latency_ms || null;
                node.last_error = result.error || null;
                node.last_checked = new Date();
                node.probe_count = (node.probe_count || 0) + 1;
                _writeLocal(vm.nodes);
                _toast((result.ok ? '✓ ' : '✗ ') + node.name + (result.latency_ms ? (' — ' + result.latency_ms + 'ms') : ''), result.ok ? 'success' : 'error');
            }).finally(function () { node._testing = false; });
        }

        function _browserPing(url) {
            var start = Date.now();
            return $http({ method: 'GET', url: url, timeout: 8000 }).then(function (res) {
                return { ok: true, latency_ms: Date.now() - start, http_status: res.status, note: 'Direct probe' };
            }).catch(function (err) {
                var elapsed = Date.now() - start;
                // If the domain doesn't exist or connection is refused, err.status is -1
                if (err.status === 0 || err.status === -1) {
                    return { ok: false, error: 'Connection refused or timed out (' + elapsed + 'ms)' };
                }
                // Any HTTP status means the server is reachable, even if forbidden
                return { ok: true, latency_ms: elapsed, http_status: err.status, note: 'HTTP ' + err.status };
            });
        }

        // ================================================================
        //  TEST ALL NODES
        // ================================================================
        vm.testAllNodes = function () {
            if (!vm.nodes.length) return;
            _toast('Testing all ' + vm.nodes.length + ' nodes…', 'info');
            vm.nodes.forEach(function (node) {
                vm.quickTest(node);
            });
        };

        // ================================================================
        //  EXPORT / IMPORT CONFIG
        // ================================================================
        vm.exportConfig = function () {
            if (!vm.nodes.length) return;
            var exportData = {
                version: 1,
                exported_at: new Date().toISOString(),
                nodes: vm.nodes.map(function (n) {
                    return {
                        node_type:   n.node_type,
                        name:        n.name,
                        url:         n.url,
                        description: n.description || '',
                        tls_verify:  n.tls_verify
                        // api_key intentionally excluded for security
                    };
                })
            };
            var blob = new Blob([JSON.stringify(exportData, null, 2)], { type: 'application/json' });
            var a = document.createElement('a');
            a.href = URL.createObjectURL(blob);
            a.download = 'nv-integrations-' + new Date().toISOString().slice(0, 10) + '.json';
            a.click();
            URL.revokeObjectURL(a.href);
            _toast('Exported ' + vm.nodes.length + ' nodes', 'success');
        };

        vm.importConfig = function () {
            var input = document.createElement('input');
            input.type = 'file';
            input.accept = '.json';
            input.onchange = function (evt) {
                var file = evt.target.files[0];
                if (!file) return;
                var reader = new FileReader();
                reader.onload = function (e) {
                    $scope.$apply(function () {
                        try {
                            var data = JSON.parse(e.target.result);
                            var importedNodes = data.nodes || data;
                            if (!angular.isArray(importedNodes)) {
                                _toast('Invalid format — expected a nodes array', 'error');
                                return;
                            }
                            var count = 0;
                            importedNodes.forEach(function (n) {
                                if (!n.node_type || !n.name || !n.url) return;
                                // Skip duplicates by URL
                                var exists = vm.nodes.some(function (existing) {
                                    return existing.url === n.url && existing.node_type === n.node_type;
                                });
                                if (exists) return;

                                vm.nodes.push({
                                    id:           _genId(),
                                    node_type:    n.node_type,
                                    name:         n.name,
                                    url:          n.url,
                                    api_key:      n.api_key || '',
                                    api_key_masked: n.api_key ? ('••••' + n.api_key.slice(-4)) : '',
                                    description:  n.description || '',
                                    tls_verify:   n.tls_verify !== false,
                                    status:       'UNKNOWN',
                                    latency_ms:   null,
                                    probe_count:  0,
                                    last_error:   null,
                                    last_checked: null,
                                    created_at:   new Date().toISOString(),
                                    _source:      'local'
                                });
                                count++;
                            });
                            _writeLocal(vm.nodes);
                            _toast('Imported ' + count + ' new nodes (skipped duplicates)', 'success');
                        } catch (parseErr) {
                            _toast('Import failed — invalid JSON', 'error');
                        }
                    });
                };
                reader.readAsText(file);
            };
            input.click();
        };

        // ================================================================
        //  TOAST SYSTEM
        // ================================================================
        function _toast(message, type) {
            var t = { message: message, type: type || 'info' };
            vm.toasts.push(t);
            $timeout(function () {
                var i = vm.toasts.indexOf(t);
                if (i !== -1) vm.toasts.splice(i, 1);
            }, 4000);
        }

        // ================================================================
        //  FORM FACTORY
        // ================================================================
        function _emptyForm() {
            return { node_type: 'cortex', name: '', url: '', api_key: '', description: '', tls_verify: true };
        }

        // ================================================================
        //  LIFECYCLE — load on entry, poll every 30s
        // ================================================================
        vm.loadNodes();

        _pollHandle = $interval(function () {
            if (!vm.showModal) { vm.loadNodes(); }
        }, 30000);

        $scope.$on('$destroy', function () {
            if (_pollHandle) $interval.cancel(_pollHandle);
        });

        // WebSocket events
        $scope.$on('nv:node:statusChanged', function (evt, payload) {
            $scope.$apply(function () {
                for (var i = 0; i < vm.nodes.length; i++) {
                    if (vm.nodes[i].id === payload.node_id) {
                        vm.nodes[i].status = payload.status;
                        vm.nodes[i].latency_ms = payload.latency_ms;
                        vm.nodes[i].last_checked = new Date();
                        break;
                    }
                }
            });
        });
    });
})();
