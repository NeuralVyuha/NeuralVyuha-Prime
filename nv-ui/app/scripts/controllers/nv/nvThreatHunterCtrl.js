'use strict';

/**
 * NeuralVyuha Threat Hunter Controller
 * Tier 5: The Hunter — OpenSearch log search & analysis
 */
angular.module('nvControllers')
    .controller('NvThreatHunterCtrl', function ($scope, NvApiSrv, NotificationSrv) {
        var vm = this;

        // ── State ──────────────────────────────────────────────────────────
        vm.query = '*';
        vm.results = null;
        vm.histogram = { buckets: [] };
        vm.loading = false;
        vm.selectedEvent = null;
        vm.source = null;

        // Pagination
        vm.pageSize = 25;
        vm.currentPage = 1;
        vm.totalPages = 1;

        // Sorting
        vm.sortField = 'timestamp';
        vm.sortOrder = 'desc';

        // Time
        vm.timePresets = [
            { label: '15m', value: 15 * 60 * 1000 },
            { label: '1h', value: 60 * 60 * 1000 },
            { label: '24h', value: 24 * 60 * 60 * 1000 },
            { label: '7d', value: 7 * 24 * 60 * 60 * 1000 },
            { label: '30d', value: 30 * 24 * 60 * 60 * 1000 }
        ];
        vm.selectedTime = 24 * 60 * 60 * 1000; // default 24h

        // Field sidebar
        vm.fieldGroups = [
            { label: 'Source', field: 'payload.source', open: true, values: [] },
            { label: 'Status', field: 'status', open: true, values: [] },
            { label: 'Severity', field: 'payload.severity', open: false, values: [] },
            { label: 'Rule ID', field: 'payload.original_payload.rule.id', open: false, values: [] },
            { label: 'Source IP', field: 'payload.original_payload.data.srcip', open: false, values: [] }
        ];

        // ── Time helpers ───────────────────────────────────────────────────
        vm.getTimeRange = function () {
            var now = Date.now();
            return { from: now - vm.selectedTime, to: now };
        };

        vm.setTimeRange = function (ms) {
            vm.selectedTime = ms;
            vm.currentPage = 1;
            vm.executeSearch();
        };

        vm.formatTime = function (epochMs) {
            if (!epochMs) return '—';
            var d = new Date(epochMs);
            var pad = function (n) { return n < 10 ? '0' + n : n; };
            return d.getFullYear() + '-' + pad(d.getMonth() + 1) + '-' + pad(d.getDate()) +
                ' ' + pad(d.getHours()) + ':' + pad(d.getMinutes()) + ':' + pad(d.getSeconds());
        };

        // ── Severity helpers ───────────────────────────────────────────────
        vm.severityLabel = function (sev) {
            var labels = { 1: 'LOW', 2: 'MED', 3: 'HIGH', 4: 'CRIT' };
            return labels[sev] || 'INFO';
        };

        // ── Sorting ────────────────────────────────────────────────────────
        vm.toggleSort = function (field) {
            if (vm.sortField === field) {
                vm.sortOrder = vm.sortOrder === 'desc' ? 'asc' : 'desc';
            } else {
                vm.sortField = field;
                vm.sortOrder = 'desc';
            }
            vm.currentPage = 1;
            vm.executeSearch();
        };

        vm.sortIcon = function (field) {
            if (vm.sortField !== field) return 'fa-sort';
            return vm.sortOrder === 'desc' ? 'fa-sort-desc' : 'fa-sort-asc';
        };

        // ── Histogram bar height ───────────────────────────────────────────
        vm.barHeight = function (count) {
            if (!vm.histogram || !vm.histogram.buckets.length) return 0;
            var max = Math.max.apply(null, vm.histogram.buckets.map(function (b) { return b.count; }));
            if (max === 0) return 0;
            return Math.max(2, (count / max) * 100);
        };

        // ── Core Search ────────────────────────────────────────────────────
        vm.executeSearch = function () {
            vm.loading = true;
            vm.selectedEvent = null;
            var tr = vm.getTimeRange();

            // Parallel: search + histogram + fields
            var searchParams = {
                q: vm.query || '*',
                size: vm.pageSize,
                from: (vm.currentPage - 1) * vm.pageSize,
                sort_field: vm.sortField,
                sort_order: vm.sortOrder,
                time_from: tr.from,
                time_to: tr.to,
                source: vm.source || undefined
            };

            NvApiSrv.huntSearch(searchParams).then(function (data) {
                vm.results = data;
                vm.totalPages = Math.max(1, Math.ceil(data.total / vm.pageSize));
                vm.loading = false;
            }).catch(function () {
                NotificationSrv.error('ThreatHunter', 'Hunt search failed', 'danger');
                vm.loading = false;
            });

            NvApiSrv.huntHistogram({
                q: vm.query || '*',
                time_from: tr.from,
                time_to: tr.to,
                interval: 'auto'
            }).then(function (data) {
                vm.histogram = data;
            });

            // Load field breakdowns
            vm.fieldGroups.forEach(function (fg) {
                NvApiSrv.huntFields({
                    field: fg.field,
                    q: vm.query || '*',
                    time_from: tr.from,
                    time_to: tr.to,
                    top_n: 10
                }).then(function (data) {
                    fg.values = data.values || [];
                });
            });
        };

        // ── Pagination ─────────────────────────────────────────────────────
        vm.changePage = function (n) {
            if (n < 1 || n > vm.totalPages) return;
            vm.currentPage = n;
            vm.executeSearch();
        };

        // ── Event detail ───────────────────────────────────────────────────
        vm.selectEvent = function (hit) {
            vm.selectedEvent = vm.selectedEvent === hit ? null : hit;
        };

        // ── Sidebar filter ─────────────────────────────────────────────────
        vm.addFilter = function (field, value) {
            var token = field + ':"' + value + '"';
            if (vm.query === '*' || !vm.query) {
                vm.query = token;
            } else {
                vm.query += ' AND ' + token;
            }
            vm.currentPage = 1;
            vm.executeSearch();
        };

        // ── Source filter ──────────────────────────────────────────────────
        vm.clearSourceFilter = function () {
            vm.source = null;
            vm.currentPage = 1;
            vm.executeSearch();
        };

        // ── Time drill-in from histogram bar click ─────────────────────────
        vm.drillIntoTime = function (epochMs) {
            // Zoom into ±30 min around the clicked bar
            vm.selectedTime = 60 * 60 * 1000; // 1h
            vm.currentPage = 1;
            vm.executeSearch();
        };

        // ── CSV Export ─────────────────────────────────────────────────────
        vm.exportCSV = function () {
            if (!vm.results || !vm.results.hits.length) return;

            var rows = [['Timestamp', 'Source', 'Severity', 'Event ID', 'Summary']];
            vm.results.hits.forEach(function (h) {
                rows.push([
                    vm.formatTime(h.timestamp),
                    h.payload && h.payload.source || '—',
                    vm.severityLabel(h.payload && h.payload.severity),
                    h.event_id || h._id || '—',
                    (h.payload && h.payload.rule_description) || (h.fingerprint) || '—'
                ]);
            });

            var csv = rows.map(function (r) { return r.join(','); }).join('\n');
            var blob = new Blob([csv], { type: 'text/csv' });
            var url = URL.createObjectURL(blob);
            var a = document.createElement('a');
            a.href = url;
            a.download = 'threat-hunt-' + new Date().toISOString().slice(0, 10) + '.csv';
            a.click();
            URL.revokeObjectURL(url);
        };

        // ── Init: auto-search on page load ─────────────────────────────────
        vm.executeSearch();
    });
