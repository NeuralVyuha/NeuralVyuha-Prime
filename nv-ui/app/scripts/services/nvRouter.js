'use strict';

angular.module('nvServices').factory('NvRouter', function ($http, $q, NvConfig, NotificationSrv) {

  var service = {};

  // Helper to enforce NV endpoint responses
  function tryNvOrElse(nvPromise, v4FallbackFn) {
    // Phase 7: Full v5 Cutover
    // Legacy v4 fallbacks are officially deprecated and removed.
    // The UI must strictly rely on the nv-query microservice.
    return nvPromise.catch(function (err) {
      // 403 Forbidden -> Show Error (Do not fallback)
      if (err.status === 403) {
        NotificationSrv.error('Permission Denied', 'You do not have permission to view this resource.');
        return $q.reject(err);
      }

      // 401 Unauthorized -> Let interceptor handle login
      if (err.status === 401) {
        return $q.reject(err);
      }

      console.error('v5 Query API failed/timed out. Legacy fallback is disabled.', err);
      return $q.reject(err);
    });
  }

  // Alerts List
  service.getAlerts = function (params) {
    var nvRequest = $http.get(NvConfig.nvBaseUrl + '/alerts', {
      params: params,
      timeout: NvConfig.timeoutMs
    }).then(function (res) {
      return res.data; // Adapter expects data
    });

    // Fallback: Legacy v4 endpoint
    // Note: Legacy list() returns PaginatedQuerySrv object, but here we just return the data promise
    // The V5ListAdapter handles the structure.
    // Ideally, fallback should return promise resolving to v4 data.
    var v4Request = function () {
      // Direct call to v4 API mimicking PaginatedQuerySrv internal call
      return $http.post('/api/v1/query', { query: { _name: 'listAlert' }, filter: params.filter, sort: params.sort }).then(function (res) { return res.data; });
    };

    return tryNvOrElse(nvRequest, v4Request);
  };

  // Alert Details
  service.getAlert = function (id) {
    var nvRequest = $http.get(NvConfig.nvBaseUrl + '/alerts/' + id, {
      timeout: NvConfig.timeoutMs
    }).then(function (res) {
      return res.data;
    });

    var v4Request = function () {
      return $http.get('./api/v1/alert/' + id).then(function (res) { return res.data; });
    };

    return tryNvOrElse(nvRequest, v4Request);
  };

  // Graph (Phase E8.1 — Visual Vyuha)
  service.getGraphRoute = function (caseId) {
    return '#!/nv/graph/' + encodeURIComponent(caseId);
  };

  return service;
});
