'use strict';

angular.module('neuralvyuha').factory('NvConfig', function () {
  // Configuration for Phase E6.1 UI Strangler
  return {
    MASTER_WRITE_TARGET: window.UI_MASTER_WRITE_TARGET || 'NV',
    useNvQueryReads: window.UI_USE_V5_QUERY_READS !== 'false', // Default true
    nvBaseUrl: window.UI_V5_QUERY_BASE_URL || '/api',
    fallbackToV4: window.UI_V5_FALLBACK_TO_V4 === 'true', // Default false for full v5 cutover
    timeoutMs: parseInt(window.UI_V5_QUERY_TIMEOUT_MS) || 3000
  };
});
