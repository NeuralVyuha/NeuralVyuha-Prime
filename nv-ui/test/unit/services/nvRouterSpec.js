'use strict';

describe('Service: NvRouter', function () {

  // load the service's module
  beforeEach(module('neuralvyuha'));

  var NvRouter, NvConfig, $httpBackend, $q, NotificationSrv;

  beforeEach(inject(function (_NvRouter_, _NvConfig_, _$httpBackend_, _$q_, _NotificationSrv_) {
    NvRouter = _NvRouter_;
    NvConfig = _NvConfig_;
    $httpBackend = _$httpBackend_;
    $q = _$q_;
    NotificationSrv = _NotificationSrv_;
  }));

  afterEach(function () {
    $httpBackend.verifyNoOutstandingExpectation();
    $httpBackend.verifyNoOutstandingRequest();
  });

  it('should use v4 endpoint when flag is disabled', function () {
    NvConfig.useNvQueryReads = false;
    $httpBackend.expectGET('/api/alert?limit=10').respond(200, []);

    NvRouter.getAlerts({limit: 10});
    $httpBackend.flush();
  });

  it('should use v5 endpoint when flag is enabled', function () {
    NvConfig.useNvQueryReads = true;
    $httpBackend.expectGET('/api/v5/alerts?limit=10').respond(200, []);

    NvRouter.getAlerts({limit: 10});
    $httpBackend.flush();
  });

  it('should fallback to v4 on v5 timeout', function () {
    NvConfig.useNvQueryReads = true;
    NvConfig.fallbackToV4 = true;

    // v5 fails
    $httpBackend.expectGET('/api/v5/alerts?limit=10').respond(504, 'Gateway Timeout');
    // fallback v4
    $httpBackend.expectGET('/api/alert?limit=10').respond(200, []);

    NvRouter.getAlerts({limit: 10});
    $httpBackend.flush();
  });

  it('should NOT fallback on 403 Forbidden', function () {
    NvConfig.useNvQueryReads = true;
    NvConfig.fallbackToV4 = true;
    spyOn(NotificationSrv, 'error');

    $httpBackend.expectGET('/api/v5/alerts?limit=10').respond(403, 'Forbidden');

    NvRouter.getAlerts({limit: 10}).catch(function(err) {
      expect(err.status).toBe(403);
    });

    $httpBackend.flush();
    expect(NotificationSrv.error).toHaveBeenCalled();
  });

});
