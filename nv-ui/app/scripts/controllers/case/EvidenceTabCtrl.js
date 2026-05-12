(function () {
    'use strict';

    /**
     * EvidenceTabCtrl — manages the Evidence tab on a Case Details view.
     *
     * Features:
     *  - Lists existing artifacts for the case (with SIGHTING badge if is_sighting=true)
     *  - Drag-and-drop / click-to-select file upload with progress simulation
     *  - Download via presigned URL (60-second TTL, MinIO credentials never touch client)
     */
    angular.module('nvControllers').controller('EvidenceTabCtrl',
        function ($scope, $q, NvApiSrv, nvArtifactSrv, NotificationSrv) {

            var vm = $scope;
            vm.artifacts = [];
            vm.loading = false;
            vm.uploading = false;
            vm.uploadProgress = 0;
            vm.dragOver = false;

            // ── Load artifacts ────────────────────────────────────────────
            vm.loadArtifacts = function () {
                vm.loading = true;
                NvApiSrv.getArtifacts(vm.caseId)
                    .then(function (data) {
                        vm.artifacts = data.artifacts || [];
                    })
                    .catch(function () {
                        NotificationSrv.error('Evidence', 'Failed to load artifacts');
                    })
                    .finally(function () {
                        vm.loading = false;
                    });
            };

            // ── Drag-and-drop handlers ────────────────────────────────────
            vm.onDragOver = function ($event) {
                $event.preventDefault();
                vm.dragOver = true;
            };

            vm.onDragLeave = function () {
                vm.dragOver = false;
            };

            vm.onDrop = function ($event) {
                $event.preventDefault();
                vm.dragOver = false;
                var files = $event.dataTransfer.files;
                if (files && files.length > 0) {
                    vm.uploadFile(files[0]);
                }
            };

            vm.onFileSelect = function ($event) {
                var files = $event.target.files;
                if (files && files.length > 0) {
                    vm.uploadFile(files[0]);
                }
            };

            // ── Upload ────────────────────────────────────────────────────
            vm.uploadFile = function (file) {
                vm.uploading = true;
                vm.uploadProgress = 0;

                // Simulate progress ticks while upload is in-flight
                var progressTick = setInterval(function () {
                    if (vm.uploadProgress < 85) {
                        vm.uploadProgress += 5;
                        $scope.$apply();
                    }
                }, 150);

                NvApiSrv.uploadArtifact(vm.caseId, file)
                    .then(function (data) {
                        clearInterval(progressTick);
                        vm.uploadProgress = 100;

                        if (data.is_sighting) {
                            NotificationSrv.warning(
                                '⚠️ GLOBAL SIGHTING DETECTED',
                                'This file (SHA-256: ' + data.sha256.substring(0, 12) + '…) has been seen in ' +
                                data.sightings_count + ' case(s). Flag for investigation.'
                            );
                        } else {
                            NotificationSrv.success('Evidence uploaded', file.name + ' stored securely.');
                        }

                        // Reload list to show new artifact + badge
                        vm.loadArtifacts();
                    })
                    .catch(function (err) {
                        clearInterval(progressTick);
                        NotificationSrv.error('Upload failed', err.data ? err.data.detail : 'Unknown error');
                    })
                    .finally(function () {
                        vm.uploading = false;
                        vm.uploadProgress = 0;
                    });
            };

            // ── Download via presigned URL ────────────────────────────────
            vm.downloadArtifact = function (artifact) {
                nvArtifactSrv.getPresignedUrl(artifact.artifact_id || artifact.id)
                    .then(function (url) {
                        // Open the 60-second presigned URL in a new tab — no credentials involved
                        window.open(url, '_blank');
                    })
                    .catch(function () {
                        NotificationSrv.error('Download', 'Could not generate download link');
                    });
            };

            // ── Init ──────────────────────────────────────────────────────
            vm.$watch('caseId', function (newVal) {
                if (newVal) {
                    vm.loadArtifacts();
                }
            });
        }
    );
})();
