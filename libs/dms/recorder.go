// Package dms records bundle deployments as versions with the Deployment
// Metadata Service (DMS).
//
// It is intentionally independent of the deployment lock: a Recorder does not
// acquire or hold any lock. Callers are responsible for serializing concurrent
// deployments (today via the workspace-filesystem lock). The server-side
// version counter — CreateVersion only succeeds when the requested version is
// last_version_id + 1 — provides the concurrency control for the records
// themselves.
package dms

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/databricks/cli/internal/build"
	"github.com/databricks/cli/libs/log"
	"github.com/databricks/databricks-sdk-go/apierr"
	"github.com/databricks/databricks-sdk-go/service/bundledeployments"
)

// The server expires a version's lease if it does not receive a heartbeat
// within a 2-minute TTL; we heartbeat well inside that window.
const defaultHeartbeatInterval = 30 * time.Second

// VersionType identifies the kind of deployment a version records.
type VersionType = bundledeployments.VersionType

const (
	VersionTypeDeploy  VersionType = bundledeployments.VersionTypeVersionTypeDeploy
	VersionTypeDestroy VersionType = bundledeployments.VersionTypeVersionTypeDestroy
)

// Recorder records a single deploy/destroy as a version with DMS. The DMS
// deployment is identified by the bundle's state lineage and each version by
// the state serial, so a bundle deployment maps one-to-one to a DMS deployment
// record and each deploy to a version.
type Recorder struct {
	svc         bundledeployments.BundleDeploymentsInterface
	lineage     string
	targetName  string
	versionType VersionType

	// populated by CreateVersion
	serial        int64
	stopHeartbeat context.CancelFunc
}

// NewRecorder returns a Recorder for the given deployment.
func NewRecorder(svc bundledeployments.BundleDeploymentsInterface, lineage, targetName string, versionType VersionType) *Recorder {
	return &Recorder{
		svc:         svc,
		lineage:     lineage,
		targetName:  targetName,
		versionType: versionType,
	}
}

// CreateVersion registers a new version with DMS, claiming it for the duration
// of the deployment. A nil Recorder is a no-op, so callers can leave it nil
// when recording is disabled.
func (r *Recorder) CreateVersion(ctx context.Context) error {
	if r == nil {
		return nil
	}

	versionID, err := r.createDeploymentVersion(ctx)
	if err != nil {
		return err
	}

	serial, err := strconv.ParseInt(versionID, 10, 64)
	if err != nil {
		return fmt.Errorf("failed to parse version ID %q: %w", versionID, err)
	}
	r.serial = serial
	r.stopHeartbeat = startHeartbeat(ctx, r.svc, r.lineage, versionID)
	return nil
}

// CompleteVersion finalizes the version created by CreateVersion. A nil
// Recorder, or one whose CreateVersion never ran, is a no-op.
func (r *Recorder) CompleteVersion(ctx context.Context, success bool) error {
	if r == nil || r.stopHeartbeat == nil {
		return nil
	}

	r.stopHeartbeat()

	versionIDStr := strconv.FormatInt(r.serial, 10)
	versionName := fmt.Sprintf("deployments/%s/versions/%s", r.lineage, versionIDStr)

	reason := bundledeployments.VersionCompleteVersionCompleteSuccess
	if !success {
		reason = bundledeployments.VersionCompleteVersionCompleteFailure
	}

	_, err := r.svc.CompleteVersion(ctx, bundledeployments.CompleteVersionRequest{
		Name:             versionName,
		CompletionReason: reason,
	})
	if err != nil {
		return err
	}
	log.Infof(ctx, "Completed deployment version: deployment=%s version=%s reason=%s", r.lineage, versionIDStr, reason)

	// For destroy operations, delete the deployment record after the version
	// completes successfully.
	if success && r.versionType == VersionTypeDestroy {
		err = r.svc.DeleteDeployment(ctx, bundledeployments.DeleteDeploymentRequest{
			Name: "deployments/" + r.lineage,
		})
		if err != nil {
			return fmt.Errorf("failed to delete deployment: %w", err)
		}
	}

	return nil
}

// createDeploymentVersion ensures the deployment record exists, then creates a
// new version. We GetDeployment first and only CreateDeployment when it does
// not exist yet.
func (r *Recorder) createDeploymentVersion(ctx context.Context) (versionID string, err error) {
	dep, getErr := r.svc.GetDeployment(ctx, bundledeployments.GetDeploymentRequest{
		Name: "deployments/" + r.lineage,
	})
	switch {
	case errors.Is(getErr, apierr.ErrNotFound):
		// Fresh deployment: create the record and start at version 1.
		_, createErr := r.svc.CreateDeployment(ctx, bundledeployments.CreateDeploymentRequest{
			DeploymentId: r.lineage,
			Deployment: bundledeployments.Deployment{
				TargetName: r.targetName,
			},
		})
		if createErr != nil {
			return "", fmt.Errorf("failed to create deployment: %w", createErr)
		}
		versionID = "1"
	case getErr != nil:
		return "", fmt.Errorf("failed to get deployment: %w", getErr)
	default:
		// Existing deployment: increment the last version to get the next one.
		lastVersion, parseErr := strconv.ParseInt(dep.LastVersionId, 10, 64)
		if parseErr != nil {
			return "", fmt.Errorf("failed to parse last_version_id %q: %w", dep.LastVersionId, parseErr)
		}
		versionID = strconv.FormatInt(lastVersion+1, 10)
	}

	// The server validates that versionID equals last_version_id + 1 and returns
	// ABORTED otherwise (e.g. a concurrent deploy already created this version).
	version, versionErr := r.svc.CreateVersion(ctx, bundledeployments.CreateVersionRequest{
		Parent:    "deployments/" + r.lineage,
		VersionId: versionID,
		Version: bundledeployments.Version{
			CliVersion:  build.GetInfo().Version,
			VersionType: r.versionType,
			TargetName:  r.targetName,
		},
	})
	if versionErr != nil {
		return "", fmt.Errorf("failed to create deployment version: %w", versionErr)
	}

	log.Infof(ctx, "Created deployment version: deployment=%s version=%s", r.lineage, version.VersionId)
	return versionID, nil
}

// startHeartbeat starts a background goroutine that sends heartbeats to keep
// the deployment version's lease alive. Returns a cancel function to stop it.
func startHeartbeat(ctx context.Context, svc bundledeployments.BundleDeploymentsInterface, lineage, versionID string) context.CancelFunc {
	ctx, cancel := context.WithCancel(ctx)
	versionName := fmt.Sprintf("deployments/%s/versions/%s", lineage, versionID)

	go func() {
		ticker := time.NewTicker(defaultHeartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_, err := svc.Heartbeat(ctx, bundledeployments.HeartbeatRequest{Name: versionName})
				if err != nil {
					// A 409 ABORTED is expected if the version was completed
					// between the ticker firing and the heartbeat.
					if isAbortedErr(err) {
						log.Debugf(ctx, "Heartbeat stopped: version already completed")
						return
					}
					log.Warnf(ctx, "Failed to send deployment heartbeat: %v", err)
				} else {
					log.Debugf(ctx, "Deployment heartbeat sent: deployment=%s version=%s", lineage, versionID)
				}
			}
		}
	}()

	return cancel
}

// isAbortedErr reports whether err is an HTTP 409 ABORTED from the DMS API.
func isAbortedErr(err error) bool {
	apiErr, ok := errors.AsType[*apierr.APIError](err)
	return ok && apiErr.StatusCode == http.StatusConflict && apiErr.ErrorCode == "ABORTED"
}
