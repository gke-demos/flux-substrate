package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	fluxmeta "github.com/fluxcd/pkg/apis/meta"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"github.com/gke-demos/flux-substrate/internal/actor"
)

const maxConditionMessageBytes = 4096

func patchFluxReconciling(
	ctx context.Context,
	kc client.Client,
	grKey client.ObjectKey,
	ksKey client.ObjectKey,
) error {
	now := metav1.NewTime(time.Now().UTC())

	if grKey.Name != "" {
		_ = updateGitRepositoryStatusWithRetry(ctx, kc, grKey, func(gr *sourcev1.GitRepository) {
			apimeta.SetStatusCondition(&gr.Status.Conditions, metav1.Condition{
				Type:               fluxmeta.ReconcilingCondition,
				Status:             metav1.ConditionTrue,
				ObservedGeneration: gr.Generation,
				LastTransitionTime: now,
				Reason:             fluxmeta.ProgressingReason,
				Message:            "Fetching Git source inside Workflow Actor",
			})
		})
	}

	return updateKustomizationStatusWithRetry(ctx, kc, ksKey, func(ks *kustomizev1.Kustomization) {
		apimeta.SetStatusCondition(&ks.Status.Conditions, metav1.Condition{
			Type:               fluxmeta.ReconcilingCondition,
			Status:             metav1.ConditionTrue,
			ObservedGeneration: ks.Generation,
			LastTransitionTime: now,
			Reason:             fluxmeta.ProgressingReason,
			Message:            "Executing Reconcile Turn inside Workflow Actor",
		})
	})
}

func patchFluxTurnOutcome(
	ctx context.Context,
	kc client.Client,
	actorName string,
	grKey client.ObjectKey,
	ksKey client.ObjectKey,
	res actor.ReconcileResult,
	statusCode int,
	invokeErr error,
) (*sourcev1.GitRepository, *kustomizev1.Kustomization, error) {
	now := metav1.NewTime(time.Now().UTC())

	var crashErr *ActorCrashError
	isCrashed := errors.As(invokeErr, &crashErr)
	succeeded := invokeErr == nil && statusCode == 200 && res.Ready && res.Error == ""

	errMsg := strings.TrimSpace(res.Error)
	if invokeErr != nil {
		if errMsg == "" {
			errMsg = invokeErr.Error()
		} else {
			errMsg = fmt.Sprintf("%s (%v)", errMsg, invokeErr)
		}
	}
	if errMsg == "" && !succeeded {
		errMsg = strings.TrimSpace(res.StatusMessage)
		if errMsg == "" {
			errMsg = fmt.Sprintf("reconcile failed with HTTP %d", statusCode)
		}
	}
	if len(errMsg) > maxConditionMessageBytes {
		errMsg = errMsg[:maxConditionMessageBytes]
	}

	var updatedGR *sourcev1.GitRepository
	if grKey.Name != "" {
		err := updateGitRepositoryStatusWithRetry(ctx, kc, grKey, func(gr *sourcev1.GitRepository) {
			gr.Status.ObservedGeneration = gr.Generation
			if reqAt := gr.Annotations[fluxmeta.ReconcileRequestAnnotation]; reqAt != "" {
				gr.Status.LastHandledReconcileAt = reqAt
			}
			apimeta.RemoveStatusCondition(&gr.Status.Conditions, fluxmeta.ReconcilingCondition)

			if res.CommitSHA != "" && res.Revision != "" {
				artifactReason := sourcev1.NewArtifactReason
				if gr.Status.Artifact != nil && gr.Status.Artifact.Revision == res.Revision {
					artifactReason = sourcev1.ArtifactUpToDateReason
				}
				gr.Status.Artifact = &fluxmeta.Artifact{
					Path:           fmt.Sprintf("gitrepository/%s/%s/%s.tar.gz", gr.Namespace, gr.Name, res.CommitSHA),
					URL:            fmt.Sprintf("http://%s.actor.internal/var/lib/flux/%s", actorName, gr.Name),
					Revision:       res.Revision,
					Digest:         "sha1:" + res.CommitSHA,
					LastUpdateTime: now,
				}
				apimeta.RemoveStatusCondition(&gr.Status.Conditions, sourcev1.FetchFailedCondition)
				apimeta.RemoveStatusCondition(&gr.Status.Conditions, fluxmeta.StalledCondition)
				apimeta.SetStatusCondition(&gr.Status.Conditions, metav1.Condition{
					Type:               sourcev1.ArtifactInStorageCondition,
					Status:             metav1.ConditionTrue,
					ObservedGeneration: gr.Generation,
					LastTransitionTime: now,
					Reason:             artifactReason,
					Message:            fmt.Sprintf("stored artifact for revision '%s'", res.Revision),
				})
				apimeta.SetStatusCondition(&gr.Status.Conditions, metav1.Condition{
					Type:               fluxmeta.ReadyCondition,
					Status:             metav1.ConditionTrue,
					ObservedGeneration: gr.Generation,
					LastTransitionTime: now,
					Reason:             sourcev1.GitOperationSucceedReason,
					Message:            fmt.Sprintf("stored artifact for revision '%s'", res.Revision),
				})
			} else {
				apimeta.SetStatusCondition(&gr.Status.Conditions, metav1.Condition{
					Type:               sourcev1.FetchFailedCondition,
					Status:             metav1.ConditionTrue,
					ObservedGeneration: gr.Generation,
					LastTransitionTime: now,
					Reason:             sourcev1.GitOperationFailedReason,
					Message:            errMsg,
				})
				apimeta.SetStatusCondition(&gr.Status.Conditions, metav1.Condition{
					Type:               fluxmeta.ReadyCondition,
					Status:             metav1.ConditionFalse,
					ObservedGeneration: gr.Generation,
					LastTransitionTime: now,
					Reason:             sourcev1.GitOperationFailedReason,
					Message:            errMsg,
				})
			}
			updatedGR = gr.DeepCopy()
		})
		if err != nil {
			return nil, nil, fmt.Errorf("update GitRepository status: %w", err)
		}
	}

	var updatedKS *kustomizev1.Kustomization
	err := updateKustomizationStatusWithRetry(ctx, kc, ksKey, func(ks *kustomizev1.Kustomization) {
		ks.Status.ObservedGeneration = ks.Generation
		if reqAt := ks.Annotations[fluxmeta.ReconcileRequestAnnotation]; reqAt != "" {
			ks.Status.LastHandledReconcileAt = reqAt
		}
		if res.Revision != "" {
			ks.Status.LastAttemptedRevision = res.Revision
		}
		apimeta.RemoveStatusCondition(&ks.Status.Conditions, fluxmeta.ReconcilingCondition)

		if succeeded {
			ks.Status.LastAppliedRevision = res.Revision
			refs := make([]kustomizev1.ResourceRef, 0, len(res.Inventory))
			for _, inv := range res.Inventory {
				refs = append(refs, kustomizev1.ResourceRef{
					ID:      inv.ID,
					Version: inv.Version,
				})
			}
			ks.Status.Inventory = &kustomizev1.ResourceInventory{Entries: refs}

			apimeta.RemoveStatusCondition(&ks.Status.Conditions, fluxmeta.StalledCondition)
			apimeta.SetStatusCondition(&ks.Status.Conditions, metav1.Condition{
				Type:               fluxmeta.HealthyCondition,
				Status:             metav1.ConditionTrue,
				ObservedGeneration: ks.Generation,
				LastTransitionTime: now,
				Reason:             fluxmeta.SucceededReason,
				Message:            res.StatusMessage,
			})
			apimeta.SetStatusCondition(&ks.Status.Conditions, metav1.Condition{
				Type:               fluxmeta.ReadyCondition,
				Status:             metav1.ConditionTrue,
				ObservedGeneration: ks.Generation,
				LastTransitionTime: now,
				Reason:             fluxmeta.ReconciliationSucceededReason,
				Message:            fmt.Sprintf("Applied revision: %s", res.Revision),
			})
		} else if isCrashed {
			apimeta.SetStatusCondition(&ks.Status.Conditions, metav1.Condition{
				Type:               fluxmeta.StalledCondition,
				Status:             metav1.ConditionTrue,
				ObservedGeneration: ks.Generation,
				LastTransitionTime: now,
				Reason:             "ActorCrashed",
				Message:            errMsg,
			})
			apimeta.SetStatusCondition(&ks.Status.Conditions, metav1.Condition{
				Type:               fluxmeta.ReadyCondition,
				Status:             metav1.ConditionFalse,
				ObservedGeneration: ks.Generation,
				LastTransitionTime: now,
				Reason:             "ActorCrashed",
				Message:            errMsg,
			})
		} else {
			reason := classifyFailureReason(res.FailedStage, statusCode)
			apimeta.RemoveStatusCondition(&ks.Status.Conditions, fluxmeta.StalledCondition)
			apimeta.SetStatusCondition(&ks.Status.Conditions, metav1.Condition{
				Type:               fluxmeta.ReadyCondition,
				Status:             metav1.ConditionFalse,
				ObservedGeneration: ks.Generation,
				LastTransitionTime: now,
				Reason:             reason,
				Message:            errMsg,
			})
		}
		updatedKS = ks.DeepCopy()
	})
	if err != nil {
		return updatedGR, nil, fmt.Errorf("update Kustomization status: %w", err)
	}

	if updatedKS != nil {
		emitKustomizationEvent(ctx, kc, updatedKS, succeeded, res.Revision, errMsg)
	}
	return updatedGR, updatedKS, nil
}

func classifyFailureReason(failedStage string, statusCode int) string {
	if statusCode == 403 {
		return "AccessDenied"
	}
	switch failedStage {
	case "fetch":
		return fluxmeta.ArtifactFailedReason
	case "render":
		return fluxmeta.BuildFailedReason
	case "prune":
		return fluxmeta.PruneFailedReason
	case "status":
		return fluxmeta.HealthCheckFailedReason
	default:
		return fluxmeta.ReconciliationFailedReason
	}
}

func updateGitRepositoryStatusWithRetry(
	ctx context.Context,
	kc client.Client,
	key client.ObjectKey,
	mutate func(gr *sourcev1.GitRepository),
) error {
	var lastErr error
	for range 4 {
		var gr sourcev1.GitRepository
		if err := kc.Get(ctx, key, &gr); err != nil {
			return err
		}
		mutate(&gr)
		if err := kc.Status().Update(ctx, &gr); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	return lastErr
}

func updateKustomizationStatusWithRetry(
	ctx context.Context,
	kc client.Client,
	key client.ObjectKey,
	mutate func(ks *kustomizev1.Kustomization),
) error {
	var lastErr error
	for range 4 {
		var ks kustomizev1.Kustomization
		if err := kc.Get(ctx, key, &ks); err != nil {
			return err
		}
		mutate(&ks)
		if err := kc.Status().Update(ctx, &ks); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	return lastErr
}

func emitKustomizationEvent(
	ctx context.Context,
	kc client.Client,
	ks *kustomizev1.Kustomization,
	succeeded bool,
	revision string,
	errMsg string,
) {
	evType := corev1.EventTypeNormal
	reason := fluxmeta.ReconciliationSucceededReason
	msg := fmt.Sprintf("Applied revision %s", revision)
	if !succeeded {
		evType = corev1.EventTypeWarning
		reason = fluxmeta.ReconciliationFailedReason
		msg = errMsg
	}
	now := metav1.NewTime(time.Now().UTC())
	ev := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: ks.Name + ".",
			Namespace:    ks.Namespace,
		},
		InvolvedObject: corev1.ObjectReference{
			APIVersion: kustomizev1.GroupVersion.String(),
			Kind:       kustomizev1.KustomizationKind,
			Namespace:  ks.Namespace,
			Name:       ks.Name,
			UID:        ks.UID,
		},
		Reason:         reason,
		Message:        msg,
		FirstTimestamp: now,
		LastTimestamp:  now,
		Count:          1,
		Type:           evType,
		Source:         corev1.EventSource{Component: "flux-substrate-dispatcher"},
	}
	_ = kc.Create(ctx, ev)
}

func marshalKustomizationYAML(ks *kustomizev1.Kustomization) string {
	if ks == nil {
		return ""
	}
	cleaned := ks.DeepCopy()
	cleaned.ManagedFields = nil
	if cleaned.APIVersion == "" {
		cleaned.APIVersion = kustomizev1.GroupVersion.String()
	}
	if cleaned.Kind == "" {
		cleaned.Kind = kustomizev1.KustomizationKind
	}
	raw, err := yaml.Marshal(cleaned)
	if err != nil {
		return ""
	}
	return string(raw)
}
