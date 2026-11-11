// Package dispatcher implements the cluster-resident coordinator that watches
// Flux GitRepository and Kustomization custom resources, receives webhooks,
// multiplexes Workflow Actors across a 9-slot WorkerPool FIFO scheduler, and
// serves Mission-Control telemetry and actions.
package dispatcher

import (
	"context"
	"fmt"
	"time"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/gke-demos/flux-substrate/internal/actor"
)

const (
	// ManagedByLabelKey scopes Flux CRs reconciled by flux-substrate.
	ManagedByLabelKey = "reconciler.ate.dev/managed-by"
	// ManagedByLabelValue is the label value identifying flux-substrate CRs.
	ManagedByLabelValue = "flux-substrate"
	// TenantLabelKey optionally maps a Flux CR directly to its Tenant Environment ID.
	TenantLabelKey = "reconciler.ate.dev/tenant"
	// CredentialURIAnnotation optionally overrides the ate-secret:// URI on a GitRepository.
	CredentialURIAnnotation = "reconciler.ate.dev/credential-uri"

	defaultWorkerCount       = 3
	defaultSlotsPerWorker    = 3
	defaultActorJWTLifetimeS = 600
	defaultAtespace          = "flux-substrate"
)

// ActorState describes the lifecycle state of a Workflow Actor.
type ActorState string

const (
	ActorStateUnknown   ActorState = "UNKNOWN"
	ActorStateSuspended ActorState = "SUSPENDED"
	ActorStateRunning   ActorState = "RUNNING"
	ActorStateCrashed   ActorState = "CRASHED"
)

// TriggerReason records why a Reconcile Turn was enqueued.
type TriggerReason string

const (
	TriggerCRSync       TriggerReason = "cr-sync"
	TriggerWebhook      TriggerReason = "webhook"
	TriggerInterval     TriggerReason = "interval"
	TriggerRetry        TriggerReason = "retry"
	TriggerCommit       TriggerReason = "commit"
	TriggerWave         TriggerReason = "wave"
	TriggerRecover      TriggerReason = "recover"
	TriggerReset        TriggerReason = "reset"
	TriggerPoisonOOM       TriggerReason = "poison-oom"
	TriggerPoisonRBAC      TriggerReason = "poison-rbac"
	TriggerPoisonEgress    TriggerReason = "poison-egress"
	TriggerPoisonBuggyCode TriggerReason = "poison-buggy-code"
	TriggerExternalRepo    TriggerReason = "external-repo"
)

// ActorSpec describes the desired Workflow Actor provisioned via ActorBackend.
type ActorSpec struct {
	Atespace  string `json:"atespace"`
	ActorName string `json:"actorName"`
	TenantID  string `json:"tenantId"`
}

// EgressPolicySpec describes the least-privilege per-actor EgressPolicy reconciled
// before waking a Workflow Actor (or whenever its GitRepository host/secret changes).
type EgressPolicySpec struct {
	Atespace             string `json:"atespace"`
	ActorName            string `json:"actorName"`
	GitScheme            string `json:"gitScheme"`
	GitHost              string `json:"gitHost"`
	GitCredentialURI     string `json:"gitCredentialUri,omitempty"`
	KubeBridgeHost       string `json:"kubeBridgeHost,omitempty"`
	KubeBridgeAudience   string `json:"kubeBridgeAudience,omitempty"`
	ActorJWTLifetimeSecs int64  `json:"actorJwtLifetimeSecs"`
}

// TurnInvocation carries the WorkerPool slot assignment and ReconcileRequest into ActorBackend.
type TurnInvocation struct {
	Atespace  string                 `json:"atespace"`
	ActorName string                 `json:"actorName"`
	WorkerID  string                 `json:"workerId"`
	SlotID    string                 `json:"slotId"`
	Request   actor.ReconcileRequest `json:"request"`
}

// ActorCrashError indicates that a Workflow Actor transitioned to ACTOR_STATE_CRASHED
// (for example, exceeding sandbox limits.memoryBytes during Kustomize evaluation).
type ActorCrashError struct {
	ActorName string
	Reason    string
}

func (e *ActorCrashError) Error() string {
	if e.Reason == "" {
		return fmt.Sprintf("Workflow Actor %s entered ACTOR_STATE_CRASHED", e.ActorName)
	}
	return fmt.Sprintf("Workflow Actor %s entered ACTOR_STATE_CRASHED: %s", e.ActorName, e.Reason)
}

// ActorBackend abstracts Agent Substrate v1.0.0 actor management and routing.
type ActorBackend interface {
	EnsureActor(ctx context.Context, spec ActorSpec) error
	EnsureEgressPolicy(ctx context.Context, policy EgressPolicySpec) error
	InvokeReconcile(ctx context.Context, inv TurnInvocation) (actor.ReconcileResult, int, error)
	SuspendActor(ctx context.Context, atespace, actorName string) error
	RevertActor(ctx context.Context, atespace, actorName string) error
}

// WorkerSlotStatus represents a single execution slot on a WorkerPool pod.
type WorkerSlotStatus struct {
	SlotID    string     `json:"slotId"`
	WorkerID  string     `json:"workerId"`
	SlotIndex int        `json:"slotIndex"`
	Busy      bool       `json:"busy"`
	TenantID  string     `json:"tenantId,omitempty"`
	ActorName string     `json:"actorName,omitempty"`
	StartedAt *time.Time `json:"startedAt,omitempty"`
}

// WorkerPodStatus represents one WorkerPool pod (W1..W3) and its execution slots.
type WorkerPodStatus struct {
	WorkerID string             `json:"workerId"`
	Capacity int                `json:"capacity"`
	Active   int                `json:"active"`
	Slots    []WorkerSlotStatus `json:"slots"`
}

// WorkerPoolStatus reports live WorkerPool occupancy and the waiting FIFO queue.
type WorkerPoolStatus struct {
	Workers []WorkerPodStatus `json:"workers"`
	Queue   []string          `json:"queue"`
}

// FleetSummary reports top-level Mission-Control KPI metrics.
type FleetSummary struct {
	TotalTenants        int    `json:"totalTenants"`
	ReadyTenants        int    `json:"readyTenants"`
	ActiveActors        int    `json:"activeActors"`
	SuspendedActors     int    `json:"suspendedActors"`
	CrashedActors       int    `json:"crashedActors"`
	QueuedTurns         int    `json:"queuedTurns"`
	WorkerCapacity      int    `json:"workerCapacity"`
	ActiveSlots         int    `json:"activeSlots"`
	PeakActiveSlots     int    `json:"peakActiveSlots"`
	TotalTurns          int64  `json:"totalTurns"`
	SnapshotCount       int64  `json:"snapshotCount"`
	MedianFetchMs       int64  `json:"medianFetchMs"`
	MedianCompileTestMs int64  `json:"medianCompileTestMs"`
	MedianRenderMs      int64  `json:"medianRenderMs"`
	MedianApplyMs       int64  `json:"medianApplyMs"`
	MedianTurnMs        int64  `json:"medianTurnMs"`
	DemoRepository      string `json:"demoRepository,omitempty"`
	SharedRepoURL       string `json:"sharedRepoUrl,omitempty"`
	SecretPosture       string `json:"secretPosture,omitempty"`
}

// TenantSummary captures fleet-grid status for one Tenant Environment.
type TenantSummary struct {
	TenantID              string             `json:"tenantId"`
	ActorName             string             `json:"actorName"`
	Namespace             string             `json:"namespace"`
	TargetNamespace       string             `json:"targetNamespace"`
	KustomizationName     string             `json:"kustomizationName"`
	GitRepositoryName     string             `json:"gitRepositoryName"`
	RepoURL               string             `json:"repoUrl"`
	Branch                string             `json:"branch"`
	ActorState            ActorState         `json:"actorState"`
	Queued                bool               `json:"queued"`
	QueuePosition         int                `json:"queuePosition"`
	WorkerID              string             `json:"workerId,omitempty"`
	SlotID                string             `json:"slotId,omitempty"`
	Ready                 bool               `json:"ready"`
	Reconciling           bool               `json:"reconciling"`
	Stalled               bool               `json:"stalled"`
	LastAppliedRevision   string             `json:"lastAppliedRevision,omitempty"`
	LastAttemptedRevision string             `json:"lastAttemptedRevision,omitempty"`
	CommitSHA             string             `json:"commitSha,omitempty"`
	LastCommitMessage     string             `json:"lastCommitMessage,omitempty"`
	CacheHit              bool               `json:"cacheHit"`
	BuildCacheHit         bool               `json:"buildCacheHit"`
	SecretPosture         string             `json:"secretPosture,omitempty"`
	TurnCount             int64              `json:"turnCount"`
	CoalescedTriggers     int64              `json:"coalescedTriggers"`
	SnapshotCount         int64              `json:"snapshotCount"`
	ResourceCount         int                `json:"resourceCount"`
	Timings               actor.PhaseTimings `json:"timings"`
	StatusMessage         string             `json:"statusMessage,omitempty"`
	LastError             string             `json:"lastError,omitempty"`
	FailedStage           string             `json:"failedStage,omitempty"`
	UpdatedAt             time.Time          `json:"updatedAt"`
}

// TenantDetail extends TenantSummary with full Tenant Inspector diagnostics.
type TenantDetail struct {
	TenantSummary
	LatestDiff          string                           `json:"latestDiff,omitempty"`
	EgressPolicy        *EgressPolicySpec                `json:"egressPolicy,omitempty"`
	Inventory           []actor.InventoryEntry           `json:"inventory"`
	PrunedObjects       []string                         `json:"prunedObjects"`
	Conditions          []metav1.Condition               `json:"conditions"`
	GitRepositoryStatus *sourcev1.GitRepositoryStatus    `json:"gitRepositoryStatus,omitempty"`
	KustomizationStatus *kustomizev1.KustomizationStatus `json:"kustomizationStatus,omitempty"`
	KustomizationYAML   string                           `json:"kustomizationYaml,omitempty"`
	RecentTurns         []TurnEvent                      `json:"recentTurns"`
}

// TurnEvent represents a single entry in the live Mission-Control event stream.
type TurnEvent struct {
	ID         int64         `json:"id"`
	Timestamp  time.Time     `json:"timestamp"`
	TenantID   string        `json:"tenantId"`
	ActorName  string        `json:"actorName"`
	WorkerID   string        `json:"workerId,omitempty"`
	SlotID     string        `json:"slotId,omitempty"`
	Trigger    TriggerReason `json:"trigger"`
	Outcome    string        `json:"outcome"`
	Revision   string        `json:"revision,omitempty"`
	CacheHit   bool          `json:"cacheHit"`
	DurationMs int64         `json:"durationMs"`
	Message    string        `json:"message"`
}

// FleetStateResponse is returned from GET /api/state.
type FleetStateResponse struct {
	Mode         string           `json:"mode"`
	Atespace     string           `json:"atespace"`
	AuthRequired bool             `json:"authRequired"`
	Summary      FleetSummary     `json:"summary"`
	WorkerPool   WorkerPoolStatus `json:"workerPool"`
	Tenants      []TenantSummary  `json:"tenants"`
	Events       []TurnEvent      `json:"events"`
}
