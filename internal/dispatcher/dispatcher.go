package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	fluxmeta "github.com/fluxcd/pkg/apis/meta"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gke-demos/flux-substrate/internal/actor"
	"github.com/gke-demos/flux-substrate/internal/gitserver"
	"github.com/gke-demos/flux-substrate/internal/httputil"
)

const (
	maxEventsHistory    = 120
	maxPerTenantTurns   = 25
	maxLatencySamples   = 96
	defaultCRSyncPeriod = 2 * time.Second
	maxRequestBodyBytes = 1 << 20 // 1 MiB API/webhook JSON body cap
)

var validTenantIDRe = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

// Config configures the Dispatcher coordinator and HTTP API server.
type Config struct {
	Mode           string
	Atespace       string
	WatchNamespace string
	KubeClient     client.Client
	Backend        ActorBackend
	GitServer      *gitserver.Server
	KubeBridgeURL  string
	WorkerCount    int
	SlotsPerWorker int
	CRSyncInterval time.Duration
	WebhookSecret  string
	PresenterToken string
}

type slotHandle struct {
	Index     int
	WorkerIdx int
	WorkerID  string
	SlotIndex int
	SlotID    string
}

type tenantState struct {
	tenantID          string
	actorName         string
	ksKey             client.ObjectKey
	grKey             client.ObjectKey
	targetNamespace   string
	repoURL           string
	baselineRepoURL   string
	branch            string
	path              string
	prune             bool
	actorEnsured      bool
	lastPolicy        *EgressPolicySpec
	actorState        ActorState
	queued            bool
	inFlight          int
	suspending        bool
	rerunPending      bool
	pendingReason     TriggerReason
	activeSlot        *slotHandle
	lastWorkerID      string
	lastSlotID        string
	ready             bool
	reconciling       bool
	stalled           bool
	lastAppliedRev    string
	lastAttemptedRev  string
	commitSHA         string
	commitSeq         int64
	lastCommitMessage string
	latestDiff        string
	cacheHit          bool
	buildCacheHit     bool
	secretPosture     string
	turnCount         int64
	coalescedTriggers int64
	snapshotCount     int64
	timings           actor.PhaseTimings
	inventory         []actor.InventoryEntry
	prunedObjects     []string
	conditions        []metav1.Condition
	lastGRStatus      *sourcev1.GitRepositoryStatus
	lastKSStatus      *kustomizev1.KustomizationStatus
	lastKSYAML        string
	statusMessage     string
	lastError         string
	failedStage       string
	updatedAt         time.Time
	nextDueAt         time.Time
	lastObservedKSGen int64
	lastObservedGRURL string
	lastHandledReqAt  string
	recentTurns       []TurnEvent
}

// Dispatcher watches Flux GitRepository and Kustomization CRs, schedules Workflow Actors
// across the WorkerPool FIFO slot queue, and serves the Mission-Control HTTP API.
type Dispatcher struct {
	cfg            Config
	mode           string
	atespace       string
	kubeClient     client.Client
	backend        ActorBackend
	gitServer      *gitserver.Server
	kubeBridgeHost string
	kubeBridgeAud  string
	workerCount    int
	slotsPerWorker int
	crSyncPeriod   time.Duration
	auth           *presenterAuthManager

	runCtx    context.Context
	runCancel context.CancelFunc
	wg        sync.WaitGroup

	schedMu          sync.Mutex
	tenants          map[string]*tenantState
	slots            []WorkerSlotStatus
	freeSlots        []slotHandle
	fifoQueue        []string
	activeSlots      int
	peakActiveSlots  int
	totalTurns       int64
	totalSnapshots   int64
	waveSeq          atomic.Int64
	eventSeq         atomic.Int64
	events           []TurnEvent
	fetchSamples     []int64
	compileSamples   []int64
	renderSamples    []int64
	applySamples     []int64
	turnSamples      []int64
	turnDoneCh       chan struct{}
	scheduleWakeupCh chan struct{}

	handler http.Handler
}

// New constructs a Dispatcher from cfg.
func New(cfg Config) (*Dispatcher, error) {
	if cfg.KubeClient == nil {
		return nil, errors.New("KubeClient is required")
	}
	if cfg.Backend == nil {
		return nil, errors.New("Backend is required")
	}

	mode := strings.TrimSpace(cfg.Mode)
	if mode == "" {
		mode = "dispatcher"
	}
	atespace := strings.TrimSpace(cfg.Atespace)
	if atespace == "" {
		atespace = defaultAtespace
	}
	workerCount := cfg.WorkerCount
	if workerCount <= 0 {
		workerCount = defaultWorkerCount
	}
	slotsPerWorker := cfg.SlotsPerWorker
	if slotsPerWorker <= 0 {
		slotsPerWorker = defaultSlotsPerWorker
	}
	syncPeriod := cfg.CRSyncInterval
	if syncPeriod <= 0 {
		syncPeriod = defaultCRSyncPeriod
	}

	var bridgeHost, bridgeAud string
	if rawBridge := strings.TrimSpace(cfg.KubeBridgeURL); rawBridge != "" {
		bridgeAud = rawBridge
		if u, err := url.Parse(rawBridge); err == nil && u.Host != "" {
			bridgeHost = u.Host
		}
	}

	totalSlots := workerCount * slotsPerWorker
	slots := make([]WorkerSlotStatus, totalSlots)
	for w := range workerCount {
		workerID := fmt.Sprintf("W%d", w+1)
		for s := range slotsPerWorker {
			idx := w*slotsPerWorker + s
			slotIdx := s + 1
			slots[idx] = WorkerSlotStatus{
				SlotID:    fmt.Sprintf("%s-%d", workerID, slotIdx),
				WorkerID:  workerID,
				SlotIndex: slotIdx,
			}
		}
	}

	// Order initial free slots round-robin across W1..WN so burst wakeups distribute evenly.
	freeSlots := make([]slotHandle, 0, totalSlots)
	for s := range slotsPerWorker {
		for w := range workerCount {
			idx := w*slotsPerWorker + s
			freeSlots = append(freeSlots, slotHandle{
				Index:     idx,
				WorkerIdx: w,
				WorkerID:  slots[idx].WorkerID,
				SlotIndex: slots[idx].SlotIndex,
				SlotID:    slots[idx].SlotID,
			})
		}
	}

	d := &Dispatcher{
		cfg:              cfg,
		mode:             mode,
		atespace:         atespace,
		kubeClient:       cfg.KubeClient,
		backend:          cfg.Backend,
		gitServer:        cfg.GitServer,
		kubeBridgeHost:   bridgeHost,
		kubeBridgeAud:    bridgeAud,
		workerCount:      workerCount,
		slotsPerWorker:   slotsPerWorker,
		crSyncPeriod:     syncPeriod,
		auth:             newPresenterAuthManager(cfg.PresenterToken),
		tenants:          make(map[string]*tenantState),
		slots:            slots,
		freeSlots:        freeSlots,
		turnDoneCh:       make(chan struct{}),
		scheduleWakeupCh: make(chan struct{}, 1),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /readyz", d.handleReadyz)
	mux.HandleFunc("GET /api/state", d.handleGetState)
	mux.HandleFunc("GET /api/status", d.handleGetState)
	mux.HandleFunc("GET /api/tenants/{id}", d.handleGetTenant)
	mux.HandleFunc("GET /api/auth/session", d.handleAuthSession)
	mux.HandleFunc("POST /api/auth/login", d.handleAuthLogin)
	mux.HandleFunc("POST /api/auth/logout", d.handleAuthLogout)
	d.registerActionRoutes(mux)
	d.registerUIRoutes(mux)

	d.handler = httputil.SecurityHeaders(mux)
	return d, nil
}

// Handler returns the HTTP handler for the Dispatcher API and webhook endpoints.
func (d *Dispatcher) Handler() http.Handler {
	return d.handler
}

// TurnCompletedNotify returns a channel that is closed and replaced whenever any
// Reconcile Turn + SuspendActor cycle finishes, enabling deterministic test waits
// without wall-clock sleeps.
func (d *Dispatcher) TurnCompletedNotify() <-chan struct{} {
	d.schedMu.Lock()
	defer d.schedMu.Unlock()
	return d.turnDoneCh
}

// Start performs an initial Flux CR discovery sync and launches the background
// CR watcher and spec.interval / spec.retryInterval timer scheduler.
func (d *Dispatcher) Start(ctx context.Context) error {
	d.runCtx, d.runCancel = context.WithCancel(ctx)
	if err := d.SyncCRs(d.runCtx); err != nil {
		return err
	}

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		d.scheduleLoop(d.runCtx)
	}()
	return nil
}

// Close stops background loops and waits for in-flight Reconcile Turns to drain.
func (d *Dispatcher) Close() {
	if d.runCancel != nil {
		d.runCancel()
	}
	d.wg.Wait()
}

// SyncCRs discovers Flux GitRepository and Kustomization CRs labeled
// reconciler.ate.dev/managed-by=flux-substrate and enqueues initial or spec-changed turns.
func (d *Dispatcher) SyncCRs(ctx context.Context) error {
	var ksList kustomizev1.KustomizationList
	listOpts := []client.ListOption{
		client.MatchingLabels{ManagedByLabelKey: ManagedByLabelValue},
	}
	if ns := strings.TrimSpace(d.cfg.WatchNamespace); ns != "" {
		listOpts = append(listOpts, client.InNamespace(ns))
	}
	if err := d.kubeClient.List(ctx, &ksList, listOpts...); err != nil {
		return fmt.Errorf("list managed Kustomizations: %w", err)
	}

	for i := range ksList.Items {
		ks := &ksList.Items[i]
		tenantID := resolveTenantID(ks)
		if !validTenantIDRe.MatchString(tenantID) {
			continue
		}

		srcNS := ks.Spec.SourceRef.Namespace
		if srcNS == "" {
			srcNS = ks.Namespace
		}
		grKey := client.ObjectKey{Namespace: srcNS, Name: ks.Spec.SourceRef.Name}

		var gr sourcev1.GitRepository
		var repoURL, branch string
		if err := d.kubeClient.Get(ctx, grKey, &gr); err == nil {
			repoURL = strings.TrimSpace(gr.Spec.URL)
			if gr.Spec.Reference != nil && strings.TrimSpace(gr.Spec.Reference.Branch) != "" {
				branch = strings.TrimSpace(gr.Spec.Reference.Branch)
			}
		}
		if branch == "" {
			branch = "main"
		}

		targetNS := strings.TrimSpace(ks.Spec.TargetNamespace)
		if targetNS == "" {
			targetNS = ks.Namespace
		}
		overlayPath := strings.TrimSpace(ks.Spec.Path)
		if overlayPath == "" {
			overlayPath = "./"
		}
		reqAt := ks.Annotations[fluxmeta.ReconcileRequestAnnotation]
		if reqAt == "" && gr.Annotations != nil {
			reqAt = gr.Annotations[fluxmeta.ReconcileRequestAnnotation]
		}

		shouldEnqueue := false
		d.schedMu.Lock()
		ts, exists := d.tenants[tenantID]
		if !exists {
			ts = &tenantState{
				tenantID:        tenantID,
				actorName:       tenantID,
				ksKey:           client.ObjectKeyFromObject(ks),
				grKey:           grKey,
				targetNamespace: targetNS,
				repoURL:         repoURL,
				baselineRepoURL: repoURL,
				branch:          branch,
				path:            overlayPath,
				prune:           ks.Spec.Prune,
				actorState:      ActorStateSuspended,
				inventory:       []actor.InventoryEntry{},
				prunedObjects:   []string{},
				conditions:      slices.Clone(ks.Status.Conditions),
				lastKSYAML:      marshalKustomizationYAML(ks),
				updatedAt:       time.Now().UTC(),
			}
			d.tenants[tenantID] = ts
			if !ks.Spec.Suspend && !gr.Spec.Suspend {
				shouldEnqueue = true
			}
		} else {
			ts.ksKey = client.ObjectKeyFromObject(ks)
			ts.grKey = grKey
			ts.targetNamespace = targetNS
			if repoURL != "" {
				ts.repoURL = repoURL
			}
			ts.branch = branch
			ts.path = overlayPath
			ts.prune = ks.Spec.Prune

			if !ks.Spec.Suspend && !gr.Spec.Suspend {
				if ks.Generation != ts.lastObservedKSGen ||
					(repoURL != "" && repoURL != ts.lastObservedGRURL) ||
					(reqAt != "" && reqAt != ts.lastHandledReqAt) {
					shouldEnqueue = true
				}
			}
		}
		d.schedMu.Unlock()

		if shouldEnqueue {
			d.Enqueue(tenantID, TriggerCRSync)
		}
	}
	return nil
}

// Enqueue schedules a Reconcile Turn for tenantID on the strict 9-slot FIFO queue,
// coalescing duplicate triggers if the tenant is already queued or running.
func (d *Dispatcher) Enqueue(tenantID string, reason TriggerReason) bool {
	tenantID = strings.TrimSpace(tenantID)
	d.schedMu.Lock()
	defer d.schedMu.Unlock()

	ts, exists := d.tenants[tenantID]
	if !exists {
		return false
	}
	if ts.actorState == ActorStateCrashed && reason != TriggerRecover {
		return false
	}

	if ts.queued {
		ts.coalescedTriggers++
		ts.pendingReason = reason
		return true
	}
	if ts.inFlight > 0 || ts.suspending {
		ts.coalescedTriggers++
		ts.rerunPending = true
		ts.pendingReason = reason
		return true
	}

	ts.queued = true
	ts.pendingReason = reason
	d.fifoQueue = append(d.fifoQueue, tenantID)
	d.dispatchLocked()
	return true
}

func (d *Dispatcher) dispatchLocked() {
	if d.runCtx != nil && d.runCtx.Err() != nil {
		return
	}
	for len(d.fifoQueue) > 0 && len(d.freeSlots) > 0 {
		tenantID := d.fifoQueue[0]
		d.fifoQueue = d.fifoQueue[1:]

		ts, ok := d.tenants[tenantID]
		if !ok {
			continue
		}

		slot := d.freeSlots[0]
		d.freeSlots = d.freeSlots[1:]

		now := time.Now().UTC()
		slotCopy := slot
		ts.queued = false
		ts.inFlight = 1
		ts.rerunPending = false
		ts.activeSlot = &slotCopy
		ts.lastWorkerID = slot.WorkerID
		ts.lastSlotID = slot.SlotID
		ts.actorState = ActorStateRunning
		ts.reconciling = true
		ts.updatedAt = now

		d.slots[slot.Index].Busy = true
		d.slots[slot.Index].TenantID = tenantID
		d.slots[slot.Index].ActorName = ts.actorName
		d.slots[slot.Index].StartedAt = &now

		d.activeSlots++
		if d.activeSlots > d.peakActiveSlots {
			d.peakActiveSlots = d.activeSlots
		}

		reason := ts.pendingReason
		d.wg.Add(1)
		go func(tid string, sh slotHandle, trig TriggerReason) {
			defer d.wg.Done()
			d.runSlotLoop(tid, sh, trig)
		}(tenantID, slot, reason)
	}
}

func (d *Dispatcher) runSlotLoop(tenantID string, slot slotHandle, initialReason TriggerReason) {
	reason := initialReason
	for {
		ctx := d.runCtx
		if ctx == nil {
			ctx = context.Background()
		}

		crashed := d.executeSingleTurn(ctx, tenantID, slot, reason)

		d.schedMu.Lock()
		ts := d.tenants[tenantID]
		// If a burst arrived mid-turn and no other tenant is waiting in the FIFO queue,
		// immediately run the single coalesced follow-up turn while keeping inFlight == 1
		// so SuspendActor only fires once inFlight == 0.
		if !crashed && ts.rerunPending && len(d.fifoQueue) == 0 && ctx.Err() == nil {
			ts.rerunPending = false
			reason = ts.pendingReason
			d.schedMu.Unlock()
			continue
		}

		// Transition inFlight -> 0 and hold suspending=true while SuspendActor executes
		// so concurrent webhooks coalesce cleanly and the WorkerPool slot is vacated safely.
		needsRequeue := !crashed && ts.rerunPending && ctx.Err() == nil
		requeueReason := ts.pendingReason
		ts.rerunPending = false
		ts.inFlight = 0
		ts.suspending = !crashed
		actorName := ts.actorName
		d.schedMu.Unlock()

		var suspendErr error
		if !crashed {
			suspendCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			suspendErr = d.backend.SuspendActor(suspendCtx, d.atespace, actorName)
			cancel()
		}

		d.schedMu.Lock()
		ts.suspending = false
		ts.activeSlot = nil
		ts.reconciling = false
		if crashed {
			ts.actorState = ActorStateCrashed
		} else {
			ts.actorState = ActorStateSuspended
			if suspendErr == nil {
				ts.snapshotCount++
				d.totalSnapshots++
			}
		}

		d.slots[slot.Index].Busy = false
		d.slots[slot.Index].TenantID = ""
		d.slots[slot.Index].ActorName = ""
		d.slots[slot.Index].StartedAt = nil
		d.activeSlots--
		d.freeSlots = append(d.freeSlots, slot)

		if (needsRequeue || ts.rerunPending) && !crashed && ctx.Err() == nil {
			if ts.rerunPending {
				requeueReason = ts.pendingReason
				ts.rerunPending = false
			}
			ts.queued = true
			ts.pendingReason = requeueReason
			d.fifoQueue = append(d.fifoQueue, tenantID)
		}

		// Broadcast turn completion before dispatching the freed slot.
		close(d.turnDoneCh)
		d.turnDoneCh = make(chan struct{})

		d.dispatchLocked()
		d.schedMu.Unlock()
		d.pokeScheduleLoop()
		return
	}
}

func (d *Dispatcher) executeSingleTurn(ctx context.Context, tenantID string, slot slotHandle, reason TriggerReason) bool {
	turnStart := time.Now()

	d.schedMu.Lock()
	ts := d.tenants[tenantID]
	ksKey := ts.ksKey
	grKey := ts.grKey
	actorName := ts.actorName
	actorEnsured := ts.actorEnsured
	var prevPolicy EgressPolicySpec
	hasPrevPolicy := ts.lastPolicy != nil
	if hasPrevPolicy {
		prevPolicy = *ts.lastPolicy
	}
	d.schedMu.Unlock()

	var ks kustomizev1.Kustomization
	if err := d.kubeClient.Get(ctx, ksKey, &ks); err != nil {
		d.recordFailedPrecheck(tenantID, slot, reason, turnStart, fmt.Sprintf("load Kustomization: %v", err))
		return false
	}

	srcNS := ks.Spec.SourceRef.Namespace
	if srcNS == "" {
		srcNS = ks.Namespace
	}
	grKey = client.ObjectKey{Namespace: srcNS, Name: ks.Spec.SourceRef.Name}

	var gr sourcev1.GitRepository
	if err := d.kubeClient.Get(ctx, grKey, &gr); err != nil {
		_, updatedKS, _ := patchFluxTurnOutcome(ctx, d.kubeClient, actorName, client.ObjectKey{}, ksKey, actor.ReconcileResult{
			FailedStage: "fetch",
			Error:       fmt.Sprintf("GitRepository %s not found: %v", grKey, err),
		}, http.StatusBadGateway, err)
		d.recordCompletedTurn(tenantID, slot, reason, turnStart, nil, updatedKS, actor.ReconcileResult{
			FailedStage: "fetch",
			Error:       err.Error(),
		}, http.StatusBadGateway, err)
		return false
	}

	if ks.Spec.Suspend || gr.Spec.Suspend {
		d.schedMu.Lock()
		ts.reconciling = false
		ts.lastObservedKSGen = ks.Generation
		ts.lastObservedGRURL = gr.Spec.URL
		d.schedMu.Unlock()
		return false
	}

	_ = patchFluxReconciling(ctx, d.kubeClient, grKey, ksKey)

	if !actorEnsured {
		if err := d.backend.EnsureActor(ctx, ActorSpec{
			Atespace:  d.atespace,
			ActorName: actorName,
			TenantID:  tenantID,
		}); err != nil {
			_, updatedKS, _ := patchFluxTurnOutcome(ctx, d.kubeClient, actorName, grKey, ksKey, actor.ReconcileResult{
				FailedStage: "actor",
			}, http.StatusBadGateway, err)
			d.recordCompletedTurn(tenantID, slot, reason, turnStart, &gr, updatedKS, actor.ReconcileResult{
				FailedStage: "actor",
				Error:       err.Error(),
			}, http.StatusBadGateway, err)
			return false
		}
		d.schedMu.Lock()
		ts.actorEnsured = true
		d.schedMu.Unlock()
	}

	desiredPolicy, err := d.synthesizeEgressPolicy(&gr, actorName)
	if err != nil {
		_, updatedKS, _ := patchFluxTurnOutcome(ctx, d.kubeClient, actorName, grKey, ksKey, actor.ReconcileResult{
			FailedStage: "egress",
		}, http.StatusBadRequest, err)
		d.recordCompletedTurn(tenantID, slot, reason, turnStart, &gr, updatedKS, actor.ReconcileResult{
			FailedStage: "egress",
			Error:       err.Error(),
		}, http.StatusBadRequest, err)
		return false
	}

	if !hasPrevPolicy || desiredPolicy != prevPolicy {
		if err := d.backend.EnsureEgressPolicy(ctx, desiredPolicy); err != nil {
			_, updatedKS, _ := patchFluxTurnOutcome(ctx, d.kubeClient, actorName, grKey, ksKey, actor.ReconcileResult{
				FailedStage: "egress",
			}, http.StatusBadGateway, err)
			d.recordCompletedTurn(tenantID, slot, reason, turnStart, &gr, updatedKS, actor.ReconcileResult{
				FailedStage: "egress",
				Error:       err.Error(),
			}, http.StatusBadGateway, err)
			return false
		}
		d.schedMu.Lock()
		policyCopy := desiredPolicy
		ts.lastPolicy = &policyCopy
		d.schedMu.Unlock()
	}

	branch := "main"
	if gr.Spec.Reference != nil && strings.TrimSpace(gr.Spec.Reference.Branch) != "" {
		branch = strings.TrimSpace(gr.Spec.Reference.Branch)
	}
	overlayPath := strings.TrimSpace(ks.Spec.Path)
	if overlayPath == "" {
		overlayPath = "./"
	}
	targetNS := strings.TrimSpace(ks.Spec.TargetNamespace)
	if targetNS == "" {
		targetNS = ks.Namespace
	}
	prune := ks.Spec.Prune

	var prevInv []actor.InventoryEntry
	if ks.Status.Inventory != nil {
		prevInv = make([]actor.InventoryEntry, 0, len(ks.Status.Inventory.Entries))
		for _, item := range ks.Status.Inventory.Entries {
			prevInv = append(prevInv, actor.InventoryEntry{
				ID:      item.ID,
				Version: item.Version,
			})
		}
	}

	var ociRegistryURL string
	repoURLTrimmed := strings.TrimSpace(gr.Spec.URL)
	if (tenantID == "tenant-01" || tenantID == "fulfillment-flux" || tenantID == "fulfillment-api") &&
		strings.Contains(repoURLTrimmed, "git-oci.demo-repo.svc.cluster.local") {
		if u, parseErr := url.Parse(repoURLTrimmed); parseErr == nil && u.Scheme != "" && u.Host != "" {
			ociRegistryURL = u.Scheme + "://" + u.Host
		}
	}

	res, statusCode, invokeErr := d.backend.InvokeReconcile(ctx, TurnInvocation{
		Atespace:  d.atespace,
		ActorName: actorName,
		WorkerID:  slot.WorkerID,
		SlotID:    slot.SlotID,
		Request: actor.ReconcileRequest{
			TenantID:          tenantID,
			WorkflowName:      ks.Name,
			RepoURL:           repoURLTrimmed,
			Branch:            branch,
			Path:              overlayPath,
			TargetNamespace:   targetNS,
			Prune:             &prune,
			PreviousInventory: prevInv,
			OCIRegistryURL:    ociRegistryURL,
			OCIRepository:     "fulfillment-api",
		},
	})

	succeeded := invokeErr == nil && statusCode == 200 && res.Ready && res.Error == ""
	if succeeded && (tenantID == "tenant-01" || tenantID == "fulfillment-flux" || tenantID == "fulfillment-api") {
		if ociRegistryURL != "" && res.OCIDigest != "" {
			_, _, _ = actor.PushFulfillmentOCI(
				ctx,
				nil,
				ociRegistryURL,
				"fulfillment-api",
				"",
				res.ShortSHA,
				res.PromoBanner,
				res.EnterpriseDiscountBps,
			)
		}
		d.rolloutFulfillmentFluxDeployment(ctx, gr.Namespace, res)
	}

	updatedGR, updatedKS, _ := patchFluxTurnOutcome(ctx, d.kubeClient, actorName, grKey, ksKey, res, statusCode, invokeErr)
	if updatedGR == nil {
		updatedGR = &gr
	}
	if updatedKS == nil {
		updatedKS = &ks
	}

	return d.recordCompletedTurn(tenantID, slot, reason, turnStart, updatedGR, updatedKS, res, statusCode, invokeErr)
}

func (d *Dispatcher) rolloutFulfillmentFluxDeployment(ctx context.Context, namespace string, res actor.ReconcileResult) {
	ns := strings.TrimSpace(namespace)
	if ns == "" {
		ns = d.atespace
	}
	shortSHA := strings.TrimSpace(res.ShortSHA)
	if shortSHA == "" && len(res.CommitSHA) >= 7 {
		shortSHA = res.CommitSHA[:7]
	}
	if shortSHA == "" {
		return
	}
	digest := strings.TrimSpace(res.OCIDigest)
	tag := strings.TrimSpace(res.OCITag)
	if tag == "" {
		tag = "flux-latest"
	}

	key := client.ObjectKey{Namespace: ns, Name: "fulfillment-flux"}
	for range 4 {
		var dep appsv1.Deployment
		if err := d.kubeClient.Get(ctx, key, &dep); err != nil {
			return
		}
		if dep.Spec.Template.Annotations == nil {
			dep.Spec.Template.Annotations = make(map[string]string)
		}
		dep.Spec.Template.Annotations["fulfillment.ate.dev/commit"] = shortSHA
		if digest != "" {
			dep.Spec.Template.Annotations["fulfillment.ate.dev/digest"] = digest
		}
		for i := range dep.Spec.Template.Spec.Containers {
			c := &dep.Spec.Template.Spec.Containers[i]
			if c.Name != "fulfillment-api" && len(dep.Spec.Template.Spec.Containers) > 1 {
				continue
			}
			c.Env = upsertEnvVar(c.Env, "DEPLOYED_BY", "flux")
			c.Env = upsertEnvVar(c.Env, "OCI_TAG", tag)
			c.Env = upsertEnvVar(c.Env, "BUILD_COMMIT", shortSHA)
			if digest != "" {
				c.Env = upsertEnvVar(c.Env, "OCI_BLOB_DIGEST", digest)
			}
		}
		if err := d.kubeClient.Update(ctx, &dep); err == nil {
			return
		}
	}
}

func upsertEnvVar(envs []corev1.EnvVar, name, value string) []corev1.EnvVar {
	for i := range envs {
		if envs[i].Name == name {
			envs[i].Value = value
			envs[i].ValueFrom = nil
			return envs
		}
	}
	return append(envs, corev1.EnvVar{Name: name, Value: value})
}

func (d *Dispatcher) synthesizeEgressPolicy(gr *sourcev1.GitRepository, actorName string) (EgressPolicySpec, error) {
	parsedURL, err := url.Parse(strings.TrimSpace(gr.Spec.URL))
	if err != nil || parsedURL.Host == "" {
		return EgressPolicySpec{}, fmt.Errorf("invalid GitRepository URL %q", gr.Spec.URL)
	}
	scheme := strings.ToLower(parsedURL.Scheme)
	if scheme != "https" && scheme != "http" {
		return EgressPolicySpec{}, fmt.Errorf("unsupported GitRepository scheme %q (expected https or http)", parsedURL.Scheme)
	}

	var credURI string
	if gr.Annotations != nil {
		credURI = strings.TrimSpace(gr.Annotations[CredentialURIAnnotation])
	}
	if credURI == "" && gr.Spec.SecretRef != nil && strings.TrimSpace(gr.Spec.SecretRef.Name) != "" {
		credURI = fmt.Sprintf("ate-secret://k8s.io/default/%s/%s/token", gr.Namespace, strings.TrimSpace(gr.Spec.SecretRef.Name))
	}

	return EgressPolicySpec{
		Atespace:             d.atespace,
		ActorName:            actorName,
		GitScheme:            scheme,
		GitHost:              parsedURL.Host,
		GitCredentialURI:     credURI,
		KubeBridgeHost:       d.kubeBridgeHost,
		KubeBridgeAudience:   d.kubeBridgeAud,
		ActorJWTLifetimeSecs: defaultActorJWTLifetimeS,
	}, nil
}

func (d *Dispatcher) recordFailedPrecheck(tenantID string, slot slotHandle, reason TriggerReason, turnStart time.Time, msg string) {
	now := time.Now().UTC()
	durMs := time.Since(turnStart).Milliseconds()

	d.schedMu.Lock()
	defer d.schedMu.Unlock()
	ts := d.tenants[tenantID]
	ts.ready = false
	ts.lastError = msg
	ts.statusMessage = msg
	ts.updatedAt = now
	ts.turnCount++
	d.totalTurns++
	ts.nextDueAt = now.Add(5 * time.Second)
	d.appendTurnEventLocked(ts, TurnEvent{
		ID:         d.eventSeq.Add(1),
		Timestamp:  now,
		TenantID:   tenantID,
		ActorName:  ts.actorName,
		WorkerID:   slot.WorkerID,
		SlotID:     slot.SlotID,
		Trigger:    reason,
		Outcome:    "failed",
		DurationMs: durMs,
		Message:    msg,
	})
}

func (d *Dispatcher) recordCompletedTurn(
	tenantID string,
	slot slotHandle,
	reason TriggerReason,
	turnStart time.Time,
	gr *sourcev1.GitRepository,
	ks *kustomizev1.Kustomization,
	res actor.ReconcileResult,
	statusCode int,
	invokeErr error,
) bool {
	now := time.Now().UTC()
	durMs := time.Since(turnStart).Milliseconds()
	if res.Timings.TotalMs <= 0 {
		res.Timings.TotalMs = durMs
	}

	var crashErr *ActorCrashError
	crashed := errors.As(invokeErr, &crashErr)
	succeeded := invokeErr == nil && statusCode == 200 && res.Ready && res.Error == ""

	d.schedMu.Lock()
	defer d.schedMu.Unlock()

	ts := d.tenants[tenantID]
	ts.turnCount++
	d.totalTurns++
	ts.updatedAt = now
	ts.timings = res.Timings
	ts.cacheHit = res.CacheHit
	ts.buildCacheHit = res.BuildCacheHit
	if res.SecretPosture != "" {
		ts.secretPosture = res.SecretPosture
	} else {
		ts.secretPosture = "ZERO_AMBIENT_SECRETS"
	}

	if gr != nil {
		ts.repoURL = gr.Spec.URL
		ts.lastObservedGRURL = gr.Spec.URL
		cpGR := gr.Status.DeepCopy()
		ts.lastGRStatus = cpGR
	}
	if ks != nil {
		ts.lastObservedKSGen = ks.Generation
		ts.lastHandledReqAt = ks.Status.LastHandledReconcileAt
		ts.conditions = slices.Clone(ks.Status.Conditions)
		cpKS := ks.Status.DeepCopy()
		ts.lastKSStatus = cpKS
		ts.lastKSYAML = marshalKustomizationYAML(ks)
		ts.lastAppliedRev = ks.Status.LastAppliedRevision
		ts.lastAttemptedRev = ks.Status.LastAttemptedRevision
	}

	if res.CommitSHA != "" {
		ts.commitSHA = res.CommitSHA
	}
	if res.Inventory != nil {
		ts.inventory = slices.Clone(res.Inventory)
	}
	if res.PrunedObjects != nil {
		ts.prunedObjects = slices.Clone(res.PrunedObjects)
	}

	var outcome, eventMsg string
	switch {
	case succeeded:
		ts.ready = true
		ts.stalled = false
		ts.lastError = ""
		ts.failedStage = ""
		ts.statusMessage = res.StatusMessage
		outcome = "succeeded"
		eventMsg = fmt.Sprintf("Applied %s (%d resources)", res.Revision, len(ts.inventory))
		if ks != nil && ks.GetRequeueAfter() > 0 {
			ts.nextDueAt = now.Add(ks.GetRequeueAfter())
		}
		d.recordLatencySampleLocked(res.Timings)
	case crashed:
		ts.ready = false
		ts.stalled = true
		if res.FailedStage != "" {
			ts.failedStage = res.FailedStage
		} else {
			ts.failedStage = "oom"
		}
		errText := strings.TrimSpace(res.Error)
		if errText == "" {
			errText = crashErr.Error()
		}
		ts.lastError = errText
		ts.statusMessage = errText
		ts.nextDueAt = time.Time{} // Crashed actors stay halted until RevertActor.
		outcome = "crashed"
		eventMsg = errText
	default:
		ts.ready = false
		ts.stalled = false
		ts.failedStage = res.FailedStage
		errText := strings.TrimSpace(res.Error)
		if errText == "" && invokeErr != nil {
			errText = invokeErr.Error()
		}
		if errText == "" {
			errText = res.StatusMessage
		}
		ts.lastError = errText
		ts.statusMessage = errText
		outcome = "failed"
		eventMsg = errText
		if ks != nil && ks.GetRetryInterval() > 0 {
			ts.nextDueAt = now.Add(ks.GetRetryInterval())
		}
	}

	d.appendTurnEventLocked(ts, TurnEvent{
		ID:         d.eventSeq.Add(1),
		Timestamp:  now,
		TenantID:   tenantID,
		ActorName:  ts.actorName,
		WorkerID:   slot.WorkerID,
		SlotID:     slot.SlotID,
		Trigger:    reason,
		Outcome:    outcome,
		Revision:   res.Revision,
		CacheHit:   res.CacheHit,
		DurationMs: res.Timings.TotalMs,
		Message:    eventMsg,
	})

	return crashed
}

func (d *Dispatcher) appendTurnEventLocked(ts *tenantState, ev TurnEvent) {
	d.events = append(d.events, ev)
	if len(d.events) > maxEventsHistory {
		d.events = slices.Clone(d.events[len(d.events)-maxEventsHistory:])
	}
	if ts != nil {
		ts.recentTurns = append(ts.recentTurns, ev)
		if len(ts.recentTurns) > maxPerTenantTurns {
			ts.recentTurns = slices.Clone(ts.recentTurns[len(ts.recentTurns)-maxPerTenantTurns:])
		}
	}
}

func (d *Dispatcher) recordLatencySampleLocked(t actor.PhaseTimings) {
	appendBounded := func(buf []int64, v int64) []int64 {
		buf = append(buf, v)
		if len(buf) > maxLatencySamples {
			buf = slices.Clone(buf[len(buf)-maxLatencySamples:])
		}
		return buf
	}
	d.fetchSamples = appendBounded(d.fetchSamples, t.FetchMs)
	d.compileSamples = appendBounded(d.compileSamples, max(1, t.CompileTestMs))
	d.renderSamples = appendBounded(d.renderSamples, t.RenderMs)
	d.applySamples = appendBounded(d.applySamples, t.ApplyMs)
	d.turnSamples = appendBounded(d.turnSamples, t.TotalMs)
}

func medianInt64(samples []int64) int64 {
	if len(samples) == 0 {
		return 0
	}
	cp := slices.Clone(samples)
	slices.Sort(cp)
	mid := len(cp) / 2
	if len(cp)%2 == 1 {
		return cp[mid]
	}
	return (cp[mid-1] + cp[mid]) / 2
}

func (d *Dispatcher) pokeScheduleLoop() {
	select {
	case d.scheduleWakeupCh <- struct{}{}:
	default:
	}
}

func (d *Dispatcher) scheduleLoop(ctx context.Context) {
	for {
		now := time.Now().UTC()
		waitDur := d.crSyncPeriod

		var dueTenants []struct {
			id     string
			reason TriggerReason
		}

		d.schedMu.Lock()
		for id, ts := range d.tenants {
			if ts.actorState == ActorStateCrashed || ts.nextDueAt.IsZero() {
				continue
			}
			if !now.Before(ts.nextDueAt) {
				trig := TriggerInterval
				if !ts.ready {
					trig = TriggerRetry
				}
				// Advance nextDueAt before enqueuing so we don't spin while queued.
				ts.nextDueAt = now.Add(d.crSyncPeriod)
				dueTenants = append(dueTenants, struct {
					id     string
					reason TriggerReason
				}{id: id, reason: trig})
				continue
			}
			if rem := ts.nextDueAt.Sub(now); rem < waitDur {
				waitDur = rem
			}
		}
		d.schedMu.Unlock()

		for _, item := range dueTenants {
			d.Enqueue(item.id, item.reason)
		}

		if waitDur < 10*time.Millisecond {
			waitDur = 10 * time.Millisecond
		}
		timer := time.NewTimer(waitDur)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-d.scheduleWakeupCh:
			timer.Stop()
		case <-timer.C:
			_ = d.SyncCRs(ctx)
		}
	}
}

func (d *Dispatcher) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	httputil.WriteJSON(w, http.StatusOK, map[string]string{
		"status":   "ready",
		"mode":     d.mode,
		"atespace": d.atespace,
	})
}

func (d *Dispatcher) handleGetState(w http.ResponseWriter, _ *http.Request) {
	httputil.WriteJSON(w, http.StatusOK, d.SnapshotState())
}

func (d *Dispatcher) handleGetTenant(w http.ResponseWriter, r *http.Request) {
	tenantID := strings.TrimSpace(r.PathValue("id"))
	if !validTenantIDRe.MatchString(tenantID) {
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant identifier"})
		return
	}
	detail, ok := d.SnapshotTenant(tenantID)
	if !ok {
		httputil.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "tenant not found"})
		return
	}
	httputil.WriteJSON(w, http.StatusOK, detail)
}

// SnapshotState builds a consistent read snapshot of the entire fleet and WorkerPool.
func (d *Dispatcher) SnapshotState() FleetStateResponse {
	d.schedMu.Lock()
	defer d.schedMu.Unlock()

	queuePositions := make(map[string]int, len(d.fifoQueue))
	for i, tid := range d.fifoQueue {
		queuePositions[tid] = i + 1
	}

	ids := make([]string, 0, len(d.tenants))
	for id := range d.tenants {
		ids = append(ids, id)
	}
	slices.Sort(ids)

	summaries := make([]TenantSummary, 0, len(ids))
	var readyCount, activeActors, suspendedActors, crashedActors int
	for _, id := range ids {
		ts := d.tenants[id]
		sum := buildTenantSummaryLocked(ts, queuePositions[id])
		summaries = append(summaries, sum)
		if sum.Ready {
			readyCount++
		}
		switch sum.ActorState {
		case ActorStateRunning:
			activeActors++
		case ActorStateCrashed:
			crashedActors++
		default:
			suspendedActors++
		}
	}

	workers := make([]WorkerPodStatus, d.workerCount)
	for w := range d.workerCount {
		workerID := fmt.Sprintf("W%d", w+1)
		podSlots := make([]WorkerSlotStatus, d.slotsPerWorker)
		activeOnPod := 0
		for s := range d.slotsPerWorker {
			slot := d.slots[w*d.slotsPerWorker+s]
			podSlots[s] = slot
			if slot.Busy {
				activeOnPod++
			}
		}
		workers[w] = WorkerPodStatus{
			WorkerID: workerID,
			Capacity: d.slotsPerWorker,
			Active:   activeOnPod,
			Slots:    podSlots,
		}
	}

	eventsNewestFirst := make([]TurnEvent, len(d.events))
	for i := range d.events {
		eventsNewestFirst[i] = d.events[len(d.events)-1-i]
	}

	return FleetStateResponse{
		Mode:         d.mode,
		Atespace:     d.atespace,
		AuthRequired: d.auth.isEnabled(),
		Summary: FleetSummary{
			TotalTenants:    len(summaries),
			ReadyTenants:    readyCount,
			ActiveActors:    activeActors,
			SuspendedActors: suspendedActors,
			CrashedActors:   crashedActors,
			QueuedTurns:     len(d.fifoQueue),
			WorkerCapacity:      d.workerCount * d.slotsPerWorker,
			ActiveSlots:         d.activeSlots,
			PeakActiveSlots:     d.peakActiveSlots,
			TotalTurns:          d.totalTurns,
			SnapshotCount:       d.totalSnapshots,
			MedianFetchMs:       medianInt64(d.fetchSamples),
			MedianCompileTestMs: medianInt64(d.compileSamples),
			MedianRenderMs:      medianInt64(d.renderSamples),
			MedianApplyMs:       medianInt64(d.applySamples),
			MedianTurnMs:        medianInt64(d.turnSamples),
			DemoRepository:      "fulfillment-api",
			SharedRepoURL:       "http://git-oci.demo-repo.svc.cluster.local/repo/fulfillment-api.git",
			SecretPosture:       "ZERO_AMBIENT_SECRETS",
		},
		WorkerPool: WorkerPoolStatus{
			Workers: workers,
			Queue:   slices.Clone(d.fifoQueue),
		},
		Tenants: summaries,
		Events:  eventsNewestFirst,
	}
}

// SnapshotTenant returns full Tenant Inspector state for tenantID.
func (d *Dispatcher) SnapshotTenant(tenantID string) (TenantDetail, bool) {
	d.schedMu.Lock()
	defer d.schedMu.Unlock()

	ts, exists := d.tenants[tenantID]
	if !exists {
		return TenantDetail{}, false
	}

	qPos := 0
	for i, id := range d.fifoQueue {
		if id == tenantID {
			qPos = i + 1
			break
		}
	}

	var policyCopy *EgressPolicySpec
	if ts.lastPolicy != nil {
		cp := *ts.lastPolicy
		policyCopy = &cp
	}

	recentReversed := make([]TurnEvent, len(ts.recentTurns))
	for i := range ts.recentTurns {
		recentReversed[i] = ts.recentTurns[len(ts.recentTurns)-1-i]
	}

	return TenantDetail{
		TenantSummary:       buildTenantSummaryLocked(ts, qPos),
		LatestDiff:          ts.latestDiff,
		EgressPolicy:        policyCopy,
		Inventory:           slices.Clone(ts.inventory),
		PrunedObjects:       slices.Clone(ts.prunedObjects),
		Conditions:          slices.Clone(ts.conditions),
		GitRepositoryStatus: ts.lastGRStatus,
		KustomizationStatus: ts.lastKSStatus,
		KustomizationYAML:   ts.lastKSYAML,
		RecentTurns:         recentReversed,
	}, true
}

func buildTenantSummaryLocked(ts *tenantState, queuePos int) TenantSummary {
	workerID := ts.lastWorkerID
	slotID := ts.lastSlotID
	if ts.activeSlot != nil {
		workerID = ts.activeSlot.WorkerID
		slotID = ts.activeSlot.SlotID
	}
	return TenantSummary{
		TenantID:              ts.tenantID,
		ActorName:             ts.actorName,
		Namespace:             ts.ksKey.Namespace,
		TargetNamespace:       ts.targetNamespace,
		KustomizationName:     ts.ksKey.Name,
		GitRepositoryName:     ts.grKey.Name,
		RepoURL:               ts.repoURL,
		Branch:                ts.branch,
		ActorState:            ts.actorState,
		Queued:                ts.queued,
		QueuePosition:         queuePos,
		WorkerID:              workerID,
		SlotID:                slotID,
		Ready:                 ts.ready,
		Reconciling:           ts.reconciling,
		Stalled:               ts.stalled,
		LastAppliedRevision:   ts.lastAppliedRev,
		LastAttemptedRevision: ts.lastAttemptedRev,
		CommitSHA:             ts.commitSHA,
		LastCommitMessage:     ts.lastCommitMessage,
		CacheHit:              ts.cacheHit,
		BuildCacheHit:         ts.buildCacheHit,
		SecretPosture:         ts.secretPosture,
		TurnCount:             ts.turnCount,
		CoalescedTriggers:     ts.coalescedTriggers,
		SnapshotCount:         ts.snapshotCount,
		ResourceCount:         len(ts.inventory),
		Timings:               ts.timings,
		StatusMessage:         ts.statusMessage,
		LastError:             ts.lastError,
		FailedStage:           ts.failedStage,
		UpdatedAt:             ts.updatedAt,
	}
}

func resolveTenantID(ks *kustomizev1.Kustomization) string {
	if ks.Labels != nil {
		if t := strings.TrimSpace(ks.Labels[TenantLabelKey]); t != "" {
			return t
		}
	}
	if t := strings.TrimSpace(ks.Spec.TargetNamespace); t != "" {
		return t
	}
	return strings.TrimSpace(ks.Name)
}
