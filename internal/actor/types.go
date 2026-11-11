package actor

// InventoryEntry records an applied Kubernetes object in Flux ResourceInventory format
// alongside its evaluated kstatus readiness.
type InventoryEntry struct {
	ID      string `json:"id"`
	Version string `json:"v"`
	Status  string `json:"status,omitempty"`
	Message string `json:"message,omitempty"`
}

// PhaseTimings captures per-stage latencies (in milliseconds) for a single Reconcile Turn.
type PhaseTimings struct {
	FetchMs       int64 `json:"fetchMs"`
	CompileTestMs int64 `json:"compileTestMs"`
	RenderMs      int64 `json:"renderMs"`
	ApplyMs       int64 `json:"applyMs"`
	TotalMs       int64 `json:"totalMs"`
}

// ReconcileRequest is the payload sent to POST /reconcile to execute one Reconcile Turn.
type ReconcileRequest struct {
	TenantID          string           `json:"tenantId,omitempty"`
	WorkflowName      string           `json:"workflowName,omitempty"`
	RepoURL           string           `json:"repoUrl"`
	Branch            string           `json:"branch,omitempty"`
	Path              string           `json:"path,omitempty"`
	TargetNamespace   string           `json:"targetNamespace"`
	Prune             *bool            `json:"prune,omitempty"`
	PreviousInventory []InventoryEntry `json:"previousInventory,omitempty"`
	OCIRegistryURL    string           `json:"ociRegistryUrl,omitempty"`
	OCIRepository     string           `json:"ociRepository,omitempty"`
}

// ReconcileResult is the structured turn telemetry returned from POST /reconcile.
type ReconcileResult struct {
	ActorName             string            `json:"actorName"`
	Atespace              string            `json:"atespace,omitempty"`
	TenantID              string            `json:"tenantId,omitempty"`
	TurnNumber            int64             `json:"turnNumber"`
	Revision              string            `json:"revision,omitempty"`
	CommitSHA             string            `json:"commitSha,omitempty"`
	ShortSHA              string            `json:"shortSha,omitempty"`
	Branch                string            `json:"branch,omitempty"`
	CacheHit              bool              `json:"cacheHit"`
	BuildCacheHit         bool              `json:"buildCacheHit"`
	SecretPosture         string            `json:"secretPosture,omitempty"`
	PromoBanner           string            `json:"promoBanner,omitempty"`
	EnterpriseDiscountBps int               `json:"enterpriseDiscountBps,omitempty"`
	OCIDigest             string            `json:"ociDigest,omitempty"`
	OCITag                string            `json:"ociTag,omitempty"`
	AppliedActions        map[string]string `json:"appliedActions,omitempty"`
	PrunedObjects         []string          `json:"prunedObjects"`
	Inventory             []InventoryEntry  `json:"inventory"`
	Ready                 bool              `json:"ready"`
	StatusMessage         string            `json:"statusMessage"`
	Timings               PhaseTimings      `json:"timings"`
	FetchMs               int64             `json:"fetchMs"`
	CompileTestMs         int64             `json:"compileTestMs"`
	RenderMs              int64             `json:"renderMs"`
	ApplyMs               int64             `json:"applyMs"`
	Error                 string            `json:"error,omitempty"`
	FailedStage           string            `json:"failedStage,omitempty"`
}
