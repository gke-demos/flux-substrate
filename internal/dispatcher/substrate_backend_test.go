package dispatcher_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/gke-demos/flux-substrate/internal/actor"
	ateapi "github.com/gke-demos/flux-substrate/internal/ateapi/v1"
	"github.com/gke-demos/flux-substrate/internal/dispatcher"
)

type fakeSubstrateControlServer struct {
	ateapi.UnimplementedControlServer

	mu            sync.Mutex
	atespaces     map[string]*ateapi.Atespace
	templates     map[string]*ateapi.ActorTemplate
	actors        map[string]*ateapi.Actor
	egressByActor map[string]*ateapi.EgressPolicy
	resumeCalls   []string
	suspendCalls  []string
	revertCalls   []string
	lastSuspendMd ateapi.DurabilityMode
}

func newFakeSubstrateControlServer() *fakeSubstrateControlServer {
	return &fakeSubstrateControlServer{
		atespaces:     make(map[string]*ateapi.Atespace),
		templates:     make(map[string]*ateapi.ActorTemplate),
		actors:        make(map[string]*ateapi.Actor),
		egressByActor: make(map[string]*ateapi.EgressPolicy),
	}
}

func (s *fakeSubstrateControlServer) GetAtespace(_ context.Context, req *ateapi.GetAtespaceRequest) (*ateapi.Atespace, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	name := req.GetAtespace().GetName()
	as, ok := s.atespaces[name]
	if !ok {
		return nil, status.Error(codes.NotFound, "atespace not found")
	}
	return as, nil
}

func (s *fakeSubstrateControlServer) CreateAtespace(_ context.Context, req *ateapi.CreateAtespaceRequest) (*ateapi.Atespace, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	name := req.GetAtespace().GetMetadata().GetName()
	s.atespaces[name] = req.GetAtespace()
	return req.GetAtespace(), nil
}

func (s *fakeSubstrateControlServer) GetActorTemplate(_ context.Context, req *ateapi.GetActorTemplateRequest) (*ateapi.ActorTemplate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := req.GetActorTemplate().GetAtespace() + "/" + req.GetActorTemplate().GetName()
	tpl, ok := s.templates[key]
	if !ok {
		return nil, status.Error(codes.NotFound, "template not found")
	}
	return tpl, nil
}

func (s *fakeSubstrateControlServer) CreateActorTemplate(_ context.Context, req *ateapi.CreateActorTemplateRequest) (*ateapi.ActorTemplate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tpl := req.GetActorTemplate()
	key := tpl.GetMetadata().GetAtespace() + "/" + tpl.GetMetadata().GetName()
	s.templates[key] = tpl
	return tpl, nil
}

func (s *fakeSubstrateControlServer) GetActor(_ context.Context, req *ateapi.GetActorRequest) (*ateapi.Actor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := req.GetActor().GetAtespace() + "/" + req.GetActor().GetName()
	act, ok := s.actors[key]
	if !ok {
		return nil, status.Error(codes.NotFound, "actor not found")
	}
	return act, nil
}

func (s *fakeSubstrateControlServer) CreateActor(_ context.Context, req *ateapi.CreateActorRequest) (*ateapi.Actor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	act := req.GetActor()
	if act.Status == nil {
		act.Status = &ateapi.ActorStatus{
			State: ateapi.ActorState_ACTOR_STATE_SUSPENDED,
		}
	}
	key := act.GetMetadata().GetAtespace() + "/" + act.GetMetadata().GetName()
	s.actors[key] = act
	return act, nil
}

func (s *fakeSubstrateControlServer) GetActorEgressPolicy(_ context.Context, req *ateapi.GetActorEgressPolicyRequest) (*ateapi.EgressPolicy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := req.GetActor().GetAtespace() + "/" + req.GetActor().GetName()
	pol, ok := s.egressByActor[key]
	if !ok {
		return nil, status.Error(codes.NotFound, "egress policy not found")
	}
	return pol, nil
}

func (s *fakeSubstrateControlServer) CreateActorEgressPolicy(_ context.Context, req *ateapi.CreateActorEgressPolicyRequest) (*ateapi.EgressPolicy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := req.GetActor().GetAtespace() + "/" + req.GetActor().GetName()
	pol := req.GetEgressPolicy()
	s.egressByActor[key] = pol
	return pol, nil
}

func (s *fakeSubstrateControlServer) UpdateActorEgressPolicy(_ context.Context, req *ateapi.UpdateActorEgressPolicyRequest) (*ateapi.EgressPolicy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := req.GetActor().GetAtespace() + "/" + req.GetActor().GetName()
	pol := req.GetEgressPolicy()
	s.egressByActor[key] = pol
	return pol, nil
}

func (s *fakeSubstrateControlServer) ResumeActor(_ context.Context, req *ateapi.ResumeActorRequest) (*ateapi.ResumeActorResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := req.GetActor().GetAtespace() + "/" + req.GetActor().GetName()
	s.resumeCalls = append(s.resumeCalls, key)
	act := s.actors[key]
	if act != nil {
		act.Status = &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_RUNNING}
	}
	return &ateapi.ResumeActorResponse{Actor: act}, nil
}

func (s *fakeSubstrateControlServer) SuspendActor(_ context.Context, req *ateapi.SuspendActorRequest) (*ateapi.SuspendActorResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := req.GetActor().GetAtespace() + "/" + req.GetActor().GetName()
	s.suspendCalls = append(s.suspendCalls, key)
	s.lastSuspendMd = req.GetOptions().GetMode()
	act := s.actors[key]
	if act != nil {
		act.Status = &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED}
	}
	return &ateapi.SuspendActorResponse{Actor: act}, nil
}

func (s *fakeSubstrateControlServer) RevertActor(_ context.Context, req *ateapi.RevertActorRequest) (*ateapi.RevertActorResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := req.GetActor().GetAtespace() + "/" + req.GetActor().GetName()
	s.revertCalls = append(s.revertCalls, key)
	act := s.actors[key]
	if act != nil {
		act.Status = &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED}
	}
	return &ateapi.RevertActorResponse{Actor: act}, nil
}

func TestSubstrateActorBackendEndToEndLifecycle(t *testing.T) {
	ctx := context.Background()
	controlSrv := newFakeSubstrateControlServer()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen gRPC: %v", err)
	}
	grpcServer := grpc.NewServer()
	ateapi.RegisterControlServer(grpcServer, controlSrv)
	go func() {
		_ = grpcServer.Serve(lis)
	}()
	t.Cleanup(grpcServer.Stop)

	var (
		routerMu         sync.Mutex
		lastTargetHeader string
		forceOOMCrash    bool
	)
	routerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		routerMu.Lock()
		lastTargetHeader = r.Header.Get("ate-target-actor")
		shouldCrash := forceOOMCrash
		routerMu.Unlock()

		var req actor.ReconcileRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if shouldCrash {
			controlSrv.mu.Lock()
			if act, ok := controlSrv.actors["flux-substrate/"+req.TenantID]; ok {
				act.Status = &ateapi.ActorStatus{
					State: ateapi.ActorState_ACTOR_STATE_CRASHED,
					Crash: &ateapi.ActorCrash{
						Message: "sandbox memory limit 512MiB exceeded during Kustomize evaluation",
					},
				}
			}
			controlSrv.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInsufficientStorage)
			_ = json.NewEncoder(w).Encode(actor.ReconcileResult{
				TenantID:    req.TenantID,
				FailedStage: "render",
				Error:       "sandbox memory limit 512MiB exceeded",
			})
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(actor.ReconcileResult{
			TenantID:  req.TenantID,
			Revision:  "main@sha1:0123456789abcdef0123456789abcdef01234567",
			CommitSHA: "0123456789abcdef0123456789abcdef01234567",
			CacheHit:  true,
			Ready:     true,
			Timings: actor.PhaseTimings{
				FetchMs:  4,
				RenderMs: 3,
				ApplyMs:  6,
				TotalMs:  13,
			},
		})
	}))
	t.Cleanup(routerSrv.Close)

	backend, err := dispatcher.NewSubstrateActorBackend(dispatcher.SubstrateBackendConfig{
		Atespace:        "flux-substrate",
		TemplateName:    "flux-reconciler-v1",
		ActorImage:      "ghcr.io/gke-demos/flux-substrate@sha256:1111111111111111111111111111111111111111111111111111111111111111",
		KubeBridgeURL:   "https://kube-bridge.flux-substrate.svc",
		ControlAddr:     lis.Addr().String(),
		ControlInsecure: true,
		RouterURL:       routerSrv.URL,
	})
	if err != nil {
		t.Fatalf("NewSubstrateActorBackend: %v", err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	// 1. EnsureActor creates Atespace, SNAPSHOT_FIDELITY_VOLUMES ActorTemplate, and Actor.
	if err := backend.EnsureActor(ctx, dispatcher.ActorSpec{
		Atespace:  "flux-substrate",
		ActorName: "tenant-01",
		TenantID:  "tenant-01",
	}); err != nil {
		t.Fatalf("EnsureActor: %v", err)
	}

	controlSrv.mu.Lock()
	tpl := controlSrv.templates["flux-substrate/flux-reconciler-v1"]
	act := controlSrv.actors["flux-substrate/tenant-01"]
	controlSrv.mu.Unlock()

	if tpl == nil {
		t.Fatal("expected ActorTemplate flux-substrate/flux-reconciler-v1 to be created")
	}
	if got := tpl.GetSnapshotConfig().GetPreferredFidelity(); got != ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES {
		t.Fatalf("ActorTemplate SnapshotFidelity = %v, want SNAPSHOT_FIDELITY_VOLUMES", got)
	}
	if got := tpl.GetWorkerSelector().GetMatchLabels()[dispatcher.DefaultWorkerRoleLabelKey]; got != dispatcher.DefaultWorkerRoleLabelValue {
		t.Fatalf("ActorTemplate WorkerSelector role = %q, want %q", got, dispatcher.DefaultWorkerRoleLabelValue)
	}
	if len(tpl.GetVolumes()) != 2 {
		t.Fatalf("ActorTemplate volumes len = %d, want 2 (flux-cache DurableDir + ate-system-info)", len(tpl.GetVolumes()))
	}
	if len(tpl.GetContainers()) != 1 || tpl.GetContainers()[0].GetWakeupProbe().GetHttpGet().GetPath() != "/readyz" {
		t.Fatalf("ActorTemplate container WakeupProbe invalid: %+v", tpl.GetContainers())
	}
	if act == nil || act.GetActorTemplate().GetName() != "flux-reconciler-v1" {
		t.Fatalf("Actor tenant-01 not linked to flux-reconciler-v1: %+v", act)
	}

	// 2. EnsureEgressPolicy creates singleton "default" policy with ate-secret:// and actorJwt rules,
	// and updates it when the tenant's GitRepository switches to an external GitHub repo.
	if err := backend.EnsureEgressPolicy(ctx, dispatcher.EgressPolicySpec{
		Atespace:             "flux-substrate",
		ActorName:            "tenant-01",
		GitScheme:            "https",
		GitHost:              "github.com",
		GitCredentialURI:     "ate-secret://k8s.io/default/flux-substrate/github-pat/token",
		KubeBridgeHost:       "kube-bridge.flux-substrate.svc:443",
		KubeBridgeAudience:   "https://kube-bridge.flux-substrate.svc",
		ActorJWTLifetimeSecs: 600,
	}); err != nil {
		t.Fatalf("EnsureEgressPolicy (create): %v", err)
	}

	// Second call exercises UpdateActorEgressPolicy.
	if err := backend.EnsureEgressPolicy(ctx, dispatcher.EgressPolicySpec{
		Atespace:             "flux-substrate",
		ActorName:            "tenant-01",
		GitScheme:            "https",
		GitHost:              "github.com",
		GitCredentialURI:     "ate-secret://k8s.io/default/flux-substrate/github-pat-v2/token",
		KubeBridgeHost:       "kube-bridge.flux-substrate.svc:443",
		KubeBridgeAudience:   "https://kube-bridge.flux-substrate.svc",
		ActorJWTLifetimeSecs: 600,
	}); err != nil {
		t.Fatalf("EnsureEgressPolicy (update): %v", err)
	}

	controlSrv.mu.Lock()
	pol := controlSrv.egressByActor["flux-substrate/tenant-01"]
	controlSrv.mu.Unlock()
	if pol == nil || len(pol.GetRules()) != 2 {
		t.Fatalf("expected 2 EgressRules (Git HTTPS + Identity Bridge), got %+v", pol)
	}
	gitHeader := pol.GetRules()[0].GetHttps().GetEffects().GetReplaceHeaders()[0]
	if gitHeader.GetCredentialUri() != "ate-secret://k8s.io/default/flux-substrate/github-pat-v2/token" {
		t.Fatalf("Git replaceHeader CredentialUri = %q", gitHeader.GetCredentialUri())
	}
	bridgeHeader := pol.GetRules()[1].GetHttps().GetEffects().GetReplaceHeaders()[0]
	if got := bridgeHeader.GetActorJwt().GetAudiences(); len(got) != 1 || got[0] != "https://kube-bridge.flux-substrate.svc" {
		t.Fatalf("Identity Bridge actorJwt audiences = %v", got)
	}

	// 3. InvokeReconcile forwards POST /reconcile with ate-target-actor header.
	res, code, err := backend.InvokeReconcile(ctx, dispatcher.TurnInvocation{
		Atespace:  "flux-substrate",
		ActorName: "tenant-01",
		WorkerID:  "W1",
		SlotID:    "W1-1",
		Request: actor.ReconcileRequest{
			TenantID:        "tenant-01",
			WorkflowName:    "tenant-01",
			RepoURL:         "https://github.com/gke-demos/podinfo.git",
			TargetNamespace: "tenant-01",
		},
	})
	if err != nil || code != http.StatusOK || !res.Ready || !res.CacheHit {
		t.Fatalf("InvokeReconcile warm turn failed: code=%d err=%v res=%+v", code, err, res)
	}
	routerMu.Lock()
	gotHeader := lastTargetHeader
	routerMu.Unlock()
	if gotHeader != "flux-substrate/tenant-01" {
		t.Fatalf("ate-target-actor header = %q, want %q", gotHeader, "flux-substrate/tenant-01")
	}

	// 4. SuspendActor checkpoints the actor with DURABILITY_MODE_ASYNCHRONOUS.
	if err := backend.SuspendActor(ctx, "flux-substrate", "tenant-01"); err != nil {
		t.Fatalf("SuspendActor: %v", err)
	}
	controlSrv.mu.Lock()
	if len(controlSrv.suspendCalls) != 1 || controlSrv.lastSuspendMd != ateapi.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS {
		t.Fatalf("unexpected SuspendActor calls=%v mode=%v", controlSrv.suspendCalls, controlSrv.lastSuspendMd)
	}
	controlSrv.mu.Unlock()

	// 5. Simulate sandbox OOM crash -> returns *dispatcher.ActorCrashError, followed by RevertActor.
	routerMu.Lock()
	forceOOMCrash = true
	routerMu.Unlock()

	_, crashCode, crashErr := backend.InvokeReconcile(ctx, dispatcher.TurnInvocation{
		Atespace:  "flux-substrate",
		ActorName: "tenant-01",
		WorkerID:  "W1",
		SlotID:    "W1-1",
		Request: actor.ReconcileRequest{
			TenantID: "tenant-01",
		},
	})
	var typedCrash *dispatcher.ActorCrashError
	if !errors.As(crashErr, &typedCrash) || crashCode != http.StatusInsufficientStorage {
		t.Fatalf("expected *ActorCrashError and 507, got code=%d err=%v", crashCode, crashErr)
	}

	if err := backend.RevertActor(ctx, "flux-substrate", "tenant-01"); err != nil {
		t.Fatalf("RevertActor: %v", err)
	}
	controlSrv.mu.Lock()
	if len(controlSrv.revertCalls) != 1 || controlSrv.revertCalls[0] != "flux-substrate/tenant-01" {
		t.Fatalf("unexpected RevertActor calls: %v", controlSrv.revertCalls)
	}
	controlSrv.mu.Unlock()
}
