package dispatcher

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/yaml"
)

type resourceDef struct {
	Group      string
	Version    string
	Resource   string
	Kind       string
	Namespaced bool
	HasStatus  bool
}

var localResourceCatalog = []resourceDef{
	{Group: "", Version: "v1", Resource: "namespaces", Kind: "Namespace", Namespaced: false, HasStatus: true},
	{Group: "", Version: "v1", Resource: "configmaps", Kind: "ConfigMap", Namespaced: true, HasStatus: false},
	{Group: "", Version: "v1", Resource: "services", Kind: "Service", Namespaced: true, HasStatus: true},
	{Group: "", Version: "v1", Resource: "secrets", Kind: "Secret", Namespaced: true, HasStatus: false},
	{Group: "", Version: "v1", Resource: "events", Kind: "Event", Namespaced: true, HasStatus: false},
	{Group: "apps", Version: "v1", Resource: "deployments", Kind: "Deployment", Namespaced: true, HasStatus: true},
	{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles", Kind: "Role", Namespaced: true, HasStatus: false},
	{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings", Kind: "RoleBinding", Namespaced: true, HasStatus: false},
	{Group: "source.toolkit.fluxcd.io", Version: "v1", Resource: "gitrepositories", Kind: "GitRepository", Namespaced: true, HasStatus: true},
	{Group: "kustomize.toolkit.fluxcd.io", Version: "v1", Resource: "kustomizations", Kind: "Kustomization", Namespaced: true, HasStatus: true},
}

// LocalKubeServer provides an in-process loopback (127.0.0.1) Kubernetes REST API
// target store supporting discovery, CRUD, Server-Side Apply (dry-run & live),
// status subresources, and Deployment kstatus readiness for standalone --mode=local.
type LocalKubeServer struct {
	ln      net.Listener
	httpSrv *http.Server
	baseURL string

	rvSeq atomic.Int64
	mu    sync.RWMutex
	store map[string]*unstructured.Unstructured
}

// NewLocalKubeServer starts a loopback-only (127.0.0.1:0) in-memory Kubernetes API server.
func NewLocalKubeServer() (*LocalKubeServer, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("listen 127.0.0.1:0 for local kube target store: %w", err)
	}

	s := &LocalKubeServer{
		ln:      ln,
		baseURL: "http://" + ln.Addr().String(),
		store:   make(map[string]*unstructured.Unstructured),
	}
	s.rvSeq.Store(100)

	s.httpSrv = &http.Server{
		Handler:           http.HandlerFunc(s.serveHTTP),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		_ = s.httpSrv.Serve(ln)
	}()

	// Pre-seed default namespace.
	s.seedNamespace("default")
	return s, nil
}

// RestConfig returns a client-go *rest.Config targeting this loopback server using JSON wire encoding.
func (s *LocalKubeServer) RestConfig() *rest.Config {
	return &rest.Config{
		Host: s.baseURL,
		ContentConfig: rest.ContentConfig{
			ContentType:        "application/json",
			AcceptContentTypes: "application/json",
		},
		QPS:   200,
		Burst: 400,
	}
}

// Close shuts down the loopback HTTP listener.
func (s *LocalKubeServer) Close() {
	_ = s.httpSrv.Close()
	_ = s.ln.Close()
}

func (s *LocalKubeServer) seedNamespace(name string) {
	rv := strconv.FormatInt(s.rvSeq.Add(1), 10)
	now := time.Now().UTC().Format(time.RFC3339)
	u := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Namespace",
			"metadata": map[string]any{
				"name":              name,
				"uid":               randomUID(),
				"resourceVersion":   rv,
				"generation":        int64(1),
				"creationTimestamp": now,
			},
			"status": map[string]any{"phase": "Active"},
		},
	}
	s.mu.Lock()
	s.store[storeKey("", "v1", "namespaces", "", name)] = u
	s.mu.Unlock()
}

func (s *LocalKubeServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimRight(r.URL.Path, "/")
	if path == "" {
		path = "/"
	}

	switch path {
	case "/api":
		writeKubeJSON(w, http.StatusOK, metav1.APIVersions{
			TypeMeta: metav1.TypeMeta{Kind: "APIVersions", APIVersion: "v1"},
			Versions: []string{"v1"},
		})
		return
	case "/api/v1":
		writeKubeJSON(w, http.StatusOK, buildAPIResourceList("", "v1"))
		return
	case "/apis":
		writeKubeJSON(w, http.StatusOK, buildAPIGroupList())
		return
	case "/apis/apps/v1":
		writeKubeJSON(w, http.StatusOK, buildAPIResourceList("apps", "v1"))
		return
	case "/apis/rbac.authorization.k8s.io/v1":
		writeKubeJSON(w, http.StatusOK, buildAPIResourceList("rbac.authorization.k8s.io", "v1"))
		return
	case "/apis/source.toolkit.fluxcd.io/v1":
		writeKubeJSON(w, http.StatusOK, buildAPIResourceList("source.toolkit.fluxcd.io", "v1"))
		return
	case "/apis/kustomize.toolkit.fluxcd.io/v1":
		writeKubeJSON(w, http.StatusOK, buildAPIResourceList("kustomize.toolkit.fluxcd.io", "v1"))
		return
	}

	parsed, ok := parseResourceRoute(path)
	if !ok {
		writeKubeStatus(w, http.StatusNotFound, metav1.StatusReasonNotFound, fmt.Sprintf("unknown API path %s", path), "", "")
		return
	}

	if deniedMsg := checkImpersonatedNamespaceRBAC(r, parsed); deniedMsg != "" {
		writeKubeStatus(w, http.StatusForbidden, metav1.StatusReasonForbidden, deniedMsg, parsed.def.Kind, parsed.name)
		return
	}

	isDryRun := r.URL.Query().Get("dryRun") == "All"

	switch r.Method {
	case http.MethodGet:
		if parsed.name == "" {
			s.handleList(w, r, parsed)
			return
		}
		s.handleGet(w, parsed)
	case http.MethodPost:
		s.handleCreate(w, r, parsed, isDryRun)
	case http.MethodPut:
		s.handlePut(w, r, parsed, isDryRun)
	case http.MethodPatch:
		s.handlePatch(w, r, parsed, isDryRun)
	case http.MethodDelete:
		s.handleDelete(w, parsed, isDryRun)
	default:
		writeKubeStatus(w, http.StatusMethodNotAllowed, metav1.StatusReasonMethodNotAllowed, "method not allowed", parsed.def.Kind, parsed.name)
	}
}

type parsedRoute struct {
	def          resourceDef
	namespace    string
	name         string
	isStatusSubr bool
}

func parseResourceRoute(cleanPath string) (parsedRoute, bool) {
	segs := strings.Split(strings.TrimPrefix(cleanPath, "/"), "/")
	var group, version string
	var restSegs []string

	switch {
	case len(segs) >= 3 && segs[0] == "api" && segs[1] == "v1":
		group = ""
		version = "v1"
		restSegs = segs[2:]
	case len(segs) >= 4 && segs[0] == "apis":
		group = segs[1]
		version = segs[2]
		restSegs = segs[3:]
	default:
		return parsedRoute{}, false
	}

	var ns, resName, objName string
	var isStatus bool

	if len(restSegs) >= 2 && restSegs[0] == "namespaces" && (len(restSegs) > 2 || group != "" || restSegs[0] != "namespaces") {
		if len(restSegs) == 2 && group == "" {
			// /api/v1/namespaces/{name}
			resName = "namespaces"
			objName = restSegs[1]
		} else if len(restSegs) == 3 && group == "" && restSegs[2] == "status" {
			resName = "namespaces"
			objName = restSegs[1]
			isStatus = true
		} else if len(restSegs) >= 3 {
			ns = restSegs[1]
			resName = restSegs[2]
			if len(restSegs) >= 4 {
				objName = restSegs[3]
			}
			if len(restSegs) >= 5 && restSegs[4] == "status" {
				isStatus = true
			}
		}
	} else {
		resName = restSegs[0]
		if len(restSegs) >= 2 {
			objName = restSegs[1]
		}
		if len(restSegs) >= 3 && restSegs[2] == "status" {
			isStatus = true
		}
	}

	for _, def := range localResourceCatalog {
		if def.Group == group && def.Version == version && def.Resource == resName {
			return parsedRoute{
				def:          def,
				namespace:    ns,
				name:         objName,
				isStatusSubr: isStatus,
			}, true
		}
	}
	return parsedRoute{}, false
}

func checkImpersonatedNamespaceRBAC(r *http.Request, pr parsedRoute) string {
	impUser := strings.TrimSpace(r.Header.Get("Impersonate-User"))
	if !strings.HasPrefix(impUser, "ate:actor:") {
		return ""
	}
	parts := strings.Split(impUser, ":")
	if len(parts) < 4 {
		return "invalid actor impersonation identity"
	}
	actorTenant := parts[3]
	if !pr.def.Namespaced {
		if r.Method != http.MethodGet {
			return fmt.Sprintf("User %q cannot mutate cluster-scoped %s", impUser, pr.def.Resource)
		}
		return ""
	}
	if pr.namespace != "" && pr.namespace != actorTenant {
		return fmt.Sprintf("%s %q is forbidden: User %q cannot access resource %q in namespace %q",
			pr.def.Resource, pr.name, impUser, pr.def.Resource, pr.namespace)
	}
	return ""
}

func (s *LocalKubeServer) handleGet(w http.ResponseWriter, pr parsedRoute) {
	key := storeKey(pr.def.Group, pr.def.Version, pr.def.Resource, pr.namespace, pr.name)
	s.mu.RLock()
	existing, ok := s.store[key]
	var out *unstructured.Unstructured
	if ok {
		out = existing.DeepCopy()
	}
	s.mu.RUnlock()

	if !ok {
		writeKubeStatus(w, http.StatusNotFound, metav1.StatusReasonNotFound,
			fmt.Sprintf("%s %q not found", pr.def.Resource, pr.name), pr.def.Kind, pr.name)
		return
	}
	writeKubeJSON(w, http.StatusOK, out.Object)
}

func (s *LocalKubeServer) handleList(w http.ResponseWriter, r *http.Request, pr parsedRoute) {
	sel := labels.Everything()
	if rawSel := strings.TrimSpace(r.URL.Query().Get("labelSelector")); rawSel != "" {
		parsedSel, err := labels.Parse(rawSel)
		if err != nil {
			writeKubeStatus(w, http.StatusBadRequest, metav1.StatusReasonBadRequest, err.Error(), pr.def.Kind, "")
			return
		}
		sel = parsedSel
	}

	prefix := fmt.Sprintf("%s/%s/%s/", pr.def.Group, pr.def.Version, pr.def.Resource)
	s.mu.RLock()
	items := make([]map[string]any, 0)
	for k, obj := range s.store {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		if pr.namespace != "" && obj.GetNamespace() != pr.namespace {
			continue
		}
		if !sel.Matches(labels.Set(obj.GetLabels())) {
			continue
		}
		items = append(items, obj.DeepCopy().Object)
	}
	rv := strconv.FormatInt(s.rvSeq.Load(), 10)
	s.mu.RUnlock()

	slices.SortFunc(items, func(a, b map[string]any) int {
		am, _ := a["metadata"].(map[string]any)
		bm, _ := b["metadata"].(map[string]any)
		ans, _ := am["namespace"].(string)
		bns, _ := bm["namespace"].(string)
		if c := strings.Compare(ans, bns); c != 0 {
			return c
		}
		an, _ := am["name"].(string)
		bn, _ := bm["name"].(string)
		return strings.Compare(an, bn)
	})

	apiVer := pr.def.Version
	if pr.def.Group != "" {
		apiVer = pr.def.Group + "/" + pr.def.Version
	}
	writeKubeJSON(w, http.StatusOK, map[string]any{
		"apiVersion": apiVer,
		"kind":       pr.def.Kind + "List",
		"metadata":   map[string]any{"resourceVersion": rv},
		"items":      items,
	})
}

func (s *LocalKubeServer) handleCreate(w http.ResponseWriter, r *http.Request, pr parsedRoute, isDryRun bool) {
	incoming, err := decodeUnstructuredBody(r.Body)
	if err != nil {
		writeKubeStatus(w, http.StatusBadRequest, metav1.StatusReasonBadRequest, err.Error(), pr.def.Kind, "")
		return
	}
	normalizeTypeAndMeta(incoming, pr, r.URL.Query().Get("fieldManager"))

	name := incoming.GetName()
	if name == "" {
		if gen := incoming.GetGenerateName(); gen != "" {
			name = gen + randomShortSuffix()
			incoming.SetName(name)
		}
	}
	if name == "" {
		writeKubeStatus(w, http.StatusBadRequest, metav1.StatusReasonBadRequest, "metadata.name is required", pr.def.Kind, "")
		return
	}

	key := storeKey(pr.def.Group, pr.def.Version, pr.def.Resource, incoming.GetNamespace(), name)
	s.mu.Lock()
	if _, exists := s.store[key]; exists {
		s.mu.Unlock()
		writeKubeStatus(w, http.StatusConflict, metav1.StatusReasonAlreadyExists,
			fmt.Sprintf("%s %q already exists", pr.def.Resource, name), pr.def.Kind, name)
		return
	}
	finalizeNewObject(incoming, strconv.FormatInt(s.rvSeq.Add(1), 10), pr.def)
	if !isDryRun {
		s.store[key] = incoming.DeepCopy()
	}
	s.mu.Unlock()

	writeKubeJSON(w, http.StatusCreated, incoming.Object)
}

func (s *LocalKubeServer) handlePut(w http.ResponseWriter, r *http.Request, pr parsedRoute, isDryRun bool) {
	incoming, err := decodeUnstructuredBody(r.Body)
	if err != nil {
		writeKubeStatus(w, http.StatusBadRequest, metav1.StatusReasonBadRequest, err.Error(), pr.def.Kind, pr.name)
		return
	}
	key := storeKey(pr.def.Group, pr.def.Version, pr.def.Resource, pr.namespace, pr.name)

	s.mu.Lock()
	existing, exists := s.store[key]
	if !exists {
		s.mu.Unlock()
		writeKubeStatus(w, http.StatusNotFound, metav1.StatusReasonNotFound,
			fmt.Sprintf("%s %q not found", pr.def.Resource, pr.name), pr.def.Kind, pr.name)
		return
	}

	updated := existing.DeepCopy()
	if pr.isStatusSubr {
		if st, ok := incoming.Object["status"]; ok {
			updated.Object["status"] = deepCopyAny(st)
		} else {
			delete(updated.Object, "status")
		}
	} else {
		prevStatus := deepCopyAny(existing.Object["status"])
		prevUID := existing.GetUID()
		prevCreated := existing.GetCreationTimestamp()
		prevGen := existing.GetGeneration()

		updated = incoming.DeepCopy()
		normalizeTypeAndMeta(updated, pr, r.URL.Query().Get("fieldManager"))
		updated.SetUID(prevUID)
		updated.SetCreationTimestamp(prevCreated)
		updated.SetGeneration(prevGen + 1)
		if prevStatus != nil {
			updated.Object["status"] = prevStatus
		}
		synthesizeWorkloadStatus(updated, pr.def)
	}
	updated.SetResourceVersion(strconv.FormatInt(s.rvSeq.Add(1), 10))
	if !isDryRun {
		s.store[key] = updated.DeepCopy()
	}
	s.mu.Unlock()

	writeKubeJSON(w, http.StatusOK, updated.Object)
}

func (s *LocalKubeServer) handlePatch(w http.ResponseWriter, r *http.Request, pr parsedRoute, isDryRun bool) {
	incoming, err := decodeUnstructuredBody(r.Body)
	if err != nil {
		writeKubeStatus(w, http.StatusBadRequest, metav1.StatusReasonBadRequest, err.Error(), pr.def.Kind, pr.name)
		return
	}

	fieldMgr := r.URL.Query().Get("fieldManager")
	isApply := strings.Contains(strings.ToLower(r.Header.Get("Content-Type")), "apply-patch")
	key := storeKey(pr.def.Group, pr.def.Version, pr.def.Resource, pr.namespace, pr.name)

	s.mu.Lock()
	existing, exists := s.store[key]
	if !exists {
		if !isApply {
			s.mu.Unlock()
			writeKubeStatus(w, http.StatusNotFound, metav1.StatusReasonNotFound,
				fmt.Sprintf("%s %q not found", pr.def.Resource, pr.name), pr.def.Kind, pr.name)
			return
		}
		created := incoming.DeepCopy()
		normalizeTypeAndMeta(created, pr, fieldMgr)
		finalizeNewObject(created, strconv.FormatInt(s.rvSeq.Add(1), 10), pr.def)
		if !isDryRun {
			s.store[key] = created.DeepCopy()
		}
		s.mu.Unlock()
		writeKubeJSON(w, http.StatusCreated, created.Object)
		return
	}

	merged := existing.DeepCopy()
	if pr.isStatusSubr {
		if st, ok := incoming.Object["status"].(map[string]any); ok {
			curSt, _ := merged.Object["status"].(map[string]any)
			merged.Object["status"] = deepMergeMaps(curSt, st, false)
		}
	} else {
		prevUID := existing.GetUID()
		prevCreated := existing.GetCreationTimestamp()
		prevGen := existing.GetGeneration()
		prevStatus := deepCopyAny(existing.Object["status"])

		if isApply {
			// Server-Side Apply replaces top-level spec/data owned by the applier cleanly
			// while merging metadata labels/annotations.
			mergedMap := deepMergeMaps(merged.Object, incoming.Object, true)
			merged = &unstructured.Unstructured{Object: mergedMap}
		} else {
			mergedMap := deepMergeMaps(merged.Object, incoming.Object, false)
			merged = &unstructured.Unstructured{Object: mergedMap}
		}
		normalizeTypeAndMeta(merged, pr, fieldMgr)
		merged.SetUID(prevUID)
		merged.SetCreationTimestamp(prevCreated)
		merged.SetGeneration(prevGen + 1)
		if prevStatus != nil {
			merged.Object["status"] = prevStatus
		}
		synthesizeWorkloadStatus(merged, pr.def)
	}
	merged.SetResourceVersion(strconv.FormatInt(s.rvSeq.Add(1), 10))
	if !isDryRun {
		s.store[key] = merged.DeepCopy()
	}
	s.mu.Unlock()

	writeKubeJSON(w, http.StatusOK, merged.Object)
}

func (s *LocalKubeServer) handleDelete(w http.ResponseWriter, pr parsedRoute, isDryRun bool) {
	key := storeKey(pr.def.Group, pr.def.Version, pr.def.Resource, pr.namespace, pr.name)
	s.mu.Lock()
	existing, exists := s.store[key]
	var deleted *unstructured.Unstructured
	if exists {
		deleted = existing.DeepCopy()
		if !isDryRun {
			delete(s.store, key)
		}
	}
	s.mu.Unlock()

	if !exists {
		writeKubeStatus(w, http.StatusNotFound, metav1.StatusReasonNotFound,
			fmt.Sprintf("%s %q not found", pr.def.Resource, pr.name), pr.def.Kind, pr.name)
		return
	}
	writeKubeJSON(w, http.StatusOK, deleted.Object)
}

func decodeUnstructuredBody(r io.Reader) (*unstructured.Unstructured, error) {
	defer func() {
		if c, ok := r.(io.Closer); ok {
			_ = c.Close()
		}
	}()
	raw, err := io.ReadAll(io.LimitReader(r, 4<<20))
	if err != nil {
		return nil, err
	}
	var obj map[string]any
	if err := yaml.Unmarshal(raw, &obj); err != nil {
		return nil, err
	}
	if obj == nil {
		obj = make(map[string]any)
	}
	return &unstructured.Unstructured{Object: obj}, nil
}

func normalizeTypeAndMeta(u *unstructured.Unstructured, pr parsedRoute, fieldMgr string) {
	apiVer := pr.def.Version
	if pr.def.Group != "" {
		apiVer = pr.def.Group + "/" + pr.def.Version
	}
	u.SetAPIVersion(apiVer)
	u.SetKind(pr.def.Kind)
	if pr.name != "" {
		u.SetName(pr.name)
	}
	if pr.def.Namespaced {
		if pr.namespace != "" {
			u.SetNamespace(pr.namespace)
		} else if u.GetNamespace() == "" {
			u.SetNamespace("default")
		}
	} else {
		u.SetNamespace("")
	}
	if fieldMgr != "" {
		now := metav1.NewTime(time.Now().UTC())
		u.SetManagedFields([]metav1.ManagedFieldsEntry{
			{
				Manager:    fieldMgr,
				Operation:  metav1.ManagedFieldsOperationApply,
				APIVersion: apiVer,
				Time:       &now,
				FieldsType: "FieldsV1",
			},
		})
	}
}

func finalizeNewObject(u *unstructured.Unstructured, rv string, def resourceDef) {
	u.SetUID(types.UID(randomUID()))
	u.SetCreationTimestamp(metav1.NewTime(time.Now().UTC()))
	u.SetGeneration(1)
	u.SetResourceVersion(rv)
	synthesizeWorkloadStatus(u, def)
}

func synthesizeWorkloadStatus(u *unstructured.Unstructured, def resourceDef) {
	switch {
	case def.Group == "apps" && def.Kind == "Deployment":
		replicas := int64(1)
		if spec, ok := u.Object["spec"].(map[string]any); ok {
			switch r := spec["replicas"].(type) {
			case int64:
				replicas = r
			case float64:
				replicas = int64(r)
			case int:
				replicas = int64(r)
			}
		}
		now := time.Now().UTC().Format(time.RFC3339)
		u.Object["status"] = map[string]any{
			"observedGeneration": u.GetGeneration(),
			"replicas":           replicas,
			"updatedReplicas":    replicas,
			"readyReplicas":      replicas,
			"availableReplicas":  replicas,
			"conditions": []any{
				map[string]any{
					"type":               "Progressing",
					"status":             "True",
					"reason":             "NewReplicaSetAvailable",
					"message":            "ReplicaSet has successfully progressed.",
					"lastUpdateTime":     now,
					"lastTransitionTime": now,
				},
				map[string]any{
					"type":               "Available",
					"status":             "True",
					"reason":             "MinimumReplicasAvailable",
					"message":            "Deployment has minimum availability.",
					"lastUpdateTime":     now,
					"lastTransitionTime": now,
				},
			},
		}
	case def.Group == "" && def.Kind == "Service":
		spec, _ := u.Object["spec"].(map[string]any)
		if spec != nil && spec["clusterIP"] == nil {
			spec["clusterIP"] = "10.96.0.10"
		}
	case def.Group == "" && def.Kind == "Namespace":
		if u.Object["status"] == nil {
			u.Object["status"] = map[string]any{"phase": "Active"}
		}
	}
}

func deepMergeMaps(dst, src map[string]any, replacePayloadSections bool) map[string]any {
	out := make(map[string]any, len(dst)+len(src))
	for k, v := range dst {
		out[k] = deepCopyAny(v)
	}
	for k, sv := range src {
		if sv == nil {
			delete(out, k)
			continue
		}
		if replacePayloadSections && (k == "data" || k == "stringData" || k == "binaryData" || k == "spec") {
			out[k] = deepCopyAny(sv)
			continue
		}
		sm, sIsMap := sv.(map[string]any)
		dm, dIsMap := out[k].(map[string]any)
		if sIsMap && dIsMap {
			out[k] = deepMergeMaps(dm, sm, false)
		} else {
			out[k] = deepCopyAny(sv)
		}
	}
	return out
}

func deepCopyAny(v any) any {
	switch t := v.(type) {
	case map[string]any:
		cp := make(map[string]any, len(t))
		for k, val := range t {
			cp[k] = deepCopyAny(val)
		}
		return cp
	case map[string]string:
		cp := make(map[string]string, len(t))
		maps.Copy(cp, t)
		return cp
	case []any:
		cp := make([]any, len(t))
		for i, val := range t {
			cp[i] = deepCopyAny(val)
		}
		return cp
	default:
		return t
	}
}

func storeKey(group, version, resource, ns, name string) string {
	return fmt.Sprintf("%s/%s/%s/%s/%s", group, version, resource, ns, name)
}

func buildAPIGroupList() metav1.APIGroupList {
	groups := []string{"apps", "rbac.authorization.k8s.io", "source.toolkit.fluxcd.io", "kustomize.toolkit.fluxcd.io"}
	out := metav1.APIGroupList{
		TypeMeta: metav1.TypeMeta{Kind: "APIGroupList", APIVersion: "v1"},
	}
	for _, g := range groups {
		gv := metav1.GroupVersionForDiscovery{GroupVersion: g + "/v1", Version: "v1"}
		out.Groups = append(out.Groups, metav1.APIGroup{
			Name:             g,
			Versions:         []metav1.GroupVersionForDiscovery{gv},
			PreferredVersion: gv,
		})
	}
	return out
}

func buildAPIResourceList(group, version string) metav1.APIResourceList {
	gv := version
	if group != "" {
		gv = group + "/" + version
	}
	list := metav1.APIResourceList{
		TypeMeta:     metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"},
		GroupVersion: gv,
	}
	verbs := metav1.Verbs{"create", "delete", "get", "list", "patch", "update", "watch"}
	for _, def := range localResourceCatalog {
		if def.Group != group || def.Version != version {
			continue
		}
		list.APIResources = append(list.APIResources, metav1.APIResource{
			Name:         def.Resource,
			SingularName: strings.ToLower(def.Kind),
			Namespaced:   def.Namespaced,
			Kind:         def.Kind,
			Verbs:        verbs,
		})
		if def.HasStatus {
			list.APIResources = append(list.APIResources, metav1.APIResource{
				Name:       def.Resource + "/status",
				Namespaced: def.Namespaced,
				Kind:       def.Kind,
				Verbs:      metav1.Verbs{"get", "patch", "update"},
			})
		}
	}
	return list
}

func writeKubeStatus(w http.ResponseWriter, code int, reason metav1.StatusReason, msg, kind, name string) {
	st := metav1.Status{
		TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"},
		Status:   metav1.StatusFailure,
		Message:  msg,
		Reason:   reason,
		Code:     int32(code),
	}
	if name != "" || kind != "" {
		st.Details = &metav1.StatusDetails{Name: name, Kind: kind}
	}
	writeKubeJSON(w, code, st)
}

func writeKubeJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}

func randomUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%s-%s-%s-%s-%s",
		hex.EncodeToString(b[0:4]),
		hex.EncodeToString(b[4:6]),
		hex.EncodeToString(b[6:8]),
		hex.EncodeToString(b[8:10]),
		hex.EncodeToString(b[10:16]))
}

func randomShortSuffix() string {
	var b [3]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
