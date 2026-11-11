package main_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/gke-demos/flux-substrate/internal/actor"
	"github.com/gke-demos/flux-substrate/internal/httputil"
)

var (
	cliRestCfg    *rest.Config
	cliKubeClient client.Client
	cliBinPath    string
	cliTenantSeq  atomic.Int32
)

func TestMain(m *testing.M) {
	ensureKubebuilderAssets()

	tmpBinDir, err := os.MkdirTemp("", "flux-substrate-bin-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "mkdirtemp: %v\n", err)
		os.Exit(1)
	}
	cliBinPath = filepath.Join(tmpBinDir, "flux-substrate")
	buildCmd := exec.Command("go", "build", "-o", cliBinPath, ".")
	if out, buildErr := buildCmd.CombinedOutput(); buildErr != nil {
		_ = os.RemoveAll(tmpBinDir)
		fmt.Fprintf(os.Stderr, "go build ./cmd/flux-substrate failed: %v\n%s\n", buildErr, string(out))
		os.Exit(1)
	}

	testEnv := &envtest.Environment{}
	cfg, err := testEnv.Start()
	if err != nil {
		_ = os.RemoveAll(tmpBinDir)
		fmt.Fprintf(os.Stderr, "start envtest: %v\n", err)
		os.Exit(1)
	}
	cliRestCfg = cfg

	kc, err := client.New(cfg, client.Options{})
	if err != nil {
		_ = testEnv.Stop()
		_ = os.RemoveAll(tmpBinDir)
		fmt.Fprintf(os.Stderr, "client.New: %v\n", err)
		os.Exit(1)
	}
	cliKubeClient = kc

	code := m.Run()
	_ = testEnv.Stop()
	_ = os.RemoveAll(tmpBinDir)
	os.Exit(code)
}

func TestMultiModeBinaryGitServerAndActorEndToEnd(t *testing.T) {
	tenantNS := fmt.Sprintf("tenant-cli-%02d", cliTenantSeq.Add(1))
	if err := cliKubeClient.Create(context.Background(), &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: tenantNS},
	}); err != nil {
		t.Fatalf("create namespace %s: %v", tenantNS, err)
	}

	kubeconfigPath := writeKubeconfig(
		t,
		cliRestCfg.Host,
		cliRestCfg.TLSClientConfig.CAData,
		cliRestCfg.TLSClientConfig.CertData,
		cliRestCfg.TLSClientConfig.KeyData,
		cliRestCfg.BearerToken,
	)

	gitToken, err := httputil.RandomToken(24)
	if err != nil {
		t.Fatalf("RandomToken: %v", err)
	}
	adminToken, err := httputil.RandomToken(24)
	if err != nil {
		t.Fatalf("RandomToken admin: %v", err)
	}

	gitAddr := reserveLoopbackAddr(t)
	actorAddr := reserveLoopbackAddr(t)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	gitCmd := exec.CommandContext(ctx, cliBinPath,
		"--mode=git-server",
		"--addr="+gitAddr,
		"--repos-dir="+t.TempDir(),
		"--seed-tenants="+tenantNS,
	)
	gitCmd.Env = append(os.Environ(),
		"GIT_SERVER_TOKEN="+gitToken,
		"GIT_ADMIN_TOKEN="+adminToken,
	)
	if err := gitCmd.Start(); err != nil {
		t.Fatalf("start git-server: %v", err)
	}
	t.Cleanup(func() {
		_ = gitCmd.Process.Kill()
		_ = gitCmd.Wait()
	})
	waitForReadyz(t, "http://"+gitAddr+"/readyz")

	actorCmd := exec.CommandContext(ctx, cliBinPath,
		"--mode=actor",
		"--addr="+actorAddr,
		"--actor-name="+tenantNS,
		"--atespace=flux-substrate",
		"--durable-dir="+t.TempDir(),
		"--kubeconfig="+kubeconfigPath,
	)
	actorCmd.Env = append(os.Environ(),
		"GIT_BEARER_TOKEN="+gitToken,
	)
	if err := actorCmd.Start(); err != nil {
		t.Fatalf("start actor: %v", err)
	}
	t.Cleanup(func() {
		_ = actorCmd.Process.Kill()
		_ = actorCmd.Wait()
	})
	waitForReadyz(t, "http://"+actorAddr+"/readyz")

	reqBody, _ := json.Marshal(actor.ReconcileRequest{
		TenantID:        tenantNS,
		RepoURL:         "http://" + gitAddr + "/git/" + tenantNS + ".git",
		Branch:          "main",
		Path:            "./",
		TargetNamespace: tenantNS,
	})
	resp, err := http.Post("http://"+actorAddr+"/reconcile", "application/json", bytes.NewReader(reqBody))
	if err != nil {
		t.Fatalf("POST /reconcile: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /reconcile status = %d, want 200", resp.StatusCode)
	}
	var res actor.ReconcileResult
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		t.Fatalf("decode ReconcileResult: %v", err)
	}
	if res.CacheHit {
		t.Errorf("Turn 1 CacheHit = true, want false")
	}
	if !res.Ready || len(res.Inventory) != 3 {
		t.Fatalf("unexpected Turn 1 result: ready=%v inventory=%+v error=%q", res.Ready, res.Inventory, res.Error)
	}
}

func TestMultiModeBinaryOIDCBridgeMode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	tenant01 := fmt.Sprintf("tenant-cli-%02d", cliTenantSeq.Add(1))
	tenant02 := fmt.Sprintf("tenant-cli-%02d", cliTenantSeq.Add(1))
	for _, ns := range []string{tenant01, tenant02} {
		if err := cliKubeClient.Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: ns},
		}); err != nil {
			t.Fatalf("create namespace %s: %v", ns, err)
		}
	}
	grantNamespaceRBAC(t, ctx, tenant01, "ate:actor:flux-substrate:"+tenant01)
	grantNamespaceRBAC(t, ctx, tenant02, "ate:actor:flux-substrate:"+tenant02)

	kubeconfigPath := writeKubeconfig(
		t,
		cliRestCfg.Host,
		cliRestCfg.TLSClientConfig.CAData,
		cliRestCfg.TLSClientConfig.CertData,
		cliRestCfg.TLSClientConfig.KeyData,
		cliRestCfg.BearerToken,
	)

	ecPriv, idpSrv, idpCAFile := startCLITestIDP(t, "cli-kid-1")

	bridgeAddr := reserveLoopbackAddr(t)
	bridgeURL := "https://" + bridgeAddr
	bridgeCertFile, bridgeKeyFile, bridgeCAPEM := writeLoopbackTLSCert(t)

	trustBundlePath := filepath.Join(t.TempDir(), "trust-bundle.pem")
	if err := os.WriteFile(trustBundlePath, bridgeCAPEM, 0o600); err != nil {
		t.Fatalf("write trust-bundle.pem: %v", err)
	}

	bridgeCmd := exec.CommandContext(ctx, cliBinPath,
		"--mode=oidc-bridge",
		"--addr="+bridgeAddr,
		"--tls-cert-file="+bridgeCertFile,
		"--tls-key-file="+bridgeKeyFile,
		"--oidc-issuer-url="+idpSrv.URL,
		"--oidc-audience="+bridgeURL,
		"--oidc-ca-file="+idpCAFile,
		"--kubeconfig="+kubeconfigPath,
	)
	if err := bridgeCmd.Start(); err != nil {
		t.Fatalf("start oidc-bridge: %v", err)
	}
	t.Cleanup(func() {
		_ = bridgeCmd.Process.Kill()
		_ = bridgeCmd.Wait()
	})
	waitForTLSReadyz(t, bridgeURL+"/readyz", bridgeCAPEM)

	gitToken, err := httputil.RandomToken(24)
	if err != nil {
		t.Fatalf("RandomToken: %v", err)
	}
	adminToken, err := httputil.RandomToken(24)
	if err != nil {
		t.Fatalf("RandomToken admin: %v", err)
	}

	gitAddr := reserveLoopbackAddr(t)
	gitCmd := exec.CommandContext(ctx, cliBinPath,
		"--mode=git-server",
		"--addr="+gitAddr,
		"--repos-dir="+t.TempDir(),
		"--seed-tenants="+tenant01,
	)
	gitCmd.Env = append(os.Environ(),
		"GIT_SERVER_TOKEN="+gitToken,
		"GIT_ADMIN_TOKEN="+adminToken,
	)
	if err := gitCmd.Start(); err != nil {
		t.Fatalf("start git-server: %v", err)
	}
	t.Cleanup(func() {
		_ = gitCmd.Process.Kill()
		_ = gitCmd.Wait()
	})
	waitForReadyz(t, "http://"+gitAddr+"/readyz")

	actorJWT := mintCLIActorJWT(t, ecPriv, "cli-kid-1", idpSrv.URL, bridgeURL, "actor/flux-substrate/"+tenant01, tenant01)
	actorAddr := reserveLoopbackAddr(t)
	actorCmd := exec.CommandContext(ctx, cliBinPath,
		"--mode=actor",
		"--addr="+actorAddr,
		"--actor-name="+tenant01,
		"--atespace=flux-substrate",
		"--durable-dir="+t.TempDir(),
		"--trust-bundle-file="+trustBundlePath,
		"--kube-bridge-url="+bridgeURL,
	)
	actorCmd.Env = append(os.Environ(),
		"GIT_BEARER_TOKEN="+gitToken,
		"KUBE_BEARER_TOKEN="+actorJWT,
	)
	if err := actorCmd.Start(); err != nil {
		t.Fatalf("start actor: %v", err)
	}
	t.Cleanup(func() {
		_ = actorCmd.Process.Kill()
		_ = actorCmd.Wait()
	})
	waitForReadyz(t, "http://"+actorAddr+"/readyz")

	// Reconcile into own namespace succeeds (200 OK).
	reqBody, _ := json.Marshal(actor.ReconcileRequest{
		TenantID:        tenant01,
		RepoURL:         "http://" + gitAddr + "/git/" + tenant01 + ".git",
		Branch:          "main",
		Path:            "./",
		TargetNamespace: tenant01,
	})
	resp, err := http.Post("http://"+actorAddr+"/reconcile", "application/json", bytes.NewReader(reqBody))
	if err != nil {
		t.Fatalf("POST /reconcile: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /reconcile via oidc-bridge status = %d, want 200", resp.StatusCode)
	}

	// Commit cross-namespace manifest targeting tenant02 and verify 403 Forbidden.
	commitPayload, _ := json.Marshal(map[string]any{
		"upsert": map[string]string{
			"kustomization.yaml": "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - escape.yaml\n",
			"escape.yaml":        fmt.Sprintf("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: breach\n  namespace: %s\ndata:\n  x: y\n", tenant02),
		},
		"message": "cross-namespace escape test",
	})
	commitReq, _ := http.NewRequest(http.MethodPost, "http://"+gitAddr+"/admin/repos/"+tenant01+"/commit", bytes.NewReader(commitPayload))
	commitReq.Header.Set("Authorization", "Bearer "+adminToken)
	commitReq.Header.Set("Content-Type", "application/json")
	commitResp, err := http.DefaultClient.Do(commitReq)
	if err != nil {
		t.Fatalf("admin commit: %v", err)
	}
	commitResp.Body.Close()
	if commitResp.StatusCode != http.StatusOK {
		t.Fatalf("admin commit status = %d, want 200", commitResp.StatusCode)
	}

	resp2, err := http.Post("http://"+actorAddr+"/reconcile", "application/json", bytes.NewReader(reqBody))
	if err != nil {
		t.Fatalf("POST /reconcile cross-namespace: %v", err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusForbidden {
		t.Fatalf("POST /reconcile cross-namespace status = %d, want 403", resp2.StatusCode)
	}
}

func grantNamespaceRBAC(t *testing.T, ctx context.Context, ns, user string) {
	t.Helper()
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: "reconciler", Namespace: ns},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{"", "apps"},
				Resources: []string{"configmaps", "services", "deployments", "replicasets", "pods"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
			},
		},
	}
	if err := cliKubeClient.Create(ctx, role); err != nil {
		t.Fatalf("create role in %s: %v", ns, err)
	}
	rb := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "reconciler", Namespace: ns},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "Role",
			Name:     role.Name,
		},
		Subjects: []rbacv1.Subject{
			{Kind: rbacv1.UserKind, APIGroup: rbacv1.GroupName, Name: user},
		},
	}
	if err := cliKubeClient.Create(ctx, rb); err != nil {
		t.Fatalf("create rolebinding in %s: %v", ns, err)
	}
}

func startCLITestIDP(t *testing.T, kid string) (*ecdsa.PrivateKey, *httptest.Server, string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ecdsa.GenerateKey: %v", err)
	}
	uncompressed, err := priv.PublicKey.Bytes()
	if err != nil {
		t.Fatalf("PublicKey.Bytes: %v", err)
	}
	var srv *httptest.Server
	mux := http.NewServeMux()
	mux.HandleFunc("GET /.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"issuer":   srv.URL,
			"jwks_uri": srv.URL + "/openid/v1/jwks",
		})
	})
	mux.HandleFunc("GET /openid/v1/jwks", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"keys": []map[string]any{
				{
					"kty": "EC",
					"kid": kid,
					"use": "sig",
					"alg": "ES256",
					"crv": "P-256",
					"x":   base64.RawURLEncoding.EncodeToString(uncompressed[1:33]),
					"y":   base64.RawURLEncoding.EncodeToString(uncompressed[33:65]),
				},
			},
		})
	})
	srv = httptest.NewTLSServer(mux)
	t.Cleanup(srv.Close)

	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	caFile := filepath.Join(t.TempDir(), "idp-ca.pem")
	if err := os.WriteFile(caFile, caPEM, 0o600); err != nil {
		t.Fatalf("WriteFile idp-ca.pem: %v", err)
	}
	return priv, srv, caFile
}

func writeLoopbackTLSCert(t *testing.T) (string, string, []byte) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ecdsa.GenerateKey: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "127.0.0.1"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("CreateCertificate: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("MarshalECPrivateKey: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	dir := t.TempDir()
	certFile := filepath.Join(dir, "tls.crt")
	keyFile := filepath.Join(dir, "tls.key")
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatalf("WriteFile cert: %v", err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatalf("WriteFile key: %v", err)
	}
	return certFile, keyFile, certPEM
}

func waitForTLSReadyz(t *testing.T, url string, caPEM []byte) {
	t.Helper()
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPEM)
	hc := &http.Client{
		Timeout: 2 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				MinVersion: tls.VersionTLS12,
				RootCAs:    pool,
			},
		},
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := hc.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for TLS readyz %s", url)
}

func mintCLIActorJWT(t *testing.T, priv *ecdsa.PrivateKey, kid, issuer, audience, sub, actorName string) string {
	t.Helper()
	now := time.Now()
	hdrJSON, _ := json.Marshal(map[string]any{"typ": "JWT", "alg": "ES256", "kid": kid})
	payJSON, _ := json.Marshal(map[string]any{
		"iss": issuer,
		"sub": sub,
		"aud": []string{audience},
		"iat": now.Unix(),
		"nbf": now.Add(-1 * time.Minute).Unix(),
		"exp": now.Add(10 * time.Minute).Unix(),
		"actor.ate.dev": map[string]any{
			"atespace":  "flux-substrate",
			"actorName": actorName,
			"actorUID":  "uid-" + actorName,
		},
	})
	signingInput := base64.RawURLEncoding.EncodeToString(hdrJSON) + "." + base64.RawURLEncoding.EncodeToString(payJSON)
	digest := sha256.Sum256([]byte(signingInput))
	r, s, err := ecdsa.Sign(rand.Reader, priv, digest[:])
	if err != nil {
		t.Fatalf("ecdsa.Sign: %v", err)
	}
	sig := make([]byte, 64)
	r.FillBytes(sig[:32])
	s.FillBytes(sig[32:])
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func reserveLoopbackAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen 127.0.0.1:0: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func waitForReadyz(t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", url)
}

func writeKubeconfig(t *testing.T, host string, caData, certData, keyData []byte, bearerToken string) string {
	t.Helper()
	cfg := clientcmdapi.NewConfig()
	cfg.Clusters["envtest"] = &clientcmdapi.Cluster{
		Server:                   host,
		CertificateAuthorityData: caData,
	}
	cfg.AuthInfos["envtest-user"] = &clientcmdapi.AuthInfo{
		ClientCertificateData: certData,
		ClientKeyData:         keyData,
		Token:                 bearerToken,
	}
	cfg.Contexts["envtest"] = &clientcmdapi.Context{
		Cluster:  "envtest",
		AuthInfo: "envtest-user",
	}
	cfg.CurrentContext = "envtest"

	outPath := filepath.Join(t.TempDir(), "kubeconfig")
	if err := clientcmd.WriteToFile(*cfg, outPath); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}
	return outPath
}

func ensureKubebuilderAssets() {
	if os.Getenv("KUBEBUILDER_ASSETS") != "" {
		return
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return
	}
	k8sDir := filepath.Join(home, ".local", "share", "kubebuilder-envtest", "k8s")
	entries, err := os.ReadDir(k8sDir)
	if err != nil || len(entries) == 0 {
		return
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			names = append(names, e.Name())
		}
	}
	if len(names) == 0 {
		return
	}
	slices.Sort(names)
	_ = os.Setenv("KUBEBUILDER_ASSETS", filepath.Join(k8sDir, names[len(names)-1]))
}
