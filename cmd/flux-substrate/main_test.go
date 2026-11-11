package main_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
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
