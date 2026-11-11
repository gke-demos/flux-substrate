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
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gke-demos/flux-substrate/internal/gitserver"
)

const DefaultFleetTenantCount = 24

// SeedFleetOptions configures bootstrap of the 24 Tenant Environments, bare Git
// repositories, and managed Flux GitRepository/Kustomization CRs.
type SeedFleetOptions struct {
	KubeClient       client.Client
	GitServer        *gitserver.Server
	GitBaseURL       string
	ControlNamespace string
	TenantCount      int
	SyncInterval     time.Duration
	RetryInterval    time.Duration
}

// SeedFleet provisions namespaces, baseline Git repositories, and Flux GitRepository
// and Kustomization CRs (labeled reconciler.ate.dev/managed-by=flux-substrate) for
// tenant-01..tenant-NN.
func SeedFleet(ctx context.Context, opts SeedFleetOptions) error {
	if opts.KubeClient == nil {
		return errors.New("KubeClient is required")
	}
	if opts.GitServer == nil {
		return errors.New("GitServer is required")
	}
	gitBase := strings.TrimRight(strings.TrimSpace(opts.GitBaseURL), "/")
	if gitBase == "" {
		return errors.New("GitBaseURL is required")
	}

	ctrlNS := strings.TrimSpace(opts.ControlNamespace)
	if ctrlNS == "" {
		ctrlNS = defaultAtespace
	}
	count := opts.TenantCount
	if count <= 0 {
		count = DefaultFleetTenantCount
	}
	interval := opts.SyncInterval
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	retryInterval := opts.RetryInterval
	if retryInterval <= 0 {
		retryInterval = 30 * time.Second
	}

	if err := ensureNamespace(ctx, opts.KubeClient, ctrlNS); err != nil {
		return fmt.Errorf("ensure control namespace %s: %w", ctrlNS, err)
	}

	for i := 1; i <= count; i++ {
		tenantID := fmt.Sprintf("tenant-%02d", i)
		if err := ensureNamespace(ctx, opts.KubeClient, tenantID); err != nil {
			return fmt.Errorf("ensure tenant namespace %s: %w", tenantID, err)
		}

		if _, err := opts.GitServer.SeedDefaultTenant(tenantID, tenantID); err != nil {
			return fmt.Errorf("seed Git repo for %s: %w", tenantID, err)
		}

		if tok := strings.TrimSpace(opts.GitServer.TokenForTenant(tenantID)); tok != "" {
			if err := upsertGitTokenSecret(ctx, opts.KubeClient, ctrlNS, "git-"+tenantID, tok); err != nil {
				return fmt.Errorf("upsert Git token Secret for %s: %w", tenantID, err)
			}
		}

		if err := ensureTenantActorRBAC(ctx, opts.KubeClient, ctrlNS, tenantID, tenantID); err != nil {
			return fmt.Errorf("ensure actor RBAC for %s: %w", tenantID, err)
		}

		labels := map[string]string{
			ManagedByLabelKey: ManagedByLabelValue,
			TenantLabelKey:    tenantID,
		}
		repoURL := fmt.Sprintf("%s/git/%s.git", gitBase, tenantID)

		gr := &sourcev1.GitRepository{
			ObjectMeta: metav1.ObjectMeta{
				Name:      tenantID,
				Namespace: ctrlNS,
				Labels:    labels,
			},
			Spec: sourcev1.GitRepositorySpec{
				URL:       repoURL,
				Interval:  metav1.Duration{Duration: interval},
				Reference: &sourcev1.GitRepositoryRef{Branch: "main"},
				SecretRef: &fluxmeta.LocalObjectReference{Name: "git-" + tenantID},
			},
		}
		if err := upsertGitRepository(ctx, opts.KubeClient, gr); err != nil {
			return fmt.Errorf("upsert GitRepository %s: %w", tenantID, err)
		}

		retryDur := metav1.Duration{Duration: retryInterval}
		ks := &kustomizev1.Kustomization{
			ObjectMeta: metav1.ObjectMeta{
				Name:      tenantID,
				Namespace: ctrlNS,
				Labels:    labels,
			},
			Spec: kustomizev1.KustomizationSpec{
				Interval:      metav1.Duration{Duration: interval},
				RetryInterval: &retryDur,
				Path:          "./",
				Prune:         true,
				SourceRef: kustomizev1.CrossNamespaceSourceReference{
					Kind: sourcev1.GitRepositoryKind,
					Name: tenantID,
				},
				TargetNamespace: tenantID,
			},
		}
		if err := upsertKustomization(ctx, opts.KubeClient, ks); err != nil {
			return fmt.Errorf("upsert Kustomization %s: %w", tenantID, err)
		}
	}
	return nil
}

func ensureNamespace(ctx context.Context, kc client.Client, name string) error {
	var existing corev1.Namespace
	if err := kc.Get(ctx, client.ObjectKey{Name: name}, &existing); err == nil {
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if err := kc.Create(ctx, ns); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

func upsertGitRepository(ctx context.Context, kc client.Client, desired *sourcev1.GitRepository) error {
	var existing sourcev1.GitRepository
	err := kc.Get(ctx, client.ObjectKeyFromObject(desired), &existing)
	if apierrors.IsNotFound(err) {
		return kc.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	existing.Labels = desired.Labels
	existing.Spec = desired.Spec
	return kc.Update(ctx, &existing)
}

func upsertKustomization(ctx context.Context, kc client.Client, desired *kustomizev1.Kustomization) error {
	var existing kustomizev1.Kustomization
	err := kc.Get(ctx, client.ObjectKeyFromObject(desired), &existing)
	if apierrors.IsNotFound(err) {
		return kc.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	existing.Labels = desired.Labels
	existing.Spec = desired.Spec
	return kc.Update(ctx, &existing)
}

func ensureTenantActorRBAC(ctx context.Context, kc client.Client, atespace, actorName, targetNamespace string) error {
	if strings.TrimSpace(atespace) == "" {
		atespace = defaultAtespace
	}
	roleName := "flux-reconciler-" + actorName
	principal := fmt.Sprintf("ate:actor:%s:%s", atespace, actorName)
	labels := map[string]string{
		ManagedByLabelKey: ManagedByLabelValue,
		TenantLabelKey:    actorName,
	}

	desiredRole := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      roleName,
			Namespace: targetNamespace,
			Labels:    labels,
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{"", "apps", "autoscaling"},
				Resources: []string{"configmaps", "services", "serviceaccounts", "deployments", "horizontalpodautoscalers"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
			},
		},
	}

	var existingRole rbacv1.Role
	if err := kc.Get(ctx, client.ObjectKeyFromObject(desiredRole), &existingRole); apierrors.IsNotFound(err) {
		if createErr := kc.Create(ctx, desiredRole); createErr != nil && !apierrors.IsAlreadyExists(createErr) {
			return createErr
		}
	} else if err != nil {
		return err
	} else {
		existingRole.Labels = desiredRole.Labels
		existingRole.Rules = desiredRole.Rules
		if err := kc.Update(ctx, &existingRole); err != nil {
			return err
		}
	}

	desiredRB := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      roleName,
			Namespace: targetNamespace,
			Labels:    labels,
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "Role",
			Name:     roleName,
		},
		Subjects: []rbacv1.Subject{
			{
				APIGroup: rbacv1.GroupName,
				Kind:     rbacv1.UserKind,
				Name:     principal,
			},
		},
	}

	var existingRB rbacv1.RoleBinding
	if err := kc.Get(ctx, client.ObjectKeyFromObject(desiredRB), &existingRB); apierrors.IsNotFound(err) {
		if createErr := kc.Create(ctx, desiredRB); createErr != nil && !apierrors.IsAlreadyExists(createErr) {
			return createErr
		}
	} else if err != nil {
		return err
	} else {
		existingRB.Labels = desiredRB.Labels
		existingRB.Subjects = desiredRB.Subjects
		if err := kc.Update(ctx, &existingRB); err != nil {
			return err
		}
	}
	return nil
}
