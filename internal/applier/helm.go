package applier

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"

	helmclient "github.com/operator-framework/helm-operator-plugins/pkg/client"
	ocv1alpha1 "github.com/operator-framework/operator-controller/api/v1alpha1"
	"github.com/operator-framework/operator-controller/internal/catalogmetadata"
	"github.com/operator-framework/operator-controller/internal/labels"
	"github.com/operator-framework/operator-controller/internal/rukpak/convert"
	"github.com/operator-framework/operator-controller/internal/rukpak/preflights/crdupgradesafety"
	"github.com/operator-framework/operator-controller/internal/rukpak/util"
	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/chartutil"
	"helm.sh/helm/v3/pkg/postrender"
	"helm.sh/helm/v3/pkg/release"
	"helm.sh/helm/v3/pkg/storage/driver"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	apimachyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"
	corev1 "k8s.io/api/core/v1"
)

// Preflight is a check that should be run before making any changes to the cluster
type Preflight interface {
	// Install runs checks that should be successful prior
	// to installing the Helm release. It is provided
	// a Helm release and returns an error if the
	// check is unsuccessful
	Install(context.Context, *release.Release) error

	// Upgrade runs checks that should be successful prior
	// to upgrading the Helm release. It is provided
	// a Helm release and returns an error if the
	// check is unsuccessful
	Upgrade(context.Context, *release.Release) error
}

type Helm struct {
	ActionClientGetter helmclient.ActionClientGetter
	Preflights         []Preflight
}

func (h *Helm) Apply(ctx context.Context, contentFS fs.FS, ext *ocv1alpha1.ClusterExtension, bundle *catalogmetadata.Bundle) ([]client.Object, error) {
	chrt, err := convert.RegistryV1ToHelmChart(ctx, contentFS, ext.Spec.InstallNamespace, []string{corev1.NamespaceAll})
	if err != nil {
		return nil, err
	}
	values := chartutil.Values{}

	ac, err := h.ActionClientGetter.ActionClientFor(ctx, ext)
	if err != nil {
		return nil, err
	}

	bundleVersion, err := bundle.Version()
    if err != nil {
        return nil, err
    }

	post := &postrenderer{
		labels: map[string]string{
			labels.OwnerKindKey:     ocv1alpha1.ClusterExtensionKind,
			labels.OwnerNameKey:     ext.GetName(),
			labels.BundleNameKey:    bundle.Name,
			labels.PackageNameKey:   bundle.Package,
			labels.BundleVersionKey: bundleVersion.String(),
		},
	}

	rel, desiredRel, state, err := h.getReleaseState(ac, ext, chrt, values, post)
	if err != nil {
		return nil, err
	}

	for _, preflight := range h.Preflights {
		if ext.Spec.Preflight != nil && ext.Spec.Preflight.CRDUpgradeSafety != nil {
			if _, ok := preflight.(*crdupgradesafety.Preflight); ok && ext.Spec.Preflight.CRDUpgradeSafety.Disabled {
				// Skip this preflight check because it is of type *crdupgradesafety.Preflight and the CRD Upgrade Safety
				// preflight check has been disabled
				continue
			}
		}
		switch state {
		case stateNeedsInstall:
			err := preflight.Install(ctx, desiredRel)
			if err != nil {
				return nil, err
			}
		case stateNeedsUpgrade:
			err := preflight.Upgrade(ctx, desiredRel)
			if err != nil {
				return nil, err
			}
		}
	}

	switch state {
	case stateNeedsInstall:
		rel, err = ac.Install(ext.GetName(), ext.Spec.InstallNamespace, chrt, values, func(install *action.Install) error {
			install.CreateNamespace = false
			install.Labels = map[string]string{labels.BundleNameKey: bundle.Name, labels.PackageNameKey: bundle.Package, labels.BundleVersionKey: bundleVersion.String()}
			return nil
		}, helmclient.AppendInstallPostRenderer(post))
		if err != nil {
			return nil, err
		}
	case stateNeedsUpgrade:
		rel, err = ac.Upgrade(ext.GetName(), ext.Spec.InstallNamespace, chrt, values, func(upgrade *action.Upgrade) error {
			upgrade.MaxHistory = maxHelmReleaseHistory
			upgrade.Labels = map[string]string{labels.BundleNameKey: bundle.Name, labels.PackageNameKey: bundle.Package, labels.BundleVersionKey: bundleVersion.String()}
			return nil
		}, helmclient.AppendUpgradePostRenderer(post))
		if err != nil {
			return nil, err
		}
	case stateUnchanged:
		if err := ac.Reconcile(rel); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unexpected release state %q", state)
	}

	relObjects, err := util.ManifestObjects(strings.NewReader(rel.Manifest), fmt.Sprintf("%s-release-manifest", rel.Name))
	if err != nil {
		return nil, fmt.Errorf("getting managed objects: %w", err)
	}
	return relObjects, nil
}

type releaseState string

const (
	stateNeedsInstall     releaseState = "NeedsInstall"
	stateNeedsUpgrade     releaseState = "NeedsUpgrade"
	stateUnchanged        releaseState = "Unchanged"
	stateError            releaseState = "Error"
	maxHelmReleaseHistory              = 10
)

func (h *Helm) getReleaseState(cl helmclient.ActionInterface, ext *ocv1alpha1.ClusterExtension, chrt *chart.Chart, values chartutil.Values, post *postrenderer) (*release.Release, *release.Release, releaseState, error) {
	currentRelease, err := cl.Get(ext.GetName())
	if err != nil && !errors.Is(err, driver.ErrReleaseNotFound) {
		return nil, nil, stateError, err
	}
	if errors.Is(err, driver.ErrReleaseNotFound) {
		return nil, nil, stateNeedsInstall, nil
	}

	if errors.Is(err, driver.ErrReleaseNotFound) {
		desiredRelease, err := cl.Install(ext.GetName(), ext.Spec.InstallNamespace, chrt, values, func(i *action.Install) error {
			i.DryRun = true
			i.DryRunOption = "server"
			return nil
		}, helmclient.AppendInstallPostRenderer(post))
		if err != nil {
			return nil, nil, stateError, err
		}
		return nil, desiredRelease, stateNeedsInstall, nil
	}
	desiredRelease, err := cl.Upgrade(ext.GetName(), ext.Spec.InstallNamespace, chrt, values, func(upgrade *action.Upgrade) error {
		upgrade.MaxHistory = maxHelmReleaseHistory
		upgrade.DryRun = true
		upgrade.DryRunOption = "server"
		return nil
	}, helmclient.AppendUpgradePostRenderer(post))
	if err != nil {
		return currentRelease, nil, stateError, err
	}
	relState := stateUnchanged
	if desiredRelease.Manifest != currentRelease.Manifest ||
		currentRelease.Info.Status == release.StatusFailed ||
		currentRelease.Info.Status == release.StatusSuperseded {
		relState = stateNeedsUpgrade
	}
	return currentRelease, desiredRelease, relState, nil
}

type postrenderer struct {
	labels  map[string]string
	cascade postrender.PostRenderer
}

func (p *postrenderer) Run(renderedManifests *bytes.Buffer) (*bytes.Buffer, error) {
	var buf bytes.Buffer
	dec := apimachyaml.NewYAMLOrJSONDecoder(renderedManifests, 1024)
	for {
		obj := unstructured.Unstructured{}
		err := dec.Decode(&obj)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		obj.SetLabels(util.MergeMaps(obj.GetLabels(), p.labels))
		b, err := obj.MarshalJSON()
		if err != nil {
			return nil, err
		}
		buf.Write(b)
	}
	if p.cascade != nil {
		return p.cascade.Run(&buf)
	}
	return &buf, nil
}
