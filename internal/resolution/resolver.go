package resolution

import (
	"context"
	"errors"
	"fmt"
	"sort"

	mmsemver "github.com/Masterminds/semver/v3"
	ocv1alpha1 "github.com/operator-framework/operator-controller/api/v1alpha1"
	"github.com/operator-framework/operator-controller/internal/catalogmetadata"
	catalogfilter "github.com/operator-framework/operator-controller/internal/catalogmetadata/filter"
	catalogsort "github.com/operator-framework/operator-controller/internal/catalogmetadata/sort"
	"github.com/operator-framework/operator-controller/internal/labels"
	"helm.sh/helm/v3/pkg/storage/driver"
	helmclient "github.com/operator-framework/helm-operator-plugins/pkg/client"
)

// BundleProvider provides the way to retrieve a list of Bundles from a source,
// generally from a catalog client of some kind.
type BundleProvider interface {
	Bundles(ctx context.Context, packageName string) ([]*catalogmetadata.Bundle, error)
}

type InstalledBundleGetter interface {
	GetInstalledBundle(ctx context.Context, ext *ocv1alpha1.ClusterExtension) (*ocv1alpha1.BundleMetadata, error)
}

type ValidationFunc func(*catalogmetadata.Bundle) error

type Resolver struct {
	BundleProvider        BundleProvider
	InstalledBundleGetter InstalledBundleGetter
	Validations           []ValidationFunc
}

func (r *Resolver) Resolve(ctx context.Context, ext *ocv1alpha1.ClusterExtension) (*catalogmetadata.Bundle, error) {
	if ext == nil {
		return nil, errors.New("nil ClusterExtension provided, unable to resolve")
	}

	packageName := ext.Spec.PackageName
	channelName := ext.Spec.Channel
	versionRange := ext.Spec.Version

	allBundles, err := r.BundleProvider.Bundles(ctx, packageName)
	if err != nil {
		return nil, fmt.Errorf("fetching bundles: %w", err)
	}

	installedBundle, err := r.InstalledBundleGetter.GetInstalledBundle(ctx, ext)
	if err != nil {
		return nil, fmt.Errorf("fetching installed bundle: %w", err)
	}

	predicates := []catalogfilter.Predicate[catalogmetadata.Bundle]{
		catalogfilter.WithPackageName(packageName),
	}

	if channelName != "" {
		predicates = append(predicates, catalogfilter.InChannel(channelName))
	}

	if versionRange != "" {
		vr, err := mmsemver.NewConstraint(versionRange)
		if err != nil {
			return nil, fmt.Errorf("invalid version range %q: %w", versionRange, err)
		}
		predicates = append(predicates, catalogfilter.InMastermindsSemverRange(vr))
	}

	if ext.Spec.UpgradeConstraintPolicy != ocv1alpha1.UpgradeConstraintPolicyIgnore && installedBundle != nil {
		upgradePredicate, err := SuccessorsPredicate(ext.Spec.PackageName, installedBundle)
		if err != nil {
			return nil, err
		}

		predicates = append(predicates, upgradePredicate)
	}

	resultSet := catalogfilter.Filter(allBundles, catalogfilter.And(predicates...))

	var upgradeErrorPrefix string
	if installedBundle != nil {
		installedBundleVersion, err := mmsemver.NewVersion(installedBundle.Version)
		if err != nil {
			return nil, err
		}
		upgradeErrorPrefix = fmt.Sprintf("error upgrading from currently installed version %q: ", installedBundleVersion.String())
	}
	if len(resultSet) == 0 {
		switch {
		case versionRange != "" && channelName != "":
			return nil, fmt.Errorf("%sno package %q matching version %q in channel %q found", upgradeErrorPrefix, packageName, versionRange, channelName)
		case versionRange != "":
			return nil, fmt.Errorf("%sno package %q matching version %q found", upgradeErrorPrefix, packageName, versionRange)
		case channelName != "":
			return nil, fmt.Errorf("%sno package %q in channel %q found", upgradeErrorPrefix, packageName, channelName)
		default:
			return nil, fmt.Errorf("%sno package %q found", upgradeErrorPrefix, packageName)
		}
	}

	sort.SliceStable(resultSet, func(i, j int) bool {
		return catalogsort.ByVersion(resultSet[i], resultSet[j])
	})
	sort.SliceStable(resultSet, func(i, j int) bool {
		return catalogsort.ByDeprecated(resultSet[i], resultSet[j])
	})

	resolvedBundle := resultSet[0]

	for _, validation := range r.Validations {
		err := validation(resolvedBundle)
		if err != nil {
			return nil, fmt.Errorf("validating resolved bundle: %w", err)
		}
	}

	return resolvedBundle, nil
}

type DefaultInstalledBundleGetter struct {
	helmclient.ActionClientGetter
}

func (d *DefaultInstalledBundleGetter) GetInstalledBundle(ctx context.Context, ext *ocv1alpha1.ClusterExtension) (*ocv1alpha1.BundleMetadata, error) {
	cl, err := d.ActionClientFor(ctx, ext)
	if err != nil {
		return nil, err
	}

	release, err := cl.Get(ext.GetName())
	if err != nil && !errors.Is(err, driver.ErrReleaseNotFound) {
		return nil, err
	}
	if release == nil {
		return nil, nil
	}

	return &ocv1alpha1.BundleMetadata{
		Name:    release.Labels[labels.BundleNameKey],
		Version: release.Labels[labels.BundleVersionKey],
	}, nil
}
