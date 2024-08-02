package crdupgradesafety

import (
	"context"
	"errors"
	"fmt"
	"strings"

	kappcus "carvel.dev/kapp/pkg/kapp/crdupgradesafety"
	"helm.sh/helm/v3/pkg/release"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsv1client "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/typed/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/operator-framework/operator-controller/internal/rukpak/util"
)

type Option func(p *Preflight)

func WithValidator(v *kappcus.Validator) Option {
	return func(p *Preflight) {
		p.validator = v
	}
}

type Preflight struct {
	crdClient             apiextensionsv1client.CustomResourceDefinitionInterface
	validator             *kappcus.Validator
	crossVersionValidator *crossVersionValidator
}

func NewPreflight(crdCli apiextensionsv1client.CustomResourceDefinitionInterface, opts ...Option) *Preflight {
	p := &Preflight{
		crdClient: crdCli,
		// create a default validator. Can be overridden via the options
		validator: &kappcus.Validator{
			Validations: []kappcus.Validation{
				kappcus.NewValidationFunc("NoScopeChange", kappcus.NoScopeChange),
				kappcus.NewValidationFunc("NoStoredVersionRemoved", kappcus.NoStoredVersionRemoved),
				kappcus.NewValidationFunc("NoExistingFieldRemoved", kappcus.NoExistingFieldRemoved),
				&kappcus.ChangeValidator{
					Validations: []kappcus.ChangeValidation{
						kappcus.EnumChangeValidation,
						kappcus.RequiredFieldChangeValidation,
						kappcus.MaximumChangeValidation,
						kappcus.MaximumItemsChangeValidation,
						kappcus.MaximumLengthChangeValidation,
						kappcus.MaximumPropertiesChangeValidation,
						kappcus.MinimumChangeValidation,
						kappcus.MinimumItemsChangeValidation,
						kappcus.MinimumLengthChangeValidation,
						kappcus.MinimumPropertiesChangeValidation,
						kappcus.DefaultValueChangeValidation,
					},
				},
			},
		},
		crossVersionValidator: &crossVersionValidator{
			changeValidations: []kappcus.ChangeValidation{
				kappcus.EnumChangeValidation,
				kappcus.RequiredFieldChangeValidation,
				kappcus.MaximumChangeValidation,
				kappcus.MaximumItemsChangeValidation,
				kappcus.MaximumLengthChangeValidation,
				kappcus.MaximumPropertiesChangeValidation,
				kappcus.MinimumChangeValidation,
				kappcus.MinimumItemsChangeValidation,
				kappcus.MinimumLengthChangeValidation,
				kappcus.MinimumPropertiesChangeValidation,
				kappcus.DefaultValueChangeValidation,
			},
		},
	}

	for _, o := range opts {
		o(p)
	}

	return p
}

func (p *Preflight) Install(_ context.Context, _ *release.Release) error {
	return nil
}

func (p *Preflight) Upgrade(ctx context.Context, rel *release.Release) error {
	if rel == nil {
		return nil
	}

	relObjects, err := util.ManifestObjects(strings.NewReader(rel.Manifest), fmt.Sprintf("%s-release-manifest", rel.Name))
	if err != nil {
		return fmt.Errorf("parsing release %q objects: %w", rel.Name, err)
	}

	validateErrors := []error{}
	for _, obj := range relObjects {
		if obj.GetObjectKind().GroupVersionKind() != apiextensionsv1.SchemeGroupVersion.WithKind("CustomResourceDefinition") {
			continue
		}

		newCrd := &apiextensionsv1.CustomResourceDefinition{}
		uMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
		if err != nil {
			return fmt.Errorf("converting object %q to unstructured: %w", obj.GetName(), err)
		}
		err = runtime.DefaultUnstructuredConverter.FromUnstructured(uMap, newCrd)
		if err != nil {
			return fmt.Errorf("converting unstructured to CRD object: %w", err)
		}

		oldCrd, err := p.crdClient.Get(ctx, newCrd.Name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("getting existing resource for CRD %q: %w", newCrd.Name, err)
		}

		err = p.validator.Validate(*oldCrd, *newCrd)
		if err != nil {
			validateErrors = append(validateErrors, fmt.Errorf("validating upgrade for CRD %q failed: %w", newCrd.Name, err))
		}

        err = p.crossVersionValidator.Validate(*newCrd)
        if err != nil {
			validateErrors = append(validateErrors, fmt.Errorf("cross version validation for CRD %q failed: %w", newCrd.Name, err))
        }
	}

	return errors.Join(validateErrors...)
}

type crossVersionValidator struct {
	changeValidations []kappcus.ChangeValidation
}

func (cvv *crossVersionValidator) Validate(crd apiextensionsv1.CustomResourceDefinition) error {
	errs := []error{}
	for _, versionA := range crd.Spec.Versions {
		for _, versionB := range crd.Spec.Versions {
			// We don't need to check the same version for compatibility. That should
			// already be handled.
			if versionA.Name == versionB.Name {
				continue
			}

			flatOld := kappcus.FlattenSchema(versionA.Schema.OpenAPIV3Schema)
			flatNew := kappcus.FlattenSchema(versionB.Schema.OpenAPIV3Schema)

			diffs, err := kappcus.CalculateFlatSchemaDiff(flatOld, flatNew)
			if err != nil {
				errs = append(errs, fmt.Errorf("calculating schema diff for CRD version %q against CRD version %q", versionA.Name, versionB.Name))
				continue
			}

			for field, diff := range diffs {
				handled := false
				for _, validation := range cvv.changeValidations {
					ok, err := validation(diff)
					if err != nil {
						errs = append(errs, fmt.Errorf("version %q compared to version %q, field %q: %w", versionA.Name, versionB.Name, field, err))
					}
					if ok {
						handled = true
						break
					}
				}

				if !handled {
					errs = append(errs, fmt.Errorf("version %q compared to version %q, field %q has unknown change, refusing to determine that change is safe", versionA.Name, versionB.Name, field))
				}
			}
		}
	}

	return errors.Join(errs...)
}
