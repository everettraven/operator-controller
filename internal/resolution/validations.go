package resolution

import (
	"fmt"

	"github.com/operator-framework/operator-controller/internal/catalogmetadata"
	"github.com/operator-framework/operator-registry/alpha/property"
	"k8s.io/apimachinery/pkg/util/sets"
)

func NoDependencyValidation(bundle *catalogmetadata.Bundle) error {
	unsupportedProps := sets.New(
		property.TypePackageRequired,
		property.TypeGVKRequired,
		property.TypeConstraint,
	)
	for i := range bundle.Properties {
		if unsupportedProps.Has(bundle.Properties[i].Type) {
			return fmt.Errorf(
				"bundle %q has a dependency declared via property %q which is currently not supported",
				bundle.Name,
				bundle.Properties[i].Type,
			)
		}
	}

	return nil
}
