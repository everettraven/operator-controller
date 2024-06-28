package contentmanager

import (
	"context"
	"fmt"

	"github.com/operator-framework/operator-controller/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

type ContentManager interface {
	// ManageContent will:
	// 1. Create a new controller-runtime cache.Cache belonging to the provided ClusterExtension
	// 2. For each object provided:
	//   A. Use the provided controller.Controller to establish a watch for the resource
	ManageContent(context.Context, controller.Controller, *v1alpha1.ClusterExtension, []client.Object) error
	// RemoveManagedContent will:
	// 1. Remove/stop cache and any sources/informers for the provided ClusterExtension
	RemoveManagedContent(*v1alpha1.ClusterExtension) error
}

type RestConfigMapper func(context.Context, client.Object, *rest.Config) (*rest.Config, error)

type instance struct {
	rcm             RestConfigMapper
	baseCfg         *rest.Config
	extensionCaches map[string]cache.Cache
	scheme          *runtime.Scheme
	mapper          meta.RESTMapper
}

func New(rcm RestConfigMapper, cfg *rest.Config, scheme *runtime.Scheme, mapper meta.RESTMapper) ContentManager {
	return &instance{
		rcm:             rcm,
		baseCfg:         cfg,
		extensionCaches: make(map[string]cache.Cache),
		scheme:          scheme,
		mapper:          mapper,
	}
}

func (i *instance) ManageContent(ctx context.Context, ctrl controller.Controller, ce *v1alpha1.ClusterExtension, objs []client.Object) error {
	cfg, err := i.rcm(ctx, ce, i.baseCfg)
	if err != nil {
		return fmt.Errorf("getting rest.Config for ClusterExtension %q: %w", ce.Name, err)
	}

	// TODO: add a http.RoundTripper to the config to ensure it is always using an up
	// to date authentication token for the ServiceAccount token provided in the ClusterExtension.
	// Maybe this should be handled by the RestConfigMapper?

	c, err := cache.New(cfg, cache.Options{
		// TODO: explore how we can dynamically build this scheme based on the provided
		// resources to be managed. Using a top level scheme will not be sufficient as
		// that means it will have to know of every type that could be watched on startup
		Scheme: i.scheme,
	})
	if err != nil {
		return fmt.Errorf("creating cache for ClusterExtension %q: %w", ce.Name, err)
	}

	for _, obj := range objs {
		// TODO: Make sure we are sufficiently filtering
		// the watches to cache the minimum amount of information necessary.
		// This will likely result in some default label selection option being placed
		// in the cache configuration.
		err = ctrl.Watch(
			source.Kind(
				c,
				obj,
				handler.TypedEnqueueRequestForOwner[client.Object](
					i.scheme,
					i.mapper,
					ce,
				),
				nil,
			),
		)
		if err != nil {
			return fmt.Errorf("creating watch for ClusterExtension %q managed resource %s: %w", ce.Name, obj.GetObjectKind().GroupVersionKind(), err)
		}
	}

	go c.Start(ctx)
	// TODO: If a cache already exists, we should ensure that we are removing informers
	// for any resources that no longer need to be watched. Ideally we would not always create
	// a new cache, but it could be acceptable to do so and leave optimization as a follow up item.
	// if we continue to create a new cache every time, we should ensure that we are appropriately stopping
	// the cache and configured sources _before_ replacing it in the mapping.
	i.extensionCaches[ce.Name] = c

	return nil
}

func (i *instance) RemoveManagedContent(ce *v1alpha1.ClusterExtension) error {
	panic("Not implemented!")
}
