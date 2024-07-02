package contentmanager

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestManageContent(t *testing.T) {}

func TestBuildScheme(t *testing.T) {
	type validation struct {
		gvks  []schema.GroupVersionKind
		valid bool
	}

	testcases := []struct {
		name    string
		objects []client.Object
		wantErr bool
		want    validation
	}{
		{
			name:    "Gvk is not defined",
			objects: []client.Object{&corev1.Pod{}},
			wantErr: true,
			want: validation{
				gvks:  []schema.GroupVersionKind{},
				valid: false,
			},
		},
		{
			name: "Check objects added in scheme",
			objects: []client.Object{
				&appsv1.Deployment{
					TypeMeta: metav1.TypeMeta{
						APIVersion: "apps/v1",
						Kind:       "Deployment",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name: "webserver",
					},
				},
			},
			wantErr: false,
			want: validation{
				gvks: []schema.GroupVersionKind{
					appsv1.SchemeGroupVersion.WithKind("Deployment"),
				},
				valid: true,
			},
		},
		{
			name: "Check object not defined in scheme",
			objects: []client.Object{
				&corev1.Pod{
					TypeMeta: metav1.TypeMeta{
						APIVersion: "v1",
						Kind:       "Pod",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name: "webserver",
					},
				},
			},
			wantErr: false,
			want: validation{
				gvks: []schema.GroupVersionKind{
					corev1.SchemeGroupVersion.WithKind("Secret"),
				},
				valid: false,
			},
		},
		{
			name: "Check if empty Group is valid",
			objects: []client.Object{
				&corev1.Pod{
					TypeMeta: metav1.TypeMeta{
						APIVersion: "v1",
						Kind:       "Pod",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name: "webserver",
					},
				},
			},
			wantErr: false,
			want: validation{
				gvks: []schema.GroupVersionKind{
					corev1.SchemeGroupVersion.WithKind("Pod"),
				},
				valid: true,
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			scheme, err := buildScheme(tc.objects)
			require.Equal(t, err != nil, tc.wantErr)
			for _, gvk := range tc.want.gvks {
				got := scheme.Recognizes(gvk)
				assert.Equal(t, got, tc.want.valid)
			}
		})
	}
}
