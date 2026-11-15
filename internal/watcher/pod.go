// Package watcher observes Kubernetes resources through client-go
// informers and emits normalized domain observations. It performs no
// detection, collection or persistence.
package watcher

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/ThuatPhan/PodSleuth/internal/domain"
)

// DefaultResyncPeriod is used when Options.ResyncPeriod is not set.
const DefaultResyncPeriod = 10 * time.Minute

// Handler consumes observations emitted by a Watcher.
type Handler interface {
	// OnObservation receives one normalized observation per informer event.
	OnObservation(ctx context.Context, obs domain.Observation) error
	// OnWatchError reports conversion or handler failures. The watcher
	// keeps running after recoverable errors.
	OnWatchError(ctx context.Context, err error)
}

// Watcher observes Kubernetes resources and emits domain observations.
type Watcher interface {
	// Start runs the watcher until ctx is cancelled. It blocks.
	Start(ctx context.Context, handler Handler) error
	// Ready closes once the initial cache has synced, or when the
	// watcher stops before syncing. Callers must also select on their
	// own context.
	Ready() <-chan struct{}
}

// Options configures a PodWatcher.
type Options struct {
	// Namespace restricts the watch to one namespace. Empty watches all
	// namespaces visible to the credentials.
	Namespace string

	// LabelSelector restricts the watch to Pods matching the selector.
	// Empty matches all Pods.
	LabelSelector string

	// ResyncPeriod controls informer resyncs. Zero means
	// DefaultResyncPeriod.
	ResyncPeriod time.Duration

	// Now returns the observation time. Defaults to time.Now.
	Now func() time.Time
}

// PodWatcher watches Pods through a shared informer and converts them
// into domain observations.
type PodWatcher struct {
	client    kubernetes.Interface
	clusterID string
	opts      Options

	started   atomic.Bool
	ready     chan struct{}
	readyOnce sync.Once
}

// NewPodWatcher returns a PodWatcher for the given client. The client is
// typically built by the kube factory; tests inject a fake.
func NewPodWatcher(client kubernetes.Interface, clusterID string, opts Options) *PodWatcher {
	if opts.ResyncPeriod <= 0 {
		opts.ResyncPeriod = DefaultResyncPeriod
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &PodWatcher{
		client:    client,
		clusterID: clusterID,
		opts:      opts,
		ready:     make(chan struct{}),
	}
}

// Ready implements Watcher.
func (w *PodWatcher) Ready() <-chan struct{} { return w.ready }

// Start implements Watcher. It blocks until ctx is cancelled and
// returns nil on that graceful shutdown. A second Start on the same
// watcher is an error.
func (w *PodWatcher) Start(ctx context.Context, handler Handler) error {
	if handler == nil {
		return fmt.Errorf("watcher: handler must not be nil")
	}
	if !w.started.CompareAndSwap(false, true) {
		return fmt.Errorf("watcher: already started")
	}

	informer := w.newInformer()
	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			w.handleAdd(ctx, handler, obj)
		},
		UpdateFunc: func(oldObj, newObj any) {
			w.handleUpdate(ctx, handler, oldObj, newObj)
		},
		DeleteFunc: func(obj any) {
			w.handleDelete(ctx, handler, obj)
		},
	})

	go w.awaitSync(ctx, informer)
	informer.Run(ctx.Done())
	return nil
}

func (w *PodWatcher) newInformer() cache.SharedIndexInformer {
	return informers.NewSharedInformerFactoryWithOptions(
		w.client,
		w.opts.ResyncPeriod,
		informers.WithNamespace(w.opts.Namespace),
		informers.WithTweakListOptions(func(o *metav1.ListOptions) {
			if w.opts.LabelSelector != "" {
				o.LabelSelector = w.opts.LabelSelector
			}
		}),
	).Core().V1().Pods().Informer()
}

func (w *PodWatcher) awaitSync(ctx context.Context, informer cache.SharedInformer) {
	cache.WaitForCacheSync(ctx.Done(), informer.HasSynced)
	w.closeReady()
}

func (w *PodWatcher) closeReady() {
	w.readyOnce.Do(func() { close(w.ready) })
}

func (w *PodWatcher) handleAdd(ctx context.Context, handler Handler, obj any) {
	pod, ok := asPod(obj)
	if !ok {
		w.report(ctx, handler, fmt.Errorf("watcher: unexpected added object type %T", obj))
		return
	}
	w.emit(ctx, handler, pod)
}

func (w *PodWatcher) handleUpdate(ctx context.Context, handler Handler, oldObj, newObj any) {
	oldPod, okOld := asPod(oldObj)
	newPod, okNew := asPod(newObj)
	if !okOld || !okNew {
		w.report(ctx, handler, fmt.Errorf("watcher: unexpected updated object types %T, %T", oldObj, newObj))
		return
	}
	// Resyncs and no-op updates deliver identical state; skipping them
	// avoids redundant observations. Real deduplication of repeated
	// failure episodes is the detector's responsibility.
	if oldPod.ResourceVersion == newPod.ResourceVersion {
		return
	}
	w.emit(ctx, handler, newPod)
}

func (w *PodWatcher) handleDelete(ctx context.Context, handler Handler, obj any) {
	pod, ok := asPod(obj)
	if !ok {
		w.report(ctx, handler, fmt.Errorf("watcher: unexpected deleted object type %T", obj))
		return
	}
	w.emit(ctx, handler, pod)
}

// asPod extracts a Pod from an informer event object, unwrapping
// tombstones from deleted objects whose delta was missed.
func asPod(obj any) (*corev1.Pod, bool) {
	if dfsu, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = dfsu.Obj
	}
	pod, ok := obj.(*corev1.Pod)
	return pod, ok
}

func (w *PodWatcher) emit(ctx context.Context, handler Handler, pod *corev1.Pod) {
	obs, err := convertPod(w.clusterID, pod, w.opts.Now())
	if err != nil {
		w.report(ctx, handler, err)
		return
	}
	if err := handler.OnObservation(ctx, obs); err != nil {
		w.report(ctx, handler, fmt.Errorf("watcher: handler rejected observation for pod %s/%s: %w", pod.Namespace, pod.Name, err))
	}
}

func (w *PodWatcher) report(ctx context.Context, handler Handler, err error) {
	handler.OnWatchError(ctx, err)
}

// convertPod converts a Kubernetes Pod into a domain observation. The
// payload is the full Pod JSON so later stages can extract status,
// container states and spec references without new API calls.
func convertPod(clusterID string, pod *corev1.Pod, now time.Time) (domain.Observation, error) {
	payload, err := json.Marshal(pod)
	if err != nil {
		return domain.Observation{}, fmt.Errorf("watcher: cannot serialize pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	return domain.Observation{
		Resource: domain.ResourceRef{
			ClusterID:  clusterID,
			APIVersion: "v1",
			Kind:       "Pod",
			Namespace:  pod.Namespace,
			Name:       pod.Name,
			UID:        string(pod.UID),
		},
		ObservedAt:      now.UTC(),
		ResourceVersion: pod.ResourceVersion,
		Payload:         payload,
	}, nil
}
