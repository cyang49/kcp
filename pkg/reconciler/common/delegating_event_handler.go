/*
Copyright 2022 The KCP Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package common

import (
	"sync"
	"time"

	"github.com/kcp-dev/logicalcluster/v2"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
)

// DelegatingEventHandler multiplexes event handlers for multiple resource types and logical clusters.
type DelegatingEventHandler struct {
	lock          sync.RWMutex
	eventHandlers map[schema.GroupResource]map[logicalcluster.Name]cache.ResourceEventHandler
}

// NewDelegatingEventHandler returns a new delegatingEventHandler.
func NewDelegatingEventHandler() *DelegatingEventHandler {
	return &DelegatingEventHandler{
		eventHandlers: map[schema.GroupResource]map[logicalcluster.Name]cache.ResourceEventHandler{},
	}
}

// registerEventHandler registers an event handler, h, to receive events for the given resource/informer scoped only to
// clusterName.
func (d *DelegatingEventHandler) registerEventHandler(
	resource schema.GroupResource,
	informer cache.SharedIndexInformer,
	clusterName logicalcluster.Name,
	h cache.ResourceEventHandler,
) {
	d.lock.Lock()
	defer d.lock.Unlock()

	groupResourceHandlers, ok := d.eventHandlers[resource]
	if !ok {
		groupResourceHandlers = map[logicalcluster.Name]cache.ResourceEventHandler{}
		d.eventHandlers[resource] = groupResourceHandlers

		informer.AddEventHandler(d.resourceEventHandlerFuncs(resource))
	}

	groupResourceHandlers[clusterName] = h
}

// registerEventHandlerWithResyncPeriod registers an event handler, h, to receive events for the given resource/informer
// scoped only to clusterName, with the given resync period.
func (d *DelegatingEventHandler) registerEventHandlerWithResyncPeriod(
	resource schema.GroupResource,
	informer cache.SharedIndexInformer,
	clusterName logicalcluster.Name,
	h cache.ResourceEventHandler,
	resyncPeriod time.Duration,
) {
	d.lock.Lock()
	defer d.lock.Unlock()

	groupResourceHandlers, ok := d.eventHandlers[resource]
	if !ok {
		groupResourceHandlers = map[logicalcluster.Name]cache.ResourceEventHandler{}
		d.eventHandlers[resource] = groupResourceHandlers

		informer.AddEventHandlerWithResyncPeriod(d.resourceEventHandlerFuncs(resource), resyncPeriod)
	}

	groupResourceHandlers[clusterName] = h
}

func (d *DelegatingEventHandler) resourceEventHandlerFuncs(resource schema.GroupResource) cache.ResourceEventHandlerFuncs {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if h := d.getEventHandler(resource, obj); h != nil {
				h.OnAdd(obj)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if h := d.getEventHandler(resource, oldObj); h != nil {
				h.OnUpdate(oldObj, newObj)
			}
		},
		DeleteFunc: func(obj interface{}) {
			if h := d.getEventHandler(resource, obj); h != nil {
				h.OnDelete(obj)
			}
		},
	}
}

// getEventHandler returns a cache.ResourceEventHandler for resource and the logicalcluster.Name for obj.
func (d *DelegatingEventHandler) getEventHandler(resource schema.GroupResource, obj interface{}) cache.ResourceEventHandler {
	clusterName := ClusterNameForObj(obj)
	if clusterName.Empty() {
		return nil
	}

	d.lock.RLock()
	defer d.lock.RUnlock()

	groupResourceHandlers, ok := d.eventHandlers[resource]
	if !ok {
		return nil
	}

	return groupResourceHandlers[clusterName]
}
