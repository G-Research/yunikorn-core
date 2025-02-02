/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package events

import (
	"time"

	"go.uber.org/zap"

	"github.com/G-Research/yunikorn-core/pkg/log"
	"github.com/G-Research/yunikorn-core/pkg/plugins"
)

// stores the push event internal
var defaultPushEventInterval = 2 * time.Second

type EventPublisher struct {
	store             *EventStore
	pushEventInterval time.Duration
	stop              chan struct{}
}

func CreateShimPublisher(store *EventStore) *EventPublisher {
	publisher := &EventPublisher{
		store:             store,
		pushEventInterval: defaultPushEventInterval,
		stop:              make(chan struct{}),
	}
	return publisher
}

func (sp *EventPublisher) StartService() {
	log.Log(log.Events).Info("Starting shim event publisher")
	go func() {
		for {
			select {
			case <-sp.stop:
				return
			case <-time.After(sp.pushEventInterval):
				messages := sp.store.CollectEvents()
				if len(messages) > 0 {
					if eventPlugin := plugins.GetResourceManagerCallbackPlugin(); eventPlugin != nil {
						log.Log(log.Events).Debug("Sending eventChannel", zap.Int("number of messages", len(messages)))
						eventPlugin.SendEvent(messages)
					}
				}
			}
		}
	}()
}

func (sp *EventPublisher) Stop() {
	log.Log(log.Events).Info("Stopping shim event publisher")
	close(sp.stop)
}

func (sp *EventPublisher) getEventStore() *EventStore {
	return sp.store
}
