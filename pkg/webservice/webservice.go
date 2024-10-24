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

package webservice

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/julienschmidt/httprouter"

	"go.uber.org/zap"

	"github.com/G-Research/yunikorn-core/pkg/log"
	"github.com/G-Research/yunikorn-core/pkg/metrics/history"
	"github.com/G-Research/yunikorn-core/pkg/scheduler"
)

var imHistory *history.InternalMetricsHistory
var schedulerContext atomic.Pointer[scheduler.ClusterContext]

type WebService struct {
	httpServer *http.Server
}

func newRouter() *httprouter.Router {
	router := httprouter.New()
	for _, webRoute := range webRoutes {
		handler := loggingHandler(webRoute.HandlerFunc, webRoute.Name)
		router.Handler(webRoute.Method, webRoute.Pattern, handler)
	}
	return router
}

func loggingHandler(inner http.Handler, name string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		inner.ServeHTTP(w, r)
		log.Log(log.REST).Debug("Web router call details",
			zap.String("name", name),
			zap.String("method", r.Method),
			zap.String("uri", r.RequestURI),
			zap.Duration("duration", time.Since(start)))
	}
}

// StartWebApp starts the web app on the default port.
func (m *WebService) StartWebApp() {
	router := newRouter()
	m.httpServer = &http.Server{
		Addr:              ":9080",
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
	}

	log.Log(log.REST).Info("web-app started", zap.Int("port", 9080))
	go func() {
		httpError := m.httpServer.ListenAndServe()
		if httpError != nil && !errors.Is(httpError, http.ErrServerClosed) {
			log.Log(log.REST).Error("HTTP serving error",
				zap.Error(httpError))
		}
	}()
}

func NewWebApp(context *scheduler.ClusterContext, internalMetrics *history.InternalMetricsHistory) *WebService {
	m := &WebService{}
	schedulerContext.Store(context)
	imHistory = internalMetrics
	return m
}

func (m *WebService) StopWebApp() error {
	if m.httpServer != nil {
		// graceful shutdown in 5 seconds
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return m.httpServer.Shutdown(ctx)
	}

	return nil
}
