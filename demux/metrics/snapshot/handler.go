// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package snapshot

import (
	"encoding/json"
	"net/http"
)

// NewHandler returns an http.HandlerFunc that serves a JSON snapshot.
// Attach to any router or framework that accepts http.HandlerFunc.
func NewHandler(takeSnapshot func() Snapshot) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		snap := takeSnapshot()
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(snap); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}
