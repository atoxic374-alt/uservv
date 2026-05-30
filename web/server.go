package web

import (
	"net/http"
	"users/globals"
	"users/types"
)

func post(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		h(w, r)
	}
}

func StartServer(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", HandleWS)

	// Global status
	mux.HandleFunc("/api/status", HandleStatus)

	// Username config & control
	mux.HandleFunc("/api/config", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			HandleGetConfig(w, r)
		} else if r.Method == http.MethodPost {
			HandleSetConfig(w, r)
		} else {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/api/start", post(HandleStart))
	mux.HandleFunc("/api/stop", post(HandleStop))
	mux.HandleFunc("/api/sessions", HandleGetSessions)

	// Username results
	mux.HandleFunc("/api/results/tag/", post(HandleTagResult))
	mux.HandleFunc("/api/results", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			HandleGetResults(w, r)
		case http.MethodDelete:
			HandleClearResults(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// Proxies
	mux.HandleFunc("/api/proxies/fetch", post(HandleFetchPublicProxies))
	mux.HandleFunc("/api/proxies/test/all", post(HandleTestAllProxies))
	mux.HandleFunc("/api/proxies/test/", post(HandleTestProxy))
	mux.HandleFunc("/api/proxies/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			HandleDeleteProxy(w, r)
		} else {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/api/proxies", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			HandleGetProxies(w, r)
		case http.MethodPost:
			HandleAddProxy(w, r)
		case http.MethodDelete:
			HandleDeleteAllProxies(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// Vanity config & control
	mux.HandleFunc("/api/vanity/config", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			HandleGetVanityConfig(w, r)
		} else if r.Method == http.MethodPost {
			HandleSetVanityConfig(w, r)
		} else {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/api/vanity/start", post(HandleVanityStart))
	mux.HandleFunc("/api/vanity/stop", post(HandleVanityStop))
	mux.HandleFunc("/api/vanity/status", HandleVanityStatus)
	mux.HandleFunc("/api/vanity/sessions", HandleGetVanitySessions)

	// Vanity results
	mux.HandleFunc("/api/vanity/results/tag/", post(HandleVanityTagResult))
	mux.HandleFunc("/api/vanity/results", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			HandleGetVanityResults(w, r)
		case http.MethodDelete:
			HandleClearVanityResults(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// Static files
	mux.HandleFunc("/favicon.ico", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		http.ServeFile(w, r, "web/static/index.html")
	})

	// Bridge globals.EventCh  WebSocket hub
	go func() {
		for event := range globals.EventCh {
			WSHub.Broadcast(event)
		}
	}()
	go WSHub.Run()

	_ = types.Event{}
	return http.ListenAndServe(addr, mux)
}
