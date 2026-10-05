package main

import (
	"context"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/rs/zerolog/log"
)

func startHttpListener(ctx context.Context, wg *sync.WaitGroup, router *httprouter.Router, address string) {
	router.HandleOPTIONS = true
	router.GlobalOPTIONS = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	withCors := func(router http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			header := w.Header()
			header.Set("Access-Control-Allow-Methods", "*")
			header.Set("Access-Control-Allow-Origin", "*")
			header.Set("Access-Control-Allow-Methods", "*")
			header.Set("Access-Control-Allow-Headers", "*")
			router.ServeHTTP(w, r)
		})
	}

	server := &http.Server{
		Addr:    address,
		Handler: withCors(router),
		BaseContext: func(l net.Listener) context.Context {
			return ctx
		},
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		<-ctx.Done()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		log.Info().Msgf("shutting down HTTP server..")
		if err := server.Shutdown(ctx); err != nil {
			log.Err(err).Msgf("HTTP server shutdown")
		}
	}()

	// TODO: maybe can use this for more graceful handling
	// server.RegisterOnShutdown()

	log.Info().Msgf("serving at %v", server.Addr)
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		// Error starting or closing listener:
		log.Fatal().Msgf("HTTP server ListendAndServe: %v", err)
	}
}
