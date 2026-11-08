package etl

import (
	"errors"
	"testing"

	"github.com/datazip-inc/olake-ui/server/internal/constants"
	"github.com/datazip-inc/olake-ui/server/internal/models/dto"
	"github.com/datazip-inc/olake-ui/server/internal/types"
)

func TestResyncLegacyModeFallbackAndSavedSelectionPrecedence(t *testing.T) {
	for _, tc := range []struct {
		name          string
		streamMode    string
		selectionMode string
		allowed       bool
	}{
		{name: "legacy CDC backfill", streamMode: "cdc", allowed: true},
		{name: "legacy full refresh", streamMode: "full_refresh", allowed: true},
		{name: "legacy incremental", streamMode: "incremental", allowed: true},
		{name: "legacy CDC only", streamMode: "strict_cdc"},
		{name: "unknown saved mode"},
		{name: "selected CDC only wins", streamMode: "cdc", selectionMode: `,"sync_mode":"strict_cdc"`},
		{name: "selected backfill wins", streamMode: "strict_cdc", selectionMode: `,"sync_mode":"cdc"`, allowed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog := types.StreamsCatalog{Streams: `{"streams":[{"stream":{"namespace":"public","name":"orders","sync_mode":"` + tc.streamMode + `"}}],"selected_streams":{"public":[{"stream_name":"orders"` + tc.selectionMode + `}]}}`}
			_, err := tableResyncCatalog(catalog, dto.ResyncTableRequest{Namespace: "public", StreamName: "orders"})
			if tc.allowed && err != nil {
				t.Fatal(err)
			}
			if !tc.allowed && !errors.Is(err, constants.ErrInvalidResync) {
				t.Fatalf("unsafe saved mode accepted: %v", err)
			}
		})
	}
}

func TestResyncRejectsMalformedAndAmbiguousSavedCatalogs(t *testing.T) {
	for _, catalog := range []types.StreamsCatalog{
		{},
		{Streams: `null`},
		{Streams: `{"streams":`},
		{Streams: `{"streams":[],"selected_streams":{"public":[{"stream_name":"orders"}]}}`},
		{Streams: `{"streams":[{"stream":{"namespace":"public","name":"orders","sync_mode":"cdc"}}],"selected_streams":{}}`},
		{Streams: `{"streams":[{"stream":{"namespace":"public","name":"orders","sync_mode":"cdc"}},{"stream":{"namespace":"public","name":"orders","sync_mode":"cdc"}}],"selected_streams":{"public":[{"stream_name":"orders"}]}}`},
		{Streams: `{"streams":[{"stream":{"namespace":"public","name":"orders","sync_mode":"cdc"}}],"selected_streams":{"public":[{"stream_name":"orders"},{"stream_name":"orders"}]}}`},
		{Available: resyncAvailable, Selected: `{"selected_streams":[]}`},
		{Available: `null`, Selected: resyncSelected},
	} {
		if scoped, err := tableResyncCatalog(catalog, dto.ResyncTableRequest{Namespace: "public", StreamName: "orders"}); !errors.Is(err, constants.ErrInvalidResync) || scoped != (types.StreamsCatalog{}) {
			t.Fatalf("unsafe catalog returned destructive scope: %+v, %v", scoped, err)
		}
	}
}
