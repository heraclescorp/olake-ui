package etl

import (
	"strings"
	"testing"
)

func TestResyncTableRequestAcceptsOnlyOneTableIdentity(t *testing.T) {
	req, err := decodeResyncTableRequest(strings.NewReader(`{"namespace":"public","stream_name":"orders"}`))
	if err != nil {
		t.Fatal(err)
	}
	if req.Namespace != "public" || req.StreamName != "orders" {
		t.Fatalf("unexpected identity: %+v", req)
	}
	for _, body := range []string{
		``, `null`, `[]`, `{}`, `{"namespace":"public"}`, `{"stream_name":"orders"}`,
		`{"namespace":"","stream_name":"orders"}`, `{"namespace":"public","stream_name":" "}`,
		`{"namespace":4,"stream_name":"orders"}`, `{"namespace":"public","stream_name":null}`,
		`{"namespace":"public","stream_name":"orders","streams_config":"{}"}`,
		`{"namespace":"public","stream_name":"orders","selected_streams":{}}`,
		`{"namespace":"public","stream_name":"orders","start_sync_after_clear":true}`,
		`{"namespace":"public","stream_name":"orders","column":"amount"}`,
		`{"namespace":"public","stream_name":"orders"} {"namespace":"other","stream_name":"users"}`,
		`{"namespace":"public","stream_name":"orders"} trailing`,
	} {
		t.Run(body, func(t *testing.T) {
			if _, err := decodeResyncTableRequest(strings.NewReader(body)); err == nil {
				t.Fatalf("accepted invalid body: %s", body)
			}
		})
	}
}
