package prom

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestQuery(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-Scope-OrgID"); got != "tenant-a" {
			t.Fatalf("expected tenant header, got %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,"0.42"]}]}}`))
	}))
	defer ts.Close()

	c := NewClient(ts.URL, "X-Scope-OrgID", "tenant-a")
	got, err := c.Query(context.Background(), "up")
	if err != nil {
		t.Fatal(err)
	}
	if got != 0.42 {
		t.Fatalf("expected 0.42, got %v", got)
	}
}
