package prom

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	baseURL      string
	tenantHeader string
	tenant       string
	httpClient   *http.Client
}

func NewClient(baseURL, tenantHeader, tenant string) *Client {
	return &Client{
		baseURL:      strings.TrimRight(baseURL, "/"),
		tenantHeader: tenantHeader,
		tenant:       tenant,
		httpClient:   &http.Client{Timeout: 10 * time.Second},
	}
}

type queryResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Value []any `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

func (c *Client) Query(ctx context.Context, query string) (float64, error) {
	if strings.TrimSpace(query) == "" {
		return 0, nil
	}

	u, err := url.Parse(c.baseURL + "/api/v1/query")
	if err != nil {
		return 0, err
	}
	q := u.Query()
	q.Set("query", query)
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return 0, err
	}
	if c.tenantHeader != "" && c.tenant != "" {
		req.Header.Set(c.tenantHeader, c.tenant)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		return 0, fmt.Errorf("prometheus returned %s", resp.Status)
	}

	var out queryResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, err
	}
	if out.Status != "success" {
		return 0, fmt.Errorf("prometheus query status %q", out.Status)
	}
	if len(out.Data.Result) == 0 || len(out.Data.Result[0].Value) < 2 {
		return 0, nil
	}

	s, ok := out.Data.Result[0].Value[1].(string)
	if !ok {
		return 0, fmt.Errorf("unexpected Prometheus value type")
	}
	return strconv.ParseFloat(s, 64)
}
