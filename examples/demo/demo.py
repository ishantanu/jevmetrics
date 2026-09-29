"""Synthetic traffic, deterministic mock inference, and end-to-end demo checks.

Uses only the Python standard library. Mock scores are fixtures, not Jev output.
"""
import json
import math
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

# name, datapoints, synthetic keep score
FIXTURES = [
    ("demo_checkout_latency", 2, 0.95),
    ("demo_debug_payload_size", 20, 0.05),
    ("demo_worker_queue_depth", 3, 0.5),
    ("up", 1, 1.0),
]


def fetch(url, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    request = Request(url, data=data, headers={"Content-Type": "application/json"})
    with urlopen(request, timeout=5) as response:
        return json.load(response)


class Mock(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def reply(self, status, body):
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        self.reply(200 if self.path == "/health" else 404, {"mode": "synthetic mock"})

    def do_POST(self):
        if self.path != "/v1/systemone":
            return self.reply(404, {})
        try:
            size = int(self.headers.get("Content-Length", "0"))
            if not 0 < size <= 65536:
                return self.reply(413, {})
            request = json.loads(self.rfile.read(size))
            name = request["state"]["metric"]["name"]
            keep = next((score for metric, _, score in FIXTURES if metric == name), 1.0)
        except (ValueError, KeyError, TypeError):
            return self.reply(400, {})
        time.sleep(0.03)
        self.reply(200, {"model": "demo-mock", "answers": {
            "relevance": {"type": "noul", "noul": keep},
            "redundancy": {"type": "noul", "noul": 1 - keep},
            "keep": {"type": "noul", "noul": keep},
            "action": {"type": "choice", "choice": "drop" if keep < 0.3 else "keep"},
        }})


def payload(tick):
    metrics = []
    for name, count, _ in FIXTURES:
        points = [{
            "attributes": [{"key": "demo.series", "value": {"stringValue": str(i)}}],
            "asDouble": 1 if name == "up" else 10 + i + tick % 5,
            "timeUnixNano": str(time.time_ns()),
        } for i in range(count)]
        metrics.append({"name": name, "description": "Synthetic demo fixture: " + name,
                        "gauge": {"dataPoints": points}})
    return {"resourceMetrics": [{
        "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "jevmetrics-demo"}}]},
        "scopeMetrics": [{"scope": {"name": "jevmetrics.demo"}, "metrics": metrics}],
    }]}


def generate():
    tick = 0
    while True:
        try:
            response = fetch("http://collector:4318/v1/metrics", payload(tick))
            if response.get("partialSuccess", {}).get("rejectedDataPoints", 0) not in (0, "0"):
                raise RuntimeError("Collector rejected datapoints")
            print("Sent 4 synthetic instruments / 26 datapoints", flush=True)
            tick += 1
        except Exception as error:
            print(f"Waiting for Collector: {error}", flush=True)
        time.sleep(2)


def query(expression, via_grafana=False):
    base = ("http://grafana:3000/api/datasources/proxy/uid/jevmetrics-prometheus"
            if via_grafana else "http://prometheus:9090")
    result = fetch(base + "/api/v1/query?" + urlencode({"query": expression}))
    if result.get("status") != "success":
        raise AssertionError(result)
    series = result["data"]["result"]
    if not series or any(not math.isfinite(float(item["value"][1])) for item in series):
        raise AssertionError(f"No finite data for {expression}: {series}")
    return series


def verify():
    deadline = time.monotonic() + 150
    last_error = None
    while time.monotonic() < deadline:
        try:
            # Assert the mock identity first: live-model outputs are deliberately unconstrained.
            query('jev_metric_keep_probability{jev_model="demo-mock"}')
            for name, count, _ in FIXTURES:
                series = query(name + '{job="demo-metrics",demo_series=~".+"}')
                if len(series) != count:
                    raise AssertionError(f"Original input lost: {name}: {len(series)} != {count}")
            for decision, reason in [("drop", "cached_score"), ("keep", "protected"),
                                     ("keep", "uncertain"), ("keep", "awaiting_assessment")]:
                result = query(f'jevmetrics_policy_datapoints_total{{mode="annotate",decision="{decision}",reason="{reason}"}}')
                if sum(float(item["value"][1]) for item in result) <= 0:
                    raise AssertionError(f"Missing policy decision {decision}/{reason}")
            dashboard = fetch("http://grafana:3000/api/dashboards/uid/jevmetrics-demo")["dashboard"]
            expected = json.loads(Path("/demo/dashboards/jevmetrics.json").read_text())
            if dashboard["title"] != expected["title"]:
                raise AssertionError("Grafana dashboard not provisioned")
            targets = [target["expr"] for panel in dashboard["panels"] for target in panel.get("targets", [])]
            expected_targets = [target["expr"] for panel in expected["panels"] for target in panel.get("targets", [])]
            if targets != expected_targets:
                raise AssertionError("Grafana dashboard queries differ from provisioned file")
            dropped = query('sum(jevmetrics_metrics_dropped_total) or vector(0)')
            if float(dropped[0]["value"][1]) != 0:
                raise AssertionError("Annotate mode dropped original instruments")
            for expression in targets:
                query(expression, via_grafana=True)
            print(f"PASS: original 26 datapoints retained, shadow reasons present, {len(targets)} dashboard queries return data through Grafana.")
            return
        except Exception as error:
            last_error = error
            time.sleep(2)
    raise SystemExit(f"Demo verification failed: {last_error}")


if __name__ == "__main__":
    role = sys.argv[1] if len(sys.argv) == 2 else ""
    if role == "mock":
        print("Synthetic mock inference listening on :8080", flush=True)
        ThreadingHTTPServer(("0.0.0.0", 8080), Mock).serve_forever()
    elif role == "generate":
        generate()
    elif role == "verify":
        verify()
    else:
        raise SystemExit("usage: demo.py mock|generate|verify")
