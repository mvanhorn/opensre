"""Tests for the /investigate/stream SSE endpoint."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from app.remote.server import app

_STREAM_PATCHES = (
    "app.pipeline.graph.build_graph",
    "app.config.LLMSettings",
    "app.cli.investigate.resolve_investigation_context",
    "app.state.factory.make_initial_state",
    "app.output.reset_tracker",
)


def _parse_sse(body: str) -> list[dict]:
    """Parse raw SSE text into a list of {event, data} dicts."""
    events: list[dict] = []
    current_event = ""
    data_lines: list[str] = []
    for line in body.split("\n"):
        if line.startswith("event:"):
            current_event = line[len("event:"):].strip()
            data_lines = []
        elif line.startswith("data:"):
            data_lines.append(line[len("data:"):].strip())
        elif line.startswith(":"):
            continue
        elif line == "":
            if current_event and data_lines:
                raw = "\n".join(data_lines)
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    data = {"raw": raw}
                events.append({"event": current_event, "data": data})
                current_event = ""
                data_lines = []
    if current_event and data_lines:
        raw = "\n".join(data_lines)
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            data = {"raw": raw}
        events.append({"event": current_event, "data": data})
    return events


def test_investigate_stream_emits_metadata_and_end() -> None:
    mock_graph = MagicMock()
    mock_graph.stream.return_value = iter([
        {"extract_alert": {"alert_name": "test-alert", "severity": "warning"}},
        {"resolve_integrations": {"resolved_integrations": {"grafana": {}}}},
    ])

    with (
        patch("app.pipeline.graph.build_graph", return_value=mock_graph),
        patch("app.config.LLMSettings"),
        patch(
            "app.cli.investigate.resolve_investigation_context",
            return_value=("test-alert", "test-pipeline", "warning"),
        ),
        patch("app.state.factory.make_initial_state", return_value={}),
        patch("app.output.reset_tracker"),
        patch("app.remote.server._save_investigation"),
    ):
        client = TestClient(app)
        resp = client.post(
            "/investigate/stream",
            json={"raw_alert": {"message": "test alert"}},
            headers={"x-api-key": ""},
        )

    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/event-stream")

    events = _parse_sse(resp.text)
    event_types = [e["event"] for e in events]
    assert event_types[0] == "metadata"
    assert "run_id" in events[0]["data"]
    assert "updates" in event_types
    assert event_types[-1] == "end"
    assert "id" in events[-1]["data"]


def test_investigate_stream_emits_error_on_failure() -> None:
    mock_graph = MagicMock()
    mock_graph.stream.side_effect = RuntimeError("LLM call failed")

    with (
        patch("app.pipeline.graph.build_graph", return_value=mock_graph),
        patch("app.config.LLMSettings"),
        patch(
            "app.cli.investigate.resolve_investigation_context",
            return_value=("test-alert", "test-pipeline", "warning"),
        ),
        patch("app.state.factory.make_initial_state", return_value={}),
        patch("app.output.reset_tracker"),
    ):
        client = TestClient(app)
        resp = client.post(
            "/investigate/stream",
            json={"raw_alert": {"message": "test alert"}},
            headers={"x-api-key": ""},
        )

    assert resp.status_code == 200
    events = _parse_sse(resp.text)
    event_types = [e["event"] for e in events]
    assert "metadata" in event_types
    assert "error" in event_types
    error_event = next(e for e in events if e["event"] == "error")
    assert "RuntimeError" in error_event["data"]["message"]
    assert error_event["data"]["run_id"]
    assert error_event["data"]["retryable"] is False


def test_investigate_stream_filters_messages_field() -> None:
    mock_graph = MagicMock()
    mock_graph.stream.return_value = iter([
        {"extract_alert": {
            "alert_name": "test",
            "messages": [{"role": "system", "content": "secret"}],
            "_auth_token": "tok_secret",
        }},
    ])

    with (
        patch("app.pipeline.graph.build_graph", return_value=mock_graph),
        patch("app.config.LLMSettings"),
        patch(
            "app.cli.investigate.resolve_investigation_context",
            return_value=("test", "test-pipeline", "warning"),
        ),
        patch("app.state.factory.make_initial_state", return_value={}),
        patch("app.output.reset_tracker"),
        patch("app.remote.server._save_investigation"),
    ):
        client = TestClient(app)
        resp = client.post(
            "/investigate/stream",
            json={"raw_alert": {"message": "test"}},
            headers={"x-api-key": ""},
        )

    events = _parse_sse(resp.text)
    update_events = [e for e in events if e["event"] == "updates"]
    assert len(update_events) == 1
    node_data = update_events[0]["data"]["extract_alert"]
    assert "messages" not in node_data
    assert "_auth_token" not in node_data
    assert node_data["alert_name"] == "test"
