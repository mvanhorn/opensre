"""Tests for StreamRenderer error handling and run_id propagation."""

from __future__ import annotations

import os
from unittest.mock import patch

from app.remote.renderer import StreamRenderer
from app.remote.stream import StreamEvent


def test_renderer_captures_run_id_from_metadata() -> None:
    renderer = StreamRenderer()
    events = iter([
        StreamEvent("metadata", data={"run_id": "test-run-id-123"}),
        StreamEvent("end", data={}),
    ])

    with patch.dict(os.environ, {"TRACER_OUTPUT_FORMAT": "text"}):
        renderer.render_stream(events)

    assert renderer.run_id == "test-run-id-123"
    assert renderer.stream_completed is True


def test_renderer_handles_error_event() -> None:
    renderer = StreamRenderer()
    events = iter([
        StreamEvent("metadata", data={"run_id": "run-xyz"}),
        StreamEvent(
            "updates",
            data={"extract_alert": {"alert_name": "test"}},
            node_name="extract_alert",
        ),
        StreamEvent("error", data={
            "message": "ValueError: something broke",
            "node": "plan_actions",
            "run_id": "run-xyz",
            "retryable": False,
        }),
    ])

    with patch.dict(os.environ, {"TRACER_OUTPUT_FORMAT": "text"}):
        result = renderer.render_stream(events)

    assert renderer.run_id == "run-xyz"
    assert renderer.stream_completed is False
    assert result == {"alert_name": "test"}


def test_renderer_handles_empty_stream() -> None:
    renderer = StreamRenderer()
    events = iter([])

    with patch.dict(os.environ, {"TRACER_OUTPUT_FORMAT": "text"}):
        result = renderer.render_stream(events)

    assert result == {}
    assert renderer.events_received == 0


def test_renderer_tracks_node_names() -> None:
    renderer = StreamRenderer()
    events = iter([
        StreamEvent(
            "updates",
            data={"extract_alert": {"alert_name": "test"}},
            node_name="extract_alert",
        ),
        StreamEvent(
            "updates",
            data={"resolve_integrations": {"resolved_integrations": {}}},
            node_name="resolve_integrations",
        ),
        StreamEvent("end", data={}),
    ])

    with patch.dict(os.environ, {"TRACER_OUTPUT_FORMAT": "text"}):
        renderer.render_stream(events)

    assert renderer.node_names_seen == ["extract_alert", "resolve_integrations"]
    assert renderer.stream_completed is True
