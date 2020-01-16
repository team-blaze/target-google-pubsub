#!/usr/bin/env python3
import subprocess
from unittest.mock import Mock, call
from target_google_pubsub import main


def test_happy(monkeypatch):
    mock_publisher = Mock()
    monkeypatch.setattr(
        "google.cloud.pubsub_v1.PublisherClient.publish", mock_publisher
    )

    tap_ps = subprocess.Popen(
        ("cat", "./tests/dummy_messages.txt"), stdout=subprocess.PIPE
    )

    tap_ps.wait()
    state = main(tap_ps.stdout)
    assert mock_publisher.call_count == 5  # one for each line in dummy_messages.txt

    calls = [
        call(
            "projects/xyz/topics/users",
            data=b'{"type": "SCHEMA", "stream": "users", "key_properties": ["id"], "schema": {"required": ["id"], "type": "object", "properties": {"id": {"type": "integer"}}}}',
            stream="users",
        ),
        call().result(),
        call(
            "projects/xyz/topics/users",
            data=b'{"type": "RECORD", "stream": "users", "record": {"id": 1, "name": "Chris"}}',
            stream="users",
        ),
        call().result(),
        call(
            "projects/xyz/topics/users",
            data=b'{"type": "RECORD", "stream": "users", "record": {"id": 2, "name": "Mike"}}',
            stream="users",
        ),
        call().result(),
        call(
            "projects/xyz/topics/locations",
            data=b'{"type": "SCHEMA", "stream": "locations", "key_properties": ["id"], "schema": {"required": ["id"], "type": "object", "properties": {"id": {"type": "integer"}}}}',
            stream="locations",
        ),
        call().result(),
        call(
            "projects/xyz/topics/locations",
            data=b'{"type": "RECORD", "stream": "locations", "record": {"id": 1, "name": "Philadelphia"}}',
            stream="locations",
        ),
        call().result(),
    ]

    mock_publisher.assert_has_calls(calls)
    assert state == {"users": 2, "locations": 1}
