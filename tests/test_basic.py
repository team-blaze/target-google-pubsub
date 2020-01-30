#!/usr/bin/env python3
import subprocess
from unittest.mock import Mock, call
from target_google_pubsub import main


def test_happy(monkeypatch):
    mock_publisher = Mock()
    monkeypatch.setattr("google.cloud.pubsub_v1.PublisherClient.publish", mock_publisher)

    tap_ps = subprocess.Popen(("cat", "./tests/dummy_messages.txt"), stdout=subprocess.PIPE)

    tap_ps.wait()
    state = main(tap_ps.stdout)
    assert mock_publisher.call_count == 3  # one for each record in dummy_messages.txt

    calls = [
        call(
            "projects/xyz/topics/users",
            data=b'{"stream": "users", "key_properties": ["id"], "record": {"id": 1, "name": "Chris"}, "schema": {"required": ["id"], "type": "object", "properties": {"id": {"type": "integer"}}}, "schema_hash": "45111a2c9a8c755247802e236dd5e26775228872b081ac480b4d04f0d3b3748d"}',  # noqa: E501
            stream="users",
        ),
        call().result(),
        call(
            "projects/xyz/topics/users",
            data=b'{"stream": "users", "key_properties": ["id"], "record": {"id": 2, "name": "Mike"}, "schema": {"required": ["id"], "type": "object", "properties": {"id": {"type": "integer"}}}, "schema_hash": "45111a2c9a8c755247802e236dd5e26775228872b081ac480b4d04f0d3b3748d"}',  # noqa: E501
            stream="users",
        ),
        call().result(),
        call(
            "projects/xyz/topics/locations",
            data=b'{"stream": "locations", "key_properties": ["id"], "record": {"id": 1, "name": "Philadelphia"}, "schema": {"required": ["id"], "type": "object", "properties": {"id": {"type": "integer"}}}, "schema_hash": "7198513b001d6010cc1258d71c2d064d0e4ffae5d7e336afe6e2d5a8a9723d56", "bookmark_properties": ["id"]}',  # noqa: E501
            stream="locations",
        ),
        call().result(),
    ]

    mock_publisher.assert_has_calls(calls)
    assert state == {"locations": [1], "users": [2]}
