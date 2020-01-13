#!/usr/bin/env python3
import subprocess
from unittest.mock import Mock, call
from target_google_pubsub import sync 


def test_basic(monkeypatch):
    mock_publisher = Mock()
    monkeypatch.setattr("google.cloud.pubsub_v1.PublisherClient.publish", mock_publisher)

    tap_ps = subprocess.Popen(
        (
            "cat",
            "./tests/dummy_messages.txt",
        ),
        stdout=subprocess.PIPE,
    )

    tap_ps.wait()
    sync(tap_ps.stdout)
    assert mock_publisher.call_count == 5 # one for each line in dummy_messages.txt
    
    calls = [call('users', data='{"type": "SCHEMA", "stream": "users", "key_properties": ["id"], "schema": {"required": ["id"], "type": "object", "properties": {"id": {"type": "integer"}}}}'),
 call().result(),
 call('users', data='{"type": "RECORD", "stream": "users", "record": {"id": 1, "name": "Chris"}}'),
 call().result(),
 call('users', data='{"type": "RECORD", "stream": "users", "record": {"id": 2, "name": "Mike"}}'),
 call().result(),
 call('locations', data='{"type": "SCHEMA", "stream": "locations", "key_properties": ["id"], "schema": {"required": ["id"], "type": "object", "properties": {"id": {"type": "integer"}}}}'),
 call().result(),
 call('locations', data='{"type": "RECORD", "stream": "locations", "record": {"id": 1, "name": "Philadelphia"}}'),
 call().result()]

    mock_publisher.assert_has_calls(calls)
