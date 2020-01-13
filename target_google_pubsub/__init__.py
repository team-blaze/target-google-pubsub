#!/usr/bin/env python3
import argparse
import io
import sys
import json
import threading
import http.client
import urllib
import collections
from google.cloud import pubsub_v1 as pubsub
import pkg_resources
from jsonschema.validators import Draft4Validator
import singer
from google.auth import default as get_credentials

logger = singer.get_logger()

def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug("Emitting state {}".format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


def flatten(d, parent_key="", sep="__"):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, str(v) if type(v) is list else v))
    return dict(items)


def publisher(config):
    publisher = pubsub.PublisherClient()

    def publish(msg):
        topic = config.get("topic")

        if topic is None:
            topic = msg["stream"]

        future = publisher.publish(topic, data=json.dumps(msg))
        message_id = future.result()
        logger.info("{} successfully published".format(message_id))

    return publish


def persist_lines(config, lines):
    state = None
    schemas = {}
    key_properties = {}
    validators = {}

    publish = publisher(config)

    # Loop over lines from stdin
    for line in lines:
        try:
            o = json.loads(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if "type" not in o:
            raise Exception("Line is missing required key 'type': {}".format(line))
        t = o["type"]

        if t == "RECORD":
            if "stream" not in o:
                raise Exception(
                    "Line is missing required key 'stream': {}".format(line)
                )
            if o["stream"] not in schemas:
                raise Exception(
                    "A record for stream {} was encountered before a corresponding schema".format(
                        o["stream"]
                    )
                )

            # Validate record
            validators[o["stream"]].validate(o["record"])
            publish(o)
            state = None
        elif t == "STATE":
            logger.debug("Setting state to {}".format(o["value"]))
            # We don't need to forward state.
            # As this is a target.
            state = o["value"]
        elif t == "SCHEMA":
            if "stream" not in o:
                raise Exception(
                    "Line is missing required key 'stream': {}".format(line)
                )
            stream = o["stream"]
            schemas[stream] = o["schema"]

            validators[stream] = Draft4Validator(o["schema"])
            if "key_properties" not in o:
                raise Exception("key_properties field is required")
            key_properties[stream] = o["key_properties"]
            publish(o)
        else:
            raise Exception(
                "Unknown message type {} in message {}".format(o["type"], o)
            )

    return state


def send_usage_stats():
    try:
        version = pkg_resources.get_distribution("target-csv").version
        conn = http.client.HTTPConnection("collector.singer.io", timeout=10)
        conn.connect()
        params = {
            "e": "se",
            "aid": "singer",
            "se_ca": "target-google-pubsub",
            "se_ac": "open",
            "se_la": version,
        }
        conn.request("GET", "/i?" + urllib.parse.urlencode(params))
        conn.getresponse()
        conn.close()
    except:
        logger.debug("Collection request failed")


def sync(buf=sys.stdin.buffer):
    _, project_id = get_credentials()

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Config file")
    args = parser.parse_args()
    config = {"google_project_id": project_id}

    if args.config:
        with open(args.config) as input:
            config = config.update(json.load(input))

    if not config.get("disable_collection", False):
        logger.info(
            "Sending version information to singer.io. "
            + "To disable sending anonymous usage data, set "
            + 'the config parameter "disable_collection" to true'
        )
        threading.Thread(target=send_usage_stats).start()

    input = io.TextIOWrapper(buf, encoding="utf-8")
    state = persist_lines(config, input)
    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == "__main__":
    sync()
