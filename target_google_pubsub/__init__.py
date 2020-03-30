#!/usr/bin/env python3
import argparse
import io
import sys
import json
import threading
import http.client
import urllib
import collections
import hashlib
import pkg_resources
import singer
import logging
from jsonschema.validators import Draft4Validator
from google.cloud import pubsub_v1 as pubsub
from google.auth import default as get_credentials

logger = singer.get_logger()


def emit_state(state):
    line = json.dumps(state)
    logger.info(f"Emitting state line: {line}")
    sys.stdout.write(f"{line}\n")
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
        stream = msg["stream"]
        topic = config.get("topic")

        if topic is None:
            topic = stream

        topic_path = publisher.topic_path(config.get("project_id"), topic)

        logger.debug("Actually publishing message")
        future = publisher.publish(topic_path, data=json.dumps(msg).encode("utf-8"), stream=stream)
        logger.debug("Waiting for future")
        message_id = future.result()
        logger.debug("Getting log message details")
        keys = msg.get("key_properties")
        values = "-".join(str(msg.get("record", {}).get(p)) for p in keys) if len(keys) else None
        extras = f" with key_properties '{keys}' and values '{values}'" if keys and values else ""
        logger.info(f"Message '{message_id}' successfully published on stream '{stream}'{extras}")

    return publish


def persist_lines(config, lines):
    state = ""
    schemas = {}
    schema_hashes = {}
    key_properties = {}
    bookmark_properties = {}
    validators = {}

    publish = publisher(config)

    # Loop over lines from stdin
    line_count = 0
    for line in lines:
        line_count += 1
        try:
            logger.debug(f"Parsing line {line_count}: {line}")
            o = json.loads(line)
        except json.decoder.JSONDecodeError:
            logger.error(f"Unable to parse JSON: {line}")
            raise

        logger.debug(f"Parsed line successfully - result is: {o}")

        if "type" not in o:
            raise Exception(f"Line is missing required key 'type': {line}")
        t = o["type"]

        if t == "RECORD":
            if "stream" not in o:
                raise Exception(f"Line is missing required key 'stream': {line}")
            if o["stream"] not in schemas:
                raise Exception(
                    f"A record for stream '{o['stream']}' was encountered before a schema"
                )

            # Don't validate record for now
            # validators[o["stream"]].validate(o["record"])
            logger.debug("Getting message details")
            msg = {
                "stream": o["stream"],
                "key_properties": key_properties[o["stream"]],
                "record": o["record"],
                "schema": schemas[o["stream"]],
                "schema_hash": schema_hashes[o["stream"]],
            }
            logger.debug("Checking bookmark properties")
            if o["stream"] in bookmark_properties:
                logger.debug("Assigning bookmark properties")
                msg["bookmark_properties"] = bookmark_properties[o["stream"]]

            logger.debug("About to publish message")
            publish(msg)

            state = ""
        elif t == "STATE":
            logger.debug(f"Setting state to: {o['value']}")
            state = o["value"]
            # We don't need to forward state as this is a target.
        elif t == "SCHEMA":
            if "stream" not in o:
                raise Exception(f"Line is missing required key 'stream': {line}")
            stream = o["stream"]
            schemas[stream] = o["schema"]
            schema_hashes[stream] = hashlib.sha256(line.encode("utf-8")).hexdigest()

            validators[stream] = Draft4Validator(o["schema"])
            if "key_properties" not in o:
                raise Exception("key_properties field is required")
            key_properties[stream] = o["key_properties"]
            if "bookmark_properties" in o:
                bookmark_properties[stream] = o["bookmark_properties"]
            # We don't publish this as it'll be bundled in each message
        else:
            logger.debug(f"Unknown message type '{o['type']}' in message: {o}")

    return state


def send_usage_stats():
    try:
        version = pkg_resources.get_distribution("target-google-pubsub").version
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
    except Exception as e:
        logger.info(f"Collection request failed with error: {e}")


def main(buf=sys.stdin.buffer):
    _, project_id = get_credentials()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c",
        "--config",
        help='Config file. At least "topic" must be specified in a JSON object format.',
        required=True,
    )
    parser.add_argument(
        "-l",
        "--loglevel",
        help="Log level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )
    args = parser.parse_args()

    logger.setLevel(getattr(logging, args.loglevel))
    # We need to override this to not get a tsunami...
    if args.loglevel == "DEBUG":
        logging.getLogger("google.cloud.pubsub_v1").setLevel(logging.INFO)

    config = {"project_id": project_id}
    with open(args.config) as input:
        config.update(json.load(input))

    if not config.get("disable_collection", False):
        logger.info(
            "Sending version information to singer.io. "
            + "To disable sending anonymous usage data, set "
            + 'the config parameter "disable_collection" to true'
        )
        threading.Thread(target=send_usage_stats).start()

    input = io.TextIOWrapper(buf, encoding="utf-8")

    try:
        state = persist_lines(config, input)
    except Exception as e:
        logger.error(f"Persisting lines failed with exception: {str(e)}")
        raise

    emit_state(state)
    logger.debug("Exiting normally")
    return state


if __name__ == "__main__":
    main()
