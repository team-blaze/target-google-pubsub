# target-google-pubsub

This is a [Singer](https://singer.io) target that reads JSON-formatted data
following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md) and puts `RECORD` & `SCHEMA` messages into a google-pubsub topic.

The topic is either the `stream` name. Or can be set to a single topic by adjusting the `config.topic` property. See: [/sample_config.json](/sample_config.json)
