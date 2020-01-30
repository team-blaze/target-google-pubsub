.DEFAULT_GOAL := test
name = "target-google-pubsub"

build:
	docker build . -t $(name)

test: build
	docker run --rm --name $(name) -v $(PWD):/app $(name)
