.DEFAULT_GOAL := test
name = "target-google-pubsub"

build:
	docker build . -t	$(name)

test: build
	docker run -v $(PWD):/app $(name)
