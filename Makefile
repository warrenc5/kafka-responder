.PHONY: build 

all:
	docker compose up --build
build:
	docker build -t responder --network host .
run:
	docker run -it --network host responder
enter:
	docker run -it --entrypoint bash --network host responder
run-base:
	docker run -it --network host docker.io/adoptopenjdk/openjdk8
