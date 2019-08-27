REGISTRY=docker.io
PROJECT=pipedrive
IMAGE_NAME=uncouch

all: build push

build:
	docker build -t ${REGISTRY}/${PROJECT}/${IMAGE_NAME} .

push: build
	docker push ${REGISTRY}/${PROJECT}/${IMAGE_NAME}

.PHONY: all build push
