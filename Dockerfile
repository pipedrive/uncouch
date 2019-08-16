FROM golang:alpine as stage1

RUN set -ex \
  && apk update \
  && apk add git alpine-sdk \
  && go get github.com/spf13/cobra \
  && go get github.com/golang/snappy \
  && go get go.uber.org/zap

RUN mkdir -p /usr/local/go/src/github.com/pipedrive/uncouch
ADD . /usr/local/go/src/github.com/pipedrive/uncouch
WORKDIR /usr/local/go/src/github.com/pipedrive/uncouch/uncouch-cli/
RUN go build -o uncouch-cli .

FROM golang:alpine as stage2

USER nobody
COPY --from=stage1 /usr/local/go/src/github.com/pipedrive/uncouch/uncouch-cli/uncouch-cli /app/
WORKDIR /app
CMD ["./uncouch-cli"]