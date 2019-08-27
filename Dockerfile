FROM ubuntu:bionic as stage1

ENV GOPATH /usr/local/go

RUN set -ex \
  && apt update && apt upgrade -y \
  && apt install golang -y \
  && apt install git-core -y \
  && go get github.com/spf13/cobra \
  && go get github.com/golang/snappy \
  && go get go.uber.org/zap

RUN mkdir -p /usr/local/go/src/github.com/pipedrive/uncouch
ADD . /usr/local/go/src/github.com/pipedrive/uncouch
WORKDIR /usr/local/go/src/github.com/pipedrive/uncouch/uncouch-cli/
RUN CGO_ENABLED=0 GOOS=linux go build -o uncouch-cli .

FROM scratch as stage2

COPY --from=stage1 /usr/local/go/src/github.com/pipedrive/uncouch/uncouch-cli/uncouch-cli /app/
WORKDIR /app

ENTRYPOINT ["./uncouch-cli"]