FROM golang:alpine as builder

ENV CGO_ENABLED=0
ENV GOOS=linux
ENV GOARCH=amd64

RUN mkdir /app
WORKDIR /app

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . .

RUN go build -a -installsuffix cgo -o /go/bin/uncouch

FROM scratch

COPY --from=builder /go/bin/uncouch /go/bin/uncouch

ENTRYPOINT ["/go/bin/uncouch"]