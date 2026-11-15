# syntax=docker/dockerfile:1

# Empty unless the build passes --build-context sdk=<path to synclet-go-sdk>, which
# builds against a local SDK checkout instead of the published module.
FROM scratch AS sdk

FROM golang:1.27-alpine AS builder

WORKDIR /build

COPY go.mod go.sum* ./
COPY --from=sdk / /sdk/
RUN if [ -f /sdk/go.mod ]; then go mod edit -replace github.com/synclet-io/synclet-go-sdk=/sdk && go mod tidy; fi \
 && go mod download

COPY . .
RUN if [ -f /sdk/go.mod ]; then go mod edit -replace github.com/synclet-io/synclet-go-sdk=/sdk && go mod tidy; fi
ARG TARGETOS TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -trimpath -ldflags "-s -w" -o /connector .

FROM alpine:3.24
RUN apk --no-cache add ca-certificates

COPY --from=builder /connector /connector

ENV AIRBYTE_ENTRYPOINT=/connector
ENTRYPOINT ["/connector"]
