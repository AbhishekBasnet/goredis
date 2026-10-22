FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/goredis ./cmd/goredis && mkdir /out/data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/goredis /goredis
COPY --from=build --chown=nonroot:nonroot /out/data /data
VOLUME /data
EXPOSE 6380
ENTRYPOINT ["/goredis", "-addr", ":6380", "-dir", "/data"]
