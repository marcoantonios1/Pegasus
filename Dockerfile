# Pegasus service image: static Go binary on distroless, non-root.
#
# CGO is off, so the binary is fully static. NOTE: the future Whatsmeow
# live source will need a session store; whatsmeow's sqlstore with
# mattn/go-sqlite3 needs CGO (and then a libc-bearing runtime image).
# Not solved here: revisit (or pick a pure-Go sqlite driver / the
# Postgres store) in that issue.

FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/pegasus ./cmd/pegasus

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/pegasus /pegasus
USER nonroot:nonroot
EXPOSE 8095
# Exec form: the Go process is PID 1 and receives SIGTERM from
# `docker stop` directly (no shell in between to swallow it).
ENTRYPOINT ["/pegasus"]
