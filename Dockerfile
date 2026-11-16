# Pegasus service image: static Go binary on distroless, non-root.
#
# CGO is off, so the binary is fully static. The WhatsApp session store
# uses the pure-Go modernc.org/sqlite driver, so no CGO is needed for it.

FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/pegasus ./cmd/pegasus
# The WhatsApp session directory, owned by the runtime user (distroless
# nonroot = 65532) and 0700. A named volume mounted here on first use
# copies this ownership and mode, so the non-root process can write it.
RUN mkdir -p /out/var/lib/pegasus/whatsmeow && chmod 0700 /out/var/lib/pegasus/whatsmeow

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/pegasus /pegasus
COPY --from=build --chown=65532:65532 /out/var/lib/pegasus /var/lib/pegasus
USER nonroot:nonroot
EXPOSE 8095
# Exec form: the Go process is PID 1 and receives SIGTERM from
# `docker stop` directly (no shell in between to swallow it).
ENTRYPOINT ["/pegasus"]
