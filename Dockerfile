# ---- Stage 1: build ----------------------------------------------------
# Full Go toolchain lives here and is DISCARDED after the build; only the
# compiled binary is carried into the final image.
FROM golang:1.25 AS build
WORKDIR /src

# Copy module files first so the (slow) dependency download is cached as its
# own layer and only re-runs when go.mod/go.sum change, not on every code edit.
COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY raft ./raft

# CGO_ENABLED=0 -> fully static binary with no libc dependency, which is what
# lets the runtime stage be an image with no OS userland at all.
# -trimpath/-s -w strip local paths and debug info for a smaller binary.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" \
    -o /out/flotillanode ./cmd/flotillanode

# Create the data dir here, because the runtime image has no shell (no mkdir).
RUN mkdir /data

# ---- Stage 2: runtime --------------------------------------------------
# distroless/static: no shell, no package manager, runs as non-root (uid 65532).
FROM gcr.io/distroless/static:nonroot

COPY --from=build /out/flotillanode /flotillanode

# BoltDB (<id>.db) is written to the working directory. We make that /data and
# chown it to the non-root user, otherwise the process can't create the file.
# In Kubernetes (A3) a PersistentVolumeClaim gets mounted over this path, and
# volume ownership becomes a pod-level concern (fsGroup) instead.
COPY --from=build --chown=65532:65532 /data /data
WORKDIR /data
VOLUME /data

EXPOSE 8001
ENTRYPOINT ["/flotillanode"]
# Default listens on all interfaces, not just localhost: inside a container,
# ":8001" is what makes the port reachable from outside the container.
CMD ["-id=node1", "-addr=:8001"]
