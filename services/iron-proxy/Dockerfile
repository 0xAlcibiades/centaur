FROM golang:1.26.1-alpine AS iron-proxy-builder

ARG IRON_PROXY_REPO=https://github.com/pseudolabel/iron-proxy.git
ARG IRON_PROXY_REF=4d28af9d47f2f4bb35eed9122852c8b90e4a6cce

RUN apk add --no-cache build-base git
RUN git init /src/iron-proxy \
    && cd /src/iron-proxy \
    && git remote add origin "$IRON_PROXY_REPO" \
    && git fetch --depth 1 origin "$IRON_PROXY_REF" \
    && git checkout FETCH_HEAD
WORKDIR /src/iron-proxy
RUN go build -o /out/iron-proxy ./cmd/iron-proxy

FROM ironsh/iron-proxy:0.39.0@sha256:a2e124f1266823f3d21ada5e3bd5611d85cdbb8ca8866047ff2b18ed122a274b

USER root
RUN apk add --no-cache curl jq
COPY --from=iron-proxy-builder /out/iron-proxy /usr/local/bin/iron-proxy

COPY services/iron-proxy/iron-proxy.yaml /usr/local/share/iron-proxy/proxy.yaml.default
COPY --chmod=755 services/iron-proxy/entrypoint.sh /entrypoint.sh

EXPOSE 8080

ENTRYPOINT ["/entrypoint.sh"]
