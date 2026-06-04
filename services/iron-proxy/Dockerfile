FROM ironsh/iron-proxy:0.42.0-rc.4@sha256:9989c5fde4ba3891e53942f08d2de33e690bf2734e5e451210fd845beb992e6f

USER root
RUN apk add --no-cache curl jq

COPY services/iron-proxy/iron-proxy.yaml /usr/local/share/iron-proxy/proxy.yaml.default
COPY --chmod=755 services/iron-proxy/entrypoint.sh /entrypoint.sh

EXPOSE 8080

ENTRYPOINT ["/entrypoint.sh"]
