FROM ubuntu:24.04@sha256:a853f94d226358a79c740cfc7bce0c289748f3fe3488d921d038ccd752c61b60
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    python3 iproute2 iputils-ping nftables conntrack ethtool ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY traffic.py /traffic.py
CMD ["sleep", "infinity"]
