FROM nats:2.15.0-alpine@sha256:ac8f88a6494bffc2c2a5289a0ca61cb28a9145c11ba5677cf24265d07f46d8d4 AS broker
FROM ubuntu:24.04@sha256:a853f94d226358a79c740cfc7bce0c289748f3fe3488d921d038ccd752c61b60
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    python3 iproute2 iputils-ping nftables conntrack ca-certificates util-linux \
    dbus polkitd systemd-resolved \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 skvoz && useradd --uid 10001 --gid 10001 --no-create-home skvoz
COPY --from=broker /usr/local/bin/nats-server /usr/local/bin/nats-server
COPY network/helper/polkit/org.skvoz.network.policy /usr/share/polkit-1/actions/org.skvoz.network.policy
# Only this test image authorizes the fixture UID without an interactive agent.
RUN chmod 0644 /usr/share/polkit-1/actions/org.skvoz.network.policy \
    && printf '%s\n' 'polkit.addRule(function(action, subject) { if (action.id == "org.skvoz.network.manage" && subject.user == "skvoz") return polkit.Result.YES; });' > /etc/polkit-1/rules.d/49-skvoz-fixture.rules
CMD ["sleep", "infinity"]
