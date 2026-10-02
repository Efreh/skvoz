# syntax=docker/dockerfile:1
FROM rust:1.92.0-bookworm@sha256:e90e846de4124376164ddfbaab4b0774c7bdeef5e738866295e5a90a34a307a2 AS core-builder
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY core core
COPY daemon daemon
COPY connectors/tcp connectors/tcp
COPY testbench testbench
RUN cargo build --release --locked -p skvoz-daemon

FROM nats:2.15.0-alpine@sha256:ac8f88a6494bffc2c2a5289a0ca61cb28a9145c11ba5677cf24265d07f46d8d4 AS broker
FROM ghcr.io/letsencrypt/pebble:2.10.1@sha256:ddf230642b1a584f519f32e347de1b05a6e4c1f6c35c1863b33effeab5f78199 AS acme-test

FROM ruby:4.0.7-bookworm@sha256:119a36c51c7893215202220eabfdcb561638735de4d45bacf2e8619be3de47f2 AS base
WORKDIR /opt/skvoz/server
COPY connectors/server/Gemfile connectors/server/Gemfile.lock ./
RUN gem install bundler --version 4.0.22 --no-document \
    && bundle config set deployment true \
    && bundle config set without test \
    && bundle install \
    && bundle clean --force \
    && rm -rf /usr/local/bundle/cache
COPY connectors/server/lib lib
COPY connectors/server/bin bin

FROM base AS tests
RUN apt-get update && apt-get install -y --no-install-recommends util-linux \
    && rm -rf /var/lib/apt/lists/* \
    && bundle config unset without && bundle install
COPY --from=core-builder /src/target/release/skvoz-core-daemon /usr/local/bin/skvoz-core-daemon
COPY --from=broker /usr/local/bin/nats-server /usr/local/bin/nats-server
COPY --from=acme-test /app /usr/local/bin/pebble
COPY connectors/server/spec spec
COPY daemon/tests/fixtures /opt/daemon/tests/fixtures
COPY clients/ruby /opt/clients/ruby
ENV SKVOZ_INTEGRATION=1
CMD ["bundle", "exec", "rspec", "spec"]

FROM ruby:4.0.7-slim-bookworm@sha256:b21bc6c2360f7b7b615ff8860f239b12b2c6634ec026712745743b0661ac14b9 AS deploy
RUN apt-get update && apt-get install -y --no-install-recommends util-linux ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 skvoz && useradd --uid 10001 --gid 10001 --no-create-home skvoz \
    && install -d -o skvoz -g skvoz -m 0700 /var/lib/skvoz
WORKDIR /opt/skvoz/server
COPY --from=base /usr/local/bundle /usr/local/bundle
COPY --from=base /opt/skvoz/server /opt/skvoz/server
COPY --from=core-builder /src/target/release/skvoz-core-daemon /usr/local/bin/skvoz-core-daemon
COPY --from=broker /usr/local/bin/nats-server /usr/local/bin/nats-server
RUN ln -s /opt/skvoz/server/bin/skvoz-server /usr/local/bin/skvoz-server
ENV BUNDLE_GEMFILE=/opt/skvoz/server/Gemfile SKVOZ_STATE_DIR=/var/lib/skvoz
USER 10001:10001
EXPOSE 4222 8080
VOLUME ["/var/lib/skvoz"]
HEALTHCHECK --interval=30s --timeout=5s --start-period=180s --retries=3 CMD ["skvoz-server", "health"]
ENTRYPOINT ["skvoz-server"]
CMD ["serve"]
