# syntax=docker/dockerfile:1@sha256:4edf897a3ffa55b89f906fc8cc78afdb3f1834cc9c7083565e611a8a7d5fe99e
FROM ruby:4.0.7-slim-bookworm@sha256:b21bc6c2360f7b7b615ff8860f239b12b2c6634ec026712745743b0661ac14b9 AS runtime-base
RUN apt-get update && apt-get install -y --no-install-recommends util-linux ca-certificates iproute2 nftables \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 skvoz && useradd --uid 10001 --gid 10001 --no-create-home skvoz \
    && install -d -m 0755 /var/lib/skvoz-client \
    && install -d -o 10001 -g 10001 -m 0700 /var/lib/skvoz-client/app /run/skvoz-client \
    && install -d -m 0700 /var/lib/skvoz-client/boot
WORKDIR /opt/skvoz/client
COPY clients/container/Gemfile clients/container/Gemfile.lock ./
COPY clients/container/lib lib
COPY clients/container/bin bin
RUN ln -s /opt/skvoz/client/bin/skvoz-client /usr/local/bin/skvoz-client
ENV BUNDLE_GEMFILE=/opt/skvoz/client/Gemfile BUNDLE_PATH=/usr/local/bundle BUNDLE_WITHOUT=test BUNDLE_FROZEN=true
HEALTHCHECK --interval=5s --timeout=3s --start-period=60s --retries=2 CMD ["skvoz-client", "health"]
ENTRYPOINT ["skvoz-client"]
CMD ["serve"]

# Optional packaging target for a previously source-built runtime and locked
# Ruby bundle. The supplied manifest binds every artifact to its source inputs.
# A legacy builder can stop here without executing the later source builds.
FROM runtime-base AS artifact-verify
COPY core /inputs/core
COPY network /inputs/network
COPY vendor/async-nats /inputs/vendor/async-nats
COPY Cargo.toml Cargo.lock /inputs/
COPY clients/container/artifact /artifact
COPY clients/container/tools/verify-artifact.rb /verify-artifact.rb
RUN ruby /verify-artifact.rb /artifact /inputs

FROM runtime-base AS artifact
COPY --from=artifact-verify /artifact/bundle /usr/local/bundle
COPY --from=artifact-verify /artifact/skvoz-network-runtime /usr/local/bin/skvoz-network-runtime
COPY --from=artifact-verify /artifact/native-source.json /opt/skvoz/client/native-source.json
COPY --from=artifact-verify /artifact/bundler.gem /tmp/bundler.gem
RUN gem install --local /tmp/bundler.gem --version 4.0.22 --no-document && rm /tmp/bundler.gem && bundle check && skvoz-client --version

FROM rust:1.92.0-bookworm@sha256:e90e846de4124376164ddfbaab4b0774c7bdeef5e738866295e5a90a34a307a2 AS native-source
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY clients/ubuntu/Cargo.toml clients/ubuntu/Cargo.toml
COPY clients/android/native/Cargo.toml clients/android/native/Cargo.toml
RUN mkdir -p clients/android/native/src clients/ubuntu/src/bin clients/ubuntu/tests \
    && touch clients/android/native/src/lib.rs clients/ubuntu/src/lib.rs clients/ubuntu/tests/child_guard.rs clients/ubuntu/tests/ui.rs \
    && printf 'fn main() {}\n' > clients/ubuntu/src/main.rs \
    && printf 'fn main() {}\n' > clients/ubuntu/src/bin/test_driver.rs
COPY vendor/async-nats vendor/async-nats
COPY core core
COPY daemon daemon
COPY network network
COPY connectors/tcp connectors/tcp
COPY testbench testbench
RUN CARGO_BUILD_JOBS=1 cargo build --release --locked -p skvoz-network --features linux-runtime --bin skvoz-network-runtime

FROM ruby:4.0.7-bookworm@sha256:119a36c51c7893215202220eabfdcb561638735de4d45bacf2e8619be3de47f2 AS gems
WORKDIR /opt/skvoz/client
ENV BUNDLE_PATH=/usr/local/bundle
COPY clients/container/Gemfile clients/container/Gemfile.lock ./
RUN gem install bundler --version 4.0.22 --no-document \
    && bundle config set deployment true && bundle config set without test \
    && bundle install && bundle clean --force

FROM gems AS tests
COPY clients/container/lib lib
COPY clients/container/spec spec
RUN bundle config unset without && bundle install
CMD ["bundle", "exec", "rspec", "spec"]

FROM runtime-base AS deploy
COPY --from=gems /usr/local/bundle /usr/local/bundle
COPY --from=native-source /src/target/release/skvoz-network-runtime /usr/local/bin/skvoz-network-runtime
RUN bundle check && skvoz-client --version
