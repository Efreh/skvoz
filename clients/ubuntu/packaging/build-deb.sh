#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
version=$(sed -n 's/^version = "\([0-9][0-9.]*\)"$/\1/p' "$root/clients/ubuntu/Cargo.toml")
[ -n "$version" ] || { echo 'Invalid package version' >&2; exit 1; }
binary=${1:-"$root/target/release/skvoz-client"}
daemon=${2:-"$root/target/release/skvoz-core-daemon"}
output=${3:-"$root/clients/ubuntu/dist"}
[ "$(dpkg --print-architecture)" = amd64 ] || { echo 'Only amd64 packages are supported' >&2; exit 1; }
[ "$("$daemon" --version)" = 'skvoz-core-daemon 1.2.0 ipc=1' ] || { echo 'Incompatible bundled daemon' >&2; exit 1; }
[ "$("$binary" --version)" = "skvoz-client $version daemon=1.2.0 ipc=1" ] || { echo 'Incompatible client version' >&2; exit 1; }
mkdir -p "$output"
stage=$(mktemp -d)
chmod 755 "$stage"
trap 'rm -rf "$stage"' EXIT HUP INT TERM
install -d "$stage/DEBIAN" "$stage/usr/lib/skvoz-client" "$stage/usr/bin" "$stage/usr/share/applications" "$stage/usr/share/icons/hicolor/scalable/apps" "$stage/usr/share/doc/skvoz-client"
install -m 755 "$daemon" "$stage/usr/lib/skvoz-client/skvoz-core-daemon"
install -m 755 "$binary" "$stage/usr/lib/skvoz-client/skvoz-client"
ln -s ../lib/skvoz-client/skvoz-client "$stage/usr/bin/skvoz-client"
install -m 644 "$root/clients/ubuntu/packaging/org.skvoz.Client.desktop" "$stage/usr/share/applications/"
install -m 644 "$root/clients/ubuntu/packaging/org.skvoz.Client.svg" "$stage/usr/share/icons/hicolor/scalable/apps/"
install -m 644 "$root/LICENSE" "$stage/usr/share/doc/skvoz-client/copyright"
cat > "$stage/DEBIAN/control" <<CONTROL
Package: skvoz-client
Version: $version
Architecture: amd64
Maintainer: SKVOZ contributors
Depends: libgtk-4-1 (>= 4.14), libadwaita-1-0 (>= 1.5), libglib2.0-0t64 (>= 2.80), libpango-1.0-0, libgdk-pixbuf-2.0-0, libcairo2, libgraphene-1.0-0, adwaita-icon-theme, librsvg2-common, ca-certificates, util-linux, procps, libc6 (>= 2.39), libgcc-s1
Section: net
Priority: optional
Description: SKVOZ desktop TCP proxy for Ubuntu 24.04 and later
 Native GTK4 interface, local HTTP CONNECT and SOCKS5 proxy,
 and a bundled universal Rust Core daemon over authenticated TLS NATS.
CONTROL
dpkg-deb --root-owner-group --build "$stage" "$output/skvoz-client_${version}_amd64.deb"
(cd "$output" && sha256sum "skvoz-client_${version}_amd64.deb" > "skvoz-client_${version}_amd64.deb.sha256")
