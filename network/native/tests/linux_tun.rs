#![cfg(target_os = "linux")]
use skvoz_network_native::{TunDevice, duplicate_cloexec};
use std::io;
use std::os::fd::AsFd;

#[test]
#[ignore = "requires /dev/net/tun and CAP_NET_ADMIN in a disposable network namespace"]
fn real_tun_creation_adoption_and_atomic_packet_io() {
    let tun = TunDevice::create("skv-native-test", 1500).unwrap();
    assert_eq!(tun.name(), "skv-native-test");
    assert_eq!(tun.mtu(), 1500);
    let duplicate = duplicate_cloexec(tun.as_fd()).unwrap();
    let adopted = TunDevice::from_owned_fd(duplicate, 1500).unwrap();
    assert_eq!(adopted.name(), tun.name());
    assert!(TunDevice::create("skv-native-test", 1500).is_err());
    assert!(tun.try_write_packet(&[]).is_err());
    assert!(tun.try_write_packet(&[0; 1501]).is_err());
    assert!(tun.try_read_packet(&mut [0; 1500]).is_err());
    assert_eq!(
        tun.try_read_packet(&mut [0; 1501]).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert!(
        std::process::Command::new("ip")
            .args(["link", "set", "dev", tun.name(), "up"])
            .status()
            .unwrap()
            .success()
    );
    // Complete IPv4 packet; kernel may drop it by routing policy, but the device
    // write itself must transfer exactly one complete packet.
    let packet = [
        0x45, 0, 0, 20, 0, 0, 0, 0, 64, 143, 0x8d, 0x9f, 192, 0, 2, 1, 198, 51, 100, 1,
    ];
    tun.try_write_packet(&packet).unwrap();
    drop(adopted);
    drop(tun);
    assert!(!std::path::Path::new("/sys/class/net/skv-native-test").exists());
}
