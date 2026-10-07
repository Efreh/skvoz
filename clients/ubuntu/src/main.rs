fn main() -> gtk::glib::ExitCode {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.first().is_some_and(|arg| arg == "--child") {
        return if skvoz_ubuntu_client::backend::child_guard(&args).is_ok() {
            gtk::glib::ExitCode::SUCCESS
        } else {
            gtk::glib::ExitCode::FAILURE
        };
    }
    if args == ["--version"] {
        println!(
            "skvoz-client {} runtime={} network=4 api=1",
            env!("CARGO_PKG_VERSION"),
            skvoz_ubuntu_client::RUNTIME_VERSION
        );
        return gtk::glib::ExitCode::SUCCESS;
    }
    skvoz_ubuntu_client::app::run()
}
