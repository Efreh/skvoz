//! Ubuntu error adapter for the shared enrollment transport.
pub use skvoz_network::enrollment::{Credentials, Enrollment, RuntimeVersion};
pub async fn enroll(credentials: &Credentials, device: &str) -> crate::Result<Enrollment> {
    skvoz_network::enrollment::enroll(credentials, device)
        .await
        .map_err(|e| crate::Error(e.0))
}
