//! Explicit verified TLS identity when a trusted host dials a different address.
use crate::runtime::{RuntimeError, Trust};
use async_nats::rustls::{
    self, ClientConfig, DigitallySignedStruct, DistinguishedName, RootCertStore, SignatureScheme,
    client::{
        WebPkiServerVerifier,
        danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    },
    pki_types::{CertificateDer, ServerName, UnixTime, pem::PemObject},
};
use std::{fs::File, io::BufReader, sync::Arc};

#[derive(Debug)]
struct NamedVerifier {
    inner: Arc<WebPkiServerVerifier>,
    identity: ServerName<'static>,
}
impl ServerCertVerifier for NamedVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _dial_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        self.inner.verify_server_cert(
            end_entity,
            intermediates,
            &self.identity,
            ocsp_response,
            now,
        )
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
    fn requires_raw_public_keys(&self) -> bool {
        self.inner.requires_raw_public_keys()
    }
    fn root_hint_subjects(&self) -> Option<&[DistinguishedName]> {
        self.inner.root_hint_subjects()
    }
}

pub(crate) fn identity(name: &str) -> Result<ServerName<'static>, RuntimeError> {
    if name.is_empty()
        || name.len() > 253
        || !name.is_ascii()
        || name.contains(['%', '/', '[', ']'])
    {
        return Err(RuntimeError::Config);
    }
    ServerName::try_from(name.to_owned()).map_err(|_| RuntimeError::Config)
}

pub(crate) fn config(trust: &Trust, name: &str) -> Result<ClientConfig, RuntimeError> {
    let mut roots = RootCertStore::empty();
    match trust {
        Trust::System => {
            let result = rustls_native_certs::load_native_certs();
            if !result.errors.is_empty() {
                return Err(RuntimeError::Tls);
            }
            for cert in result.certs {
                roots.add(cert).map_err(|_| RuntimeError::Tls)?;
            }
        }
        Trust::ManagedCa(path) => {
            let file = File::open(path).map_err(|_| RuntimeError::Tls)?;
            for cert in CertificateDer::pem_reader_iter(&mut BufReader::new(file)) {
                roots
                    .add(cert.map_err(|_| RuntimeError::Tls)?)
                    .map_err(|_| RuntimeError::Tls)?;
            }
        }
    }
    if roots.is_empty() {
        return Err(RuntimeError::Tls);
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let verifier = WebPkiServerVerifier::builder_with_provider(Arc::new(roots), provider.clone())
        .build()
        .map_err(|_| RuntimeError::Tls)?;
    Ok(ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|_| RuntimeError::Tls)?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NamedVerifier {
            inner: verifier,
            identity: identity(name)?,
        }))
        .with_no_client_auth())
}
