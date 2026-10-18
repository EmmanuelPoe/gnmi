//! rustls configuration for the tunnel server (device -> collector) and for the
//! gNMI client running inside each tunnel session (collector -> device).

use std::sync::Arc;

use anyhow::{Context, Result, bail};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, WebPkiSupportedAlgorithms};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::server::WebPkiClientVerifier;
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{DigitallySignedStruct, DistinguishedName, RootCertStore, SignatureScheme};

use crate::config::{ServerTlsConfig, Target};

const CLIENT_AUTH_MODES: [&str; 5] = ["", "request", "require", "verify-if-given", "require-verify"];

fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

pub fn validate_client_auth(mode: Option<&str>) -> Result<()> {
    let mode = mode.unwrap_or("");
    if !CLIENT_AUTH_MODES.contains(&mode) {
        bail!("unknown client-auth mode {mode:?}, expected one of {CLIENT_AUTH_MODES:?}");
    }
    Ok(())
}

fn load_certs(path: &str) -> Result<Vec<CertificateDer<'static>>> {
    let certs = CertificateDer::pem_file_iter(path)
        .and_then(|it| it.collect::<Result<Vec<_>, _>>())
        .with_context(|| format!("reading certificates from {path}"))?;
    if certs.is_empty() {
        bail!("no certificates found in {path}");
    }
    Ok(certs)
}

fn load_key(path: &str) -> Result<PrivateKeyDer<'static>> {
    PrivateKeyDer::from_pem_file(path).with_context(|| format!("reading private key from {path}"))
}

fn load_roots(path: &str) -> Result<RootCertStore> {
    let mut roots = RootCertStore::empty();
    for cert in load_certs(path)? {
        roots
            .add(cert)
            .with_context(|| format!("invalid CA certificate in {path}"))?;
    }
    Ok(roots)
}

/// Tunnel server TLS, with gnmic semantics: without cert/key files a
/// self-signed certificate is generated; `client-auth` defaults to
/// "require-verify" when a CA file is set and to no client auth otherwise.
pub fn server_config(cfg: &ServerTlsConfig) -> Result<Arc<rustls::ServerConfig>> {
    let (certs, key) = match (&cfg.cert_file, &cfg.key_file) {
        (Some(c), Some(k)) => (load_certs(c)?, load_key(k)?),
        _ => {
            let ck = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])?;
            tracing::warn!(
                "tunnel-server tls: no cert-file/key-file, using a generated self-signed certificate"
            );
            let key = PrivateKeyDer::try_from(ck.signing_key.serialize_der())
                .map_err(|e| anyhow::anyhow!("self-signed key: {e}"))?;
            (vec![ck.cert.der().clone()], key)
        }
    };
    let provider = provider();
    let ca = cfg.ca_file.as_deref().filter(|s| !s.is_empty());
    let mode = match cfg.client_auth.as_deref().unwrap_or("") {
        "" if ca.is_some() => "require-verify",
        m => m,
    };
    let builder = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()?;
    let verifier: Option<Arc<dyn ClientCertVerifier>> = match mode {
        "" => None,
        "request" | "require" => Some(Arc::new(AnyClientCert {
            mandatory: mode == "require",
            algs: provider.signature_verification_algorithms,
        })),
        "verify-if-given" | "require-verify" => {
            let ca = ca.with_context(|| format!("client-auth {mode:?} requires a ca-file"))?;
            let v = WebPkiClientVerifier::builder_with_provider(Arc::new(load_roots(ca)?), provider);
            let v = if mode == "verify-if-given" {
                v.allow_unauthenticated()
            } else {
                v
            };
            Some(v.build()?)
        }
        m => bail!("unknown client-auth mode {m:?}"),
    };
    let mut config = match verifier {
        Some(v) => builder.with_client_cert_verifier(v),
        None => builder.with_no_client_auth(),
    }
    .with_single_cert(certs, key)?;
    config.alpn_protocols = vec![b"h2".to_vec()];
    Ok(Arc::new(config))
}

/// The OS trust store, loaded once.
fn native_roots() -> Arc<RootCertStore> {
    static ROOTS: std::sync::OnceLock<Arc<RootCertStore>> = std::sync::OnceLock::new();
    ROOTS
        .get_or_init(|| {
            let mut roots = RootCertStore::empty();
            roots.add_parsable_certificates(rustls_native_certs::load_native_certs().certs);
            Arc::new(roots)
        })
        .clone()
}

/// Client-side TLS settings (gNMI targets, Postgres).
pub struct ClientTls<'a> {
    pub skip_verify: bool,
    pub ca: Option<&'a str>,
    pub cert: Option<&'a str>,
    pub key: Option<&'a str>,
    pub alpn_h2: bool,
}

impl<'a> ClientTls<'a> {
    pub fn for_target(t: &'a Target) -> Self {
        ClientTls {
            skip_verify: t.skip_verify,
            ca: t.tls_ca.as_deref(),
            cert: t.tls_cert.as_deref(),
            key: t.tls_key.as_deref(),
            alpn_h2: true,
        }
    }
}

pub fn client_config(o: &ClientTls) -> Result<Arc<rustls::ClientConfig>> {
    let provider = provider();
    let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()?;
    let builder = if o.skip_verify {
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoServerVerification(
                provider.signature_verification_algorithms,
            )))
    } else if let Some(ca) = o.ca {
        builder.with_root_certificates(load_roots(ca)?)
    } else {
        builder.with_root_certificates(native_roots())
    };
    let mut config = match (o.cert, o.key) {
        (Some(c), Some(k)) => builder.with_client_auth_cert(load_certs(c)?, load_key(k)?)?,
        _ => builder.with_no_client_auth(),
    };
    if o.alpn_h2 {
        config.alpn_protocols = vec![b"h2".to_vec()];
    }
    Ok(Arc::new(config))
}

/// SNI / verification name: tls-server-name, else the target name.
pub fn server_name(t: &Target) -> Result<ServerName<'static>> {
    let name = t.tls_server_name.as_deref().unwrap_or(&t.name);
    ServerName::try_from(name.to_string())
        .with_context(|| format!("target {:?}: invalid TLS server name {name:?}", t.name))
}

/// skip-verify: accept any server certificate (handshake signatures are still checked).
#[derive(Debug)]
struct NoServerVerification(WebPkiSupportedAlgorithms);

impl ServerCertVerifier for NoServerVerification {
    fn verify_server_cert(
        &self,
        _: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        _: &[u8],
        _: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.0)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.0)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_schemes()
    }
}

/// client-auth "request"/"require": ask for a client certificate without
/// validating its chain (Go's RequestClientCert / RequireAnyClientCert).
#[derive(Debug)]
struct AnyClientCert {
    mandatory: bool,
    algs: WebPkiSupportedAlgorithms,
}

impl ClientCertVerifier for AnyClientCert {
    fn client_auth_mandatory(&self) -> bool {
        self.mandatory
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        _: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.algs)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.algs)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algs.supported_schemes()
    }
}
