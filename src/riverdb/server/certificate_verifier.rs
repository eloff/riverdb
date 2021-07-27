use std::time::SystemTime;
use std::sync::Arc;

use rustls::{ServerCertVerifier, ServerCertVerified, ServerName, Error, Certificate, ClientCertVerifier, DnsName, DistinguishedNames, ClientCertVerified};

pub struct DangerousCertificateNonverifier {}

impl DangerousCertificateNonverifier {
    pub fn new() -> Arc<Self> {
        Arc::new(Self{})
    }
}

impl ServerCertVerifier for DangerousCertificateNonverifier {
    fn verify_server_cert(&self, end_entity: &Certificate, intermediates: &[Certificate], server_name: &ServerName, scts: &mut dyn Iterator<Item=&[u8]>, ocsp_response: &[u8], now: SystemTime) -> Result<ServerCertVerified, Error> {
        Ok(ServerCertVerified::assertion())
    }
}

impl ClientCertVerifier for DangerousCertificateNonverifier {
    fn client_auth_root_subjects(&self, sni: Option<&DnsName>) -> Option<DistinguishedNames> {
        None
    }

    fn verify_client_cert(&self, end_entity: &Certificate, intermediates: &[Certificate], sni: Option<&DnsName>, now: SystemTime) -> Result<ClientCertVerified, Error> {
        Ok(ClientCertVerified::assertion())
    }
}