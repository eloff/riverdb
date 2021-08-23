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
    fn verify_server_cert(&self, _end_entity: &Certificate, _intermediates: &[Certificate], _server_name: &ServerName, _scts: &mut dyn Iterator<Item=&[u8]>, _ocsp_response: &[u8], _now: SystemTime) -> Result<ServerCertVerified, Error> {
        Ok(ServerCertVerified::assertion())
    }
}

impl ClientCertVerifier for DangerousCertificateNonverifier {
    fn client_auth_root_subjects(&self, _sni: Option<&DnsName>) -> Option<DistinguishedNames> {
        None
    }

    fn verify_client_cert(&self, _end_entity: &Certificate, _intermediates: &[Certificate], _sni: Option<&DnsName>, _now: SystemTime) -> Result<ClientCertVerified, Error> {
        Ok(ClientCertVerified::assertion())
    }
}