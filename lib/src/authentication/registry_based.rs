use std::borrow::Cow;
use std::collections::HashSet;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_ENGINE;
use crate::{authentication, log_utils};
use crate::authentication::Authenticator;
use serde::Deserialize;


/// A client descriptor
#[derive(Deserialize)]
pub struct Client {
    /// The client username
    pub username: String,
    /// The client password
    pub password: String,
}

/// The [`Authenticator`] implementation which checks presence of a client in the list.
/// Is only able to authenticate a client using the Proxy basic authorization.
pub struct RegistryBasedAuthenticator {
    clients: HashSet<Cow<'static, str>>,
}

impl RegistryBasedAuthenticator {
    pub fn new(clients: &Vec<Client>) -> Self
    {
        Self {
            clients: clients.iter()
                .map(|x| BASE64_ENGINE.encode(format!("{}:{}", x.username, x.password)))
                .map(Cow::Owned)
                .collect(),
        }
    }
}

impl Authenticator for RegistryBasedAuthenticator {
    fn authenticate(
        &self, source: &authentication::Source<'_>, _log_id: &log_utils::IdChain<u64>,
    ) -> authentication::Status {
        match &source {
            authentication::Source::ProxyBasic(str) if self.clients.contains(str) =>
                authentication::Status::Pass,
            _ => authentication::Status::Reject,
        }
    }
}
