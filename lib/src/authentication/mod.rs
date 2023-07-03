pub mod registry_based;


use std::borrow::Cow;
use crate::log_utils;


/// Authentication request source
#[derive(Debug, Clone, PartialEq)]
pub enum Source<'this> {
    /// A client tries to authenticate using SNI
    Sni(Cow<'this, str>),
    /// A client tries to authenticate using
    /// [the basic authentication scheme](https://datatracker.ietf.org/doc/html/rfc7617)
    ProxyBasic(Cow<'this, str>),
}

/// Authentication procedure status
#[derive(Clone, PartialEq)]
pub enum Status {
    /// Success
    Pass,
    /// Failure
    Reject,
}

/// The authenticator abstract interface
pub trait Authenticator: Send + Sync {
    /// Authenticate client
    fn authenticate(&self, source: &Source<'_>, log_id: &log_utils::IdChain<u64>) -> Status;
}

impl<'a> Source<'a> {
    pub fn into_owned(self) -> Source<'static> {
        match self {
            Source::Sni(x) => Source::Sni(Cow::Owned(x.into_owned())),
            Source::ProxyBasic(x) => Source::ProxyBasic(Cow::Owned(x.into_owned())),
        }
    }
}
