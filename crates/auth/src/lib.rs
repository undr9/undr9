use serde::{Deserialize, Serialize};
use undr9_common::{Result, Undr9Error};
use undr9_config::AuthConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    Reader,
    Writer,
    Admin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Action {
    Read,
    Write,
    Administer,
    Maintain,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Principal {
    pub name: String,
    pub role: Role,
}

pub struct Authorizer;
pub struct ApiKeyAuthenticator;

impl Authorizer {
    pub fn is_allowed(principal: Principal, action: Action) -> bool {
        matches!(
            (principal.role, action),
            (Role::Admin, _)
                | (Role::Writer, Action::Read | Action::Write)
                | (Role::Reader, Action::Read)
        )
    }
}

impl ApiKeyAuthenticator {
    pub fn authenticate(config: &AuthConfig, api_key: &str) -> Result<Principal> {
        let principal = if secure_compare(api_key, &config.admin_api_key) {
            Principal {
                name: config.bootstrap_admin_username.clone(),
                role: Role::Admin,
            }
        } else if secure_compare(api_key, &config.writer_api_key) {
            Principal {
                name: "writer".to_owned(),
                role: Role::Writer,
            }
        } else if secure_compare(api_key, &config.reader_api_key) {
            Principal {
                name: "reader".to_owned(),
                role: Role::Reader,
            }
        } else {
            return Err(Undr9Error::Validation("invalid API key".to_owned()));
        };

        Ok(principal)
    }
}

fn secure_compare(left: &str, right: &str) -> bool {
    let left_bytes = left.as_bytes();
    let right_bytes = right.as_bytes();
    let mut diff = left_bytes.len() ^ right_bytes.len();
    let max_len = left_bytes.len().max(right_bytes.len());

    for index in 0..max_len {
        let lhs = left_bytes.get(index).copied().unwrap_or(0);
        let rhs = right_bytes.get(index).copied().unwrap_or(0);
        diff |= usize::from(lhs ^ rhs);
    }

    diff == 0
}

#[cfg(test)]
mod tests {
    use super::{Action, ApiKeyAuthenticator, Authorizer, Principal, Role};
    use undr9_config::AppConfig;

    #[test]
    fn admin_can_perform_maintenance() {
        assert!(Authorizer::is_allowed(
            Principal {
                name: "admin".to_owned(),
                role: Role::Admin,
            },
            Action::Maintain
        ));
    }

    #[test]
    fn readers_cannot_write() {
        assert!(!Authorizer::is_allowed(
            Principal {
                name: "reader".to_owned(),
                role: Role::Reader,
            },
            Action::Write
        ));
    }

    #[test]
    fn authenticates_admin_api_key() {
        let config = AppConfig::default();
        let principal = ApiKeyAuthenticator::authenticate(&config.auth, &config.auth.admin_api_key)
            .expect("admin key should authenticate");

        assert_eq!(principal.role, Role::Admin);
        assert_eq!(principal.name, "admin");
    }

    #[test]
    fn rejects_similar_but_invalid_api_key() {
        let config = AppConfig::default();
        let mut invalid = config.auth.admin_api_key.clone();
        invalid.push('x');

        let error = ApiKeyAuthenticator::authenticate(&config.auth, &invalid)
            .expect_err("similar but invalid key should fail");
        assert!(error.to_string().contains("invalid API key"));
    }
}
