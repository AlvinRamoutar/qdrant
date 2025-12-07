use std::convert::Infallible;
use std::future::{Ready, ready};
use std::sync::Arc;

use actix_web::body::{BoxBody, EitherBody};
use actix_web::dev::{Service, ServiceRequest, ServiceResponse, Transform, forward_ready};
use actix_web::{Error, FromRequest, HttpMessage, HttpResponse, ResponseError};
use futures_util::future::LocalBoxFuture;
use storage::rbac::Access;

use super::helpers::HttpError;
use crate::common::auth::{AuthError, AuthKeys};

pub struct Auth {
    auth_keys: AuthKeys,
    whitelist: Vec<WhitelistItem>,
}

impl Auth {
    pub fn new(auth_keys: AuthKeys, whitelist: Vec<WhitelistItem>) -> Self {
        Self {
            auth_keys,
            whitelist,
        }
    }
}

impl<S, B> Transform<S, ServiceRequest> for Auth
where
    S: Service<ServiceRequest, Response = ServiceResponse<EitherBody<B, BoxBody>>, Error = Error>
        + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<EitherBody<B, BoxBody>>;
    type Error = Error;
    type InitError = ();
    type Transform = AuthMiddleware<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(AuthMiddleware {
            auth_keys: Arc::new(self.auth_keys.clone()),
            whitelist: self.whitelist.clone(),
            service: Arc::new(service),
        }))
    }
}

#[derive(Clone, Eq, PartialEq, Hash)]
pub struct WhitelistItem(pub String, pub PathMode);

impl WhitelistItem {
    pub fn exact<S: Into<String>>(path: S) -> Self {
        Self(path.into(), PathMode::Exact)
    }

    pub fn prefix<S: Into<String>>(path: S) -> Self {
        Self(path.into(), PathMode::Prefix)
    }

    pub fn matches(&self, other: &str) -> bool {
        self.1.check(&self.0, other)
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
pub enum PathMode {
    /// Path must match exactly
    Exact,
    /// Path must have given prefix
    Prefix,
}

impl PathMode {
    fn check(&self, key: &str, other: &str) -> bool {
        match self {
            Self::Exact => key == other,
            Self::Prefix => other.starts_with(key),
        }
    }
}

pub struct AuthMiddleware<S> {
    auth_keys: Arc<AuthKeys>,
    /// List of items whitelisted from authentication.
    whitelist: Vec<WhitelistItem>,
    service: Arc<S>,
}

impl<S> AuthMiddleware<S> {
    pub fn is_path_whitelisted(&self, path: &str) -> bool {
        self.whitelist.iter().any(|item| item.matches(path))
    }
}

impl<S, B> Service<ServiceRequest> for AuthMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<EitherBody<B, BoxBody>>, Error = Error>
        + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<EitherBody<B, BoxBody>>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let path = req.path();
        if self.is_path_whitelisted(path) {
            // For whitelisted paths, set AuthMethod::None
            req.extensions_mut().insert(AuthMethod::None);
            return Box::pin(self.service.call(req));
        }

        let auth_keys = self.auth_keys.clone();
        let service = self.service.clone();
        Box::pin(async move {
            // Determine auth method by checking which key/token was used
            let auth_method = determine_auth_method(&auth_keys, &req).await;
            
            match auth_keys
                .validate_request(|key| req.headers().get(key).and_then(|val| val.to_str().ok()))
                .await
            {
                Ok((access, inference_token)) => {
                    let previous = req.extensions_mut().insert::<Access>(access);
                    req.extensions_mut().insert(inference_token);
                    req.extensions_mut().insert(auth_method);
                    debug_assert!(
                        previous.is_none(),
                        "Previous access object should not exist in the request"
                    );
                    service.call(req).await
                }
                Err(e) => {
                    let resp = match e {
                        AuthError::Unauthorized(e) => HttpResponse::Unauthorized().body(e),
                        AuthError::Forbidden(e) => HttpResponse::Forbidden().body(e),
                        AuthError::StorageError(e) => HttpError::from(e).error_response(),
                    };
                    Ok(req.into_response(resp).map_into_right_body())
                }
            }
        })
    }
}

/// Helper function to determine which authentication method was used
async fn determine_auth_method(auth_keys: &AuthKeys, req: &ServiceRequest) -> AuthMethod {
    use crate::common::auth::HTTP_HEADER_API_KEY;
    use crate::common::strings::ct_eq;
    
    let key = req
        .headers()
        .get(HTTP_HEADER_API_KEY)
        .and_then(|v| v.to_str().ok())
        .or_else(|| {
            req.headers()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "))
        });

    match key {
        Some(key_value) => {
            // Check if it's the read-write API key
            if let Some(rw_key) = auth_keys.read_write_key() {
                if ct_eq(rw_key, key_value) {
                    return AuthMethod::ApiKey;
                }
            }
            
            // Check if it's the read-only API key
            if let Some(ro_key) = auth_keys.read_only_key() {
                if ct_eq(ro_key, key_value) {
                    return AuthMethod::ReadOnlyApiKey;
                }
            }
            
            // Check if it's a JWT token
            if let Some(jwt_parser) = auth_keys.jwt_parser() {
                if let Some(Ok(claims)) = jwt_parser.decode(key_value) {
                    return AuthMethod::Jwt(claims.sub);
                }
            }
            
            AuthMethod::None
        }
        None => AuthMethod::None,
    }
}

/// Authentication method used for the request
#[derive(Debug, Clone)]
pub enum AuthMethod {
    /// Authenticated with read-write API key
    ApiKey,
    /// Authenticated with read-only API key  
    ReadOnlyApiKey,
    /// Authenticated with JWT token
    Jwt(Option<String>), // Contains subject (sub) claim from JWT
    /// No authentication (when auth is disabled)
    None,
}

impl AuthMethod {
    /// Convert to audit log types: (AuthType, subject)
    pub fn to_audit_types(&self) -> (crate::tracing::audit_event::AuthType, Option<String>) {
        use crate::tracing::audit_event::AuthType;
        
        match self {
            AuthMethod::ApiKey => (AuthType::ApiKey, None),
            AuthMethod::ReadOnlyApiKey => (AuthType::ReadOnlyApiKey, None),
            AuthMethod::Jwt(subject) => (AuthType::Jwt, subject.clone()),
            AuthMethod::None => (AuthType::Anonymous, None),
        }
    }
}

pub struct ActixAccess(pub Access);

impl FromRequest for ActixAccess {
    type Error = Infallible;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(
        req: &actix_web::HttpRequest,
        _payload: &mut actix_web::dev::Payload,
    ) -> Self::Future {
        let access = req.extensions_mut().remove::<Access>().unwrap_or_else(|| {
            Access::full("All requests have full by default access when API key is not configured")
        });
        ready(Ok(ActixAccess(access)))
    }
}

/// Extractor that provides both Access and AuthMethod
///
/// This is used for audit logging to track both authorization and authentication details.
///
/// # Usage
/// ```rust
/// async fn my_handler(
///     ActixAccessWithMethod { access, auth_method }: ActixAccessWithMethod,
/// ) -> HttpResponse {
///     // Use access for authorization checks
///     // Use auth_method for audit logging
/// }
/// ```
pub struct ActixAccessWithMethod {
    pub access: Access,
    pub auth_method: AuthMethod,
}

impl FromRequest for ActixAccessWithMethod {
    type Error = Infallible;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(
        req: &actix_web::HttpRequest,
        _payload: &mut actix_web::dev::Payload,
    ) -> Self::Future {
        // Extract access (same as ActixAccess)
        let access = req.extensions().get::<Access>().cloned().unwrap_or_else(|| {
            Access::full("All requests have full by default access when API key is not configured")
        });

        // Extract auth method from request extensions
        let auth_method = req
            .extensions()
            .get::<AuthMethod>()
            .cloned()
            .unwrap_or(AuthMethod::None);

        ready(Ok(ActixAccessWithMethod {
            access,
            auth_method,
        }))
    }
}
