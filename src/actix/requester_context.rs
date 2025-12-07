use std::convert::Infallible;
use std::future::{Ready, ready};

use actix_web::dev::{Service, ServiceRequest, ServiceResponse, Transform, forward_ready};
use actix_web::{Error, FromRequest, HttpMessage};
use futures_util::future::LocalBoxFuture;

/// Connection information about the requester
#[derive(Debug, Clone)]
pub struct RequesterContext {
    /// Remote IP address of the client (considers proxy headers)
    pub remote_addr: String,
    /// Local IP address of the server handling the request
    pub local_addr: String,
}

impl RequesterContext {
    pub fn new(remote_addr: String, local_addr: String) -> Self {
        Self {
            remote_addr,
            local_addr,
        }
    }
}

/// Middleware that extracts connection information and stores it in request extensions
pub struct RequesterMiddleware;

impl<S, B> Transform<S, ServiceRequest> for RequesterMiddleware
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type InitError = ();
    type Transform = RequesterMiddlewareService<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(RequesterMiddlewareService { service }))
    }
}

pub struct RequesterMiddlewareService<S> {
    service: S,
}

impl<S, B> Service<ServiceRequest> for RequesterMiddlewareService<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        // Extract remote address (considering proxy headers)
        let connection_info = req.connection_info();
        let remote_addr = connection_info
            .realip_remote_addr()
            .unwrap_or("unknown")
            .to_string();

        // Extract local address
        let local_addr = req
            .connection_info()
            .host()
            .to_string();

        // Create context and store in request extensions
        let context = RequesterContext::new(remote_addr, local_addr);
        req.extensions_mut().insert(context);

        let fut = self.service.call(req);
        Box::pin(async move {
            let res = fut.await?;
            Ok(res)
        })
    }
}

/// Extractor for RequesterContext
/// 
/// Usage in handlers:
/// ```rust
/// async fn my_handler(
///     ActixRequesterContext(context): ActixRequesterContext,
/// ) -> HttpResponse {
///     println!("Remote: {}, Local: {}", context.remote_addr, context.local_addr);
///     // ...
/// }
/// ```
pub struct ActixRequesterContext(pub RequesterContext);

impl FromRequest for ActixRequesterContext {
    type Error = Infallible;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(
        req: &actix_web::HttpRequest,
        _payload: &mut actix_web::dev::Payload,
    ) -> Self::Future {
        let context = req
            .extensions()
            .get::<RequesterContext>()
            .cloned()
            .unwrap_or_else(|| {
                // Fallback if middleware is not configured
                RequesterContext::new("unknown".to_string(), "unknown".to_string())
            });
        ready(Ok(ActixRequesterContext(context)))
    }
}