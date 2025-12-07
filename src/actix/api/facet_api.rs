use actix_web::{Responder, post, web};
use actix_web_validator::{Json, Path, Query};
use api::rest::{FacetRequest, FacetResponse};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use storage::content_manager::collection_verification::check_strict_mode;
use storage::dispatcher::Dispatcher;
use tokio::time::Instant;

use crate::actix::api::CollectionPath;
use crate::actix::api::read_params::ReadParams;
use crate::actix::auth::{ActixAccess, ActixAccessWithMethod};
use crate::actix::helpers::{
    get_request_hardware_counter, log_audit_event, process_response, process_response_error,
};
use crate::actix::requester_context::ActixRequesterContext;
use crate::settings::ServiceConfig;
use crate::tracing::audit_event::Status;

#[post("/collections/{name}/facet")]
async fn facet(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<FacetRequest>,
    params: Query<ReadParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAccessWithMethod { access, auth_method }: ActixAccessWithMethod,
    ActixRequesterContext(requester): ActixRequesterContext,
) -> impl Responder {
    let timing = Instant::now();
    let name = collection.name.clone();

    log_audit_event(
        &auth_method,
        &requester,
        "facet",
        Status::Accepted,
        Some(name.clone()),
        None,
        None,
        None,
    );

    let FacetRequest {
        facet_request,
        shard_key,
    } = request.into_inner();

    let pass = match check_strict_mode(
        &facet_request,
        params.timeout_as_secs(),
        &name,
        &dispatcher,
        &access,
    )
    .await
    {
        Ok(pass) => pass,
        Err(err) => {
            log_audit_event(
                &auth_method,
                &requester,
                "facet",
                Status::Failure,
                Some(name),
                None,
                Some(timing.elapsed().as_millis() as i64),
                Some(format!("{}", err)),
            );
            return process_response_error(err, Instant::now(), None);
        }
    };

    let facet_params = From::from(facet_request);

    let shard_selection = match shard_key {
        None => ShardSelectorInternal::All,
        Some(shard_keys) => shard_keys.into(),
    };

    let request_hw_counter = get_request_hardware_counter(
        &dispatcher,
        name.clone(),
        service_config.hardware_reporting(),
        None,
    );

    let response = dispatcher
        .toc(&access, &pass)
        .facet(
            &name,
            facet_params,
            shard_selection,
            params.consistency,
            access,
            params.timeout(),
            request_hw_counter.get_counter(),
        )
        .await
        .map(FacetResponse::from);

    log_audit_event(
        &auth_method,
        &requester,
        "facet",
        if response.is_ok() { Status::Success } else { Status::Failure },
        Some(name),
        None,
        Some(timing.elapsed().as_millis() as i64),
        response.as_ref().err().map(|e| format!("{}", e)),
    );

    process_response(response, timing, request_hw_counter.to_rest_api())
}

pub fn config_facet_api(cfg: &mut web::ServiceConfig) {
    cfg.service(facet);
}
