use actix_web::{Responder, post, web};
use actix_web_validator::{Json, Path, Query};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::{DiscoverRequest, DiscoverRequestBatch};
use itertools::Itertools;
use storage::content_manager::collection_verification::{
    check_strict_mode, check_strict_mode_batch,
};
use storage::dispatcher::Dispatcher;
use tokio::time::Instant;

use crate::actix::api::CollectionPath;
use crate::actix::api::read_params::ReadParams;
use crate::actix::auth::{ActixAccess, ActixAccessWithMethod};
use crate::actix::helpers::{self, get_request_hardware_counter, log_audit_event, process_response_error};
use crate::actix::requester_context::ActixRequesterContext;
use crate::common::query::do_discover_batch_points;
use crate::settings::ServiceConfig;
use crate::tracing::audit_event::Status;

#[post("/collections/{name}/points/discover")]
async fn discover_points(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<DiscoverRequest>,
    params: Query<ReadParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAccessWithMethod { access, auth_method }: ActixAccessWithMethod,
    ActixRequesterContext(requester): ActixRequesterContext,
) -> impl Responder {
    let overall_timing = Instant::now();
    let name = collection.name.clone();

    log_audit_event(
        &auth_method,
        &requester,
        "discover_points",
        Status::Accepted,
        Some(name.clone()),
        None,
        None,
        None,
    );

    let DiscoverRequest {
        discover_request,
        shard_key,
    } = request.into_inner();

    let pass = match check_strict_mode(
        &discover_request,
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
                "discover_points",
                Status::Failure,
                Some(name),
                None,
                Some(overall_timing.elapsed().as_millis() as i64),
                Some(format!("{}", err)),
            );
            return process_response_error(err, Instant::now(), None);
        }
    };

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

    let timing = Instant::now();

    let result = dispatcher
        .toc(&access, &pass)
        .discover(
            &name,
            discover_request,
            params.consistency,
            shard_selection,
            access,
            params.timeout(),
            request_hw_counter.get_counter(),
        )
        .await
        .map(|scored_points| {
            scored_points
                .into_iter()
                .map(api::rest::ScoredPoint::from)
                .collect_vec()
        });

    log_audit_event(
        &auth_method,
        &requester,
        "discover_points",
        if result.is_ok() { Status::Success } else { Status::Failure },
        Some(name),
        None,
        Some(overall_timing.elapsed().as_millis() as i64),
        result.as_ref().err().map(|e| format!("{}", e)),
    );

    helpers::process_response(result, timing, request_hw_counter.to_rest_api())
}

#[post("/collections/{name}/points/discover/batch")]
async fn discover_batch_points(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<DiscoverRequestBatch>,
    params: Query<ReadParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAccessWithMethod { access, auth_method }: ActixAccessWithMethod,
    ActixRequesterContext(requester): ActixRequesterContext,
) -> impl Responder {
    let overall_timing = Instant::now();
    let name = collection.name.clone();

    log_audit_event(
        &auth_method,
        &requester,
        "discover_batch_points",
        Status::Accepted,
        Some(name.clone()),
        None,
        None,
        None,
    );

    let request = request.into_inner();

    let pass = match check_strict_mode_batch(
        request.searches.iter().map(|i| &i.discover_request),
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
                "discover_batch_points",
                Status::Failure,
                Some(name),
                None,
                Some(overall_timing.elapsed().as_millis() as i64),
                Some(format!("{}", err)),
            );
            return process_response_error(err, Instant::now(), None);
        }
    };

    let request_hw_counter = get_request_hardware_counter(
        &dispatcher,
        name.clone(),
        service_config.hardware_reporting(),
        None,
    );
    let timing = Instant::now();

    let result = do_discover_batch_points(
        dispatcher.toc(&access, &pass),
        &name,
        request,
        params.consistency,
        access,
        params.timeout(),
        request_hw_counter.get_counter(),
    )
    .await
    .map(|batch_scored_points| {
        batch_scored_points
            .into_iter()
            .map(|scored_points| {
                scored_points
                    .into_iter()
                    .map(api::rest::ScoredPoint::from)
                    .collect_vec()
            })
            .collect_vec()
    });

    log_audit_event(
        &auth_method,
        &requester,
        "discover_batch_points",
        if result.is_ok() { Status::Success } else { Status::Failure },
        Some(name),
        None,
        Some(overall_timing.elapsed().as_millis() as i64),
        result.as_ref().err().map(|e| format!("{}", e)),
    );

    helpers::process_response(result, timing, request_hw_counter.to_rest_api())
}

pub fn config_discovery_api(cfg: &mut web::ServiceConfig) {
    cfg.service(discover_points);
    cfg.service(discover_batch_points);
}
