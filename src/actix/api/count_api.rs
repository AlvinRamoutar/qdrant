use actix_web::{Responder, post, web};
use actix_web_validator::{Json, Path, Query};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::CountRequest;
use storage::content_manager::collection_verification::check_strict_mode;
use storage::dispatcher::Dispatcher;
use tokio::time::Instant;

use super::CollectionPath;
use crate::actix::api::read_params::ReadParams;
use crate::actix::auth::{ActixAccess, ActixAccessWithMethod};
use crate::actix::helpers::{self, get_request_hardware_counter, log_audit_event, process_response_error};
use crate::actix::requester_context::ActixRequesterContext;
use crate::common::query::do_count_points;
use crate::settings::ServiceConfig;
use crate::tracing::audit_event::Status;

#[post("/collections/{name}/points/count")]
async fn count_points(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<CountRequest>,
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
        "count_points",
        Status::Accepted,
        Some(name.clone()),
        None,
        None,
        None,
    );

    let CountRequest {
        count_request,
        shard_key,
    } = request.into_inner();

    let pass = match check_strict_mode(
        &count_request,
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
                "count_points",
                Status::Failure,
                Some(name),
                None,
                Some(overall_timing.elapsed().as_millis() as i64),
                Some(format!("{}", err)),
            );
            return process_response_error(err, Instant::now(), None);
        }
    };

    let shard_selector = match shard_key {
        None => ShardSelectorInternal::All,
        Some(shard_keys) => ShardSelectorInternal::from(shard_keys),
    };

    let request_hw_counter = get_request_hardware_counter(
        &dispatcher,
        name.clone(),
        service_config.hardware_reporting(),
        None,
    );

    let timing = Instant::now();

    let result = do_count_points(
        dispatcher.toc(&access, &pass),
        &name,
        count_request,
        params.consistency,
        params.timeout(),
        shard_selector,
        access,
        request_hw_counter.get_counter(),
    )
    .await;

    log_audit_event(
        &auth_method,
        &requester,
        "count_points",
        if result.is_ok() { Status::Success } else { Status::Failure },
        Some(name),
        None,
        Some(overall_timing.elapsed().as_millis() as i64),
        result.as_ref().err().map(|e| format!("{}", e)),
    );

    helpers::process_response(result, timing, request_hw_counter.to_rest_api())
}
