use std::time::Duration;

use actix_web::{Responder, post, web};
use actix_web_validator::{Json, Path, Query};
use collection::operations::consistency_params::ReadConsistency;
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::{
    RecommendGroupsRequest, RecommendRequest, RecommendRequestBatch,
};
use common::counter::hardware_accumulator::HwMeasurementAcc;
use itertools::Itertools;
use segment::types::ScoredPoint;
use storage::content_manager::collection_verification::{
    check_strict_mode, check_strict_mode_batch,
};
use storage::content_manager::errors::StorageError;
use storage::content_manager::toc::TableOfContent;
use storage::dispatcher::Dispatcher;
use storage::rbac::Access;
use tokio::time::Instant;

use super::CollectionPath;
use super::read_params::ReadParams;
use crate::actix::auth::{ActixAccess, ActixAccessWithMethod};
use crate::actix::helpers::{self, get_request_hardware_counter, log_audit_event, process_response_error};
use crate::actix::requester_context::ActixRequesterContext;
use crate::settings::ServiceConfig;
use crate::tracing::audit_event::Status;

#[post("/collections/{name}/points/recommend")]
async fn recommend_points(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<RecommendRequest>,
    params: Query<ReadParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAccessWithMethod { access, auth_method }: ActixAccessWithMethod,
    ActixRequesterContext(requester): ActixRequesterContext,
) -> impl Responder {
    let overall_timing = Instant::now();
    let collection_name = collection.name.clone();

    log_audit_event(
        &auth_method,
        &requester,
        "recommend_points",
        Status::Accepted,
        Some(collection_name.clone()),
        None,
        None,
        None,
    );

    let RecommendRequest {
        recommend_request,
        shard_key,
    } = request.into_inner();

    let pass = match check_strict_mode(
        &recommend_request,
        params.timeout_as_secs(),
        &collection_name,
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
                "recommend_points",
                Status::Failure,
                Some(collection_name),
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
        collection_name.clone(),
        service_config.hardware_reporting(),
        None,
    );

    let timing = Instant::now();

    let result = dispatcher
        .toc(&access, &pass)
        .recommend(
            &collection_name,
            recommend_request,
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
        "recommend_points",
        if result.is_ok() { Status::Success } else { Status::Failure },
        Some(collection_name),
        None,
        Some(overall_timing.elapsed().as_millis() as i64),
        result.as_ref().err().map(|e| format!("{}", e)),
    );

    helpers::process_response(result, timing, request_hw_counter.to_rest_api())
}

async fn do_recommend_batch_points(
    toc: &TableOfContent,
    collection_name: &str,
    request: RecommendRequestBatch,
    read_consistency: Option<ReadConsistency>,
    access: Access,
    timeout: Option<Duration>,
    hw_measurement_acc: HwMeasurementAcc,
) -> Result<Vec<Vec<ScoredPoint>>, StorageError> {
    let requests = request
        .searches
        .into_iter()
        .map(|req| {
            let shard_selector = match req.shard_key {
                None => ShardSelectorInternal::All,
                Some(shard_key) => ShardSelectorInternal::from(shard_key),
            };

            (req.recommend_request, shard_selector)
        })
        .collect();

    toc.recommend_batch(
        collection_name,
        requests,
        read_consistency,
        access,
        timeout,
        hw_measurement_acc,
    )
    .await
}

#[post("/collections/{name}/points/recommend/batch")]
async fn recommend_batch_points(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<RecommendRequestBatch>,
    params: Query<ReadParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAccessWithMethod { access, auth_method }: ActixAccessWithMethod,
    ActixRequesterContext(requester): ActixRequesterContext,
) -> impl Responder {
    let overall_timing = Instant::now();
    let collection_name = collection.name.clone();

    log_audit_event(
        &auth_method,
        &requester,
        "recommend_batch_points",
        Status::Accepted,
        Some(collection_name.clone()),
        None,
        None,
        None,
    );

    let pass = match check_strict_mode_batch(
        request.searches.iter().map(|i| &i.recommend_request),
        params.timeout_as_secs(),
        &collection_name,
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
                "recommend_batch_points",
                Status::Failure,
                Some(collection_name),
                None,
                Some(overall_timing.elapsed().as_millis() as i64),
                Some(format!("{}", err)),
            );
            return process_response_error(err, Instant::now(), None);
        }
    };

    let request_hw_counter = get_request_hardware_counter(
        &dispatcher,
        collection_name.clone(),
        service_config.hardware_reporting(),
        None,
    );
    let timing = Instant::now();

    let result = do_recommend_batch_points(
        dispatcher.toc(&access, &pass),
        &collection_name,
        request.into_inner(),
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
        "recommend_batch_points",
        if result.is_ok() { Status::Success } else { Status::Failure },
        Some(collection_name),
        None,
        Some(overall_timing.elapsed().as_millis() as i64),
        result.as_ref().err().map(|e| format!("{}", e)),
    );

    helpers::process_response(result, timing, request_hw_counter.to_rest_api())
}

#[post("/collections/{name}/points/recommend/groups")]
async fn recommend_point_groups(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    request: Json<RecommendGroupsRequest>,
    params: Query<ReadParams>,
    service_config: web::Data<ServiceConfig>,
    ActixAccessWithMethod { access, auth_method }: ActixAccessWithMethod,
    ActixRequesterContext(requester): ActixRequesterContext,
) -> impl Responder {
    let overall_timing = Instant::now();
    let collection_name = collection.name.clone();

    log_audit_event(
        &auth_method,
        &requester,
        "recommend_point_groups",
        Status::Accepted,
        Some(collection_name.clone()),
        None,
        None,
        None,
    );

    let RecommendGroupsRequest {
        recommend_group_request,
        shard_key,
    } = request.into_inner();

    let pass = match check_strict_mode(
        &recommend_group_request,
        params.timeout_as_secs(),
        &collection_name,
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
                "recommend_point_groups",
                Status::Failure,
                Some(collection_name),
                None,
                Some(timing.elapsed().as_millis() as i64),
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
        collection_name.clone(),
        service_config.hardware_reporting(),
        None,
    );
    let timing = Instant::now();

    let result = crate::common::query::do_recommend_point_groups(
        dispatcher.toc(&access, &pass),
        &collection_name,
        recommend_group_request,
        params.consistency,
        shard_selection,
        access,
        params.timeout(),
        request_hw_counter.get_counter(),
    )
    .await;

    log_audit_event(
        &auth_method,
        &requester,
        "recommend_point_groups",
        if result.is_ok() { Status::Success } else { Status::Failure },
        Some(collection_name),
        None,
        Some(overall_timing.elapsed().as_millis() as i64),
        result.as_ref().err().map(|e| format!("{}", e)),
    );

    helpers::process_response(result, timing, request_hw_counter.to_rest_api())
}
// Configure services
pub fn config_recommend_api(cfg: &mut web::ServiceConfig) {
    cfg.service(recommend_points)
        .service(recommend_batch_points)
        .service(recommend_point_groups);
}
