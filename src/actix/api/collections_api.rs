use std::time::Duration;

use actix_web::rt::time::Instant;
use actix_web::{HttpResponse, Responder, delete, get, patch, post, put, web};
use actix_web_validator::{Json, Path, Query};
use collection::operations::cluster_ops::ClusterOperations;
use collection::operations::verification::new_unchecked_verification_pass;
use serde::Deserialize;
use storage::content_manager::collection_meta_ops::{
    ChangeAliasesOperation, CollectionMetaOperations, CreateCollection, CreateCollectionOperation,
    DeleteCollectionOperation, UpdateCollection, UpdateCollectionOperation,
};
use storage::dispatcher::Dispatcher;
use validator::Validate;

use super::CollectionPath;
use crate::actix::api::StrictCollectionPath;
use crate::actix::auth::{ActixAccess, ActixAccessWithMethod};
use crate::actix::helpers::{self, log_audit_event, process_response};
use crate::actix::requester_context::ActixRequesterContext;
use crate::common::collections::*;
use crate::tracing::audit_event::Status;

#[derive(Debug, Deserialize, Validate)]
pub struct WaitTimeout {
    #[validate(range(min = 1))]
    timeout: Option<u64>,
}

impl WaitTimeout {
    pub fn timeout(&self) -> Option<Duration> {
        self.timeout.map(Duration::from_secs)
    }
}

#[get("/collections")]
async fn get_collections(
    dispatcher: web::Data<Dispatcher>,
    ActixAccessWithMethod { access, auth_method }: ActixAccessWithMethod,
    ActixRequesterContext(requester): ActixRequesterContext,
) -> HttpResponse {
    let timing = Instant::now();

    log_audit_event(
        &auth_method,
        &requester,
        "get_collections",
        Status::Accepted,
        None,
        None,
        None,
        None,
    );

    // No request to verify
    let pass = new_unchecked_verification_pass();

    let result = do_list_collections(dispatcher.toc(&access, &pass), access).await;

    log_audit_event(
        &auth_method,
        &requester,
        "get_collections",
        if result.is_ok() { Status::Success } else { Status::Failure },
        None,
        None,
        Some(timing.elapsed().as_millis() as i64),
        result.as_ref().err().map(|e| format!("{}", e)),
    );

    helpers::time(async { result }).await
}

#[get("/aliases")]
async fn get_aliases(
    dispatcher: web::Data<Dispatcher>,
    ActixAccessWithMethod { access, auth_method }: ActixAccessWithMethod,
    ActixRequesterContext(requester): ActixRequesterContext,
) -> HttpResponse {
    let timing = Instant::now();

    log_audit_event(
        &auth_method,
        &requester,
        "get_aliases",
        Status::Accepted,
        None,
        None,
        None,
        None,
    );

    // No request to verify
    let pass = new_unchecked_verification_pass();

    let result = do_list_aliases(dispatcher.toc(&access, &pass), access).await;

    log_audit_event(
        &auth_method,
        &requester,
        "get_aliases",
        if result.is_ok() { Status::Success } else { Status::Failure },
        None,
        None,
        Some(timing.elapsed().as_millis() as i64),
        result.as_ref().err().map(|e| format!("{}", e)),
    );

    helpers::time(async { result }).await
}

#[get("/collections/{name}")]
async fn get_collection(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    ActixAccessWithMethod { access, auth_method }: ActixAccessWithMethod,
    ActixRequesterContext(requester): ActixRequesterContext,
) -> HttpResponse {
    let timing = Instant::now();
    let name = collection.name.clone();

    log_audit_event(
        &auth_method,
        &requester,
        "get_collection",
        Status::Accepted,
        Some(name.clone()),
        None,
        None,
        None,
    );

    // No request to verify
    let pass = new_unchecked_verification_pass();

    let result = do_get_collection(
        dispatcher.toc(&access, &pass),
        access,
        &name,
        None,
    )
    .await;

    log_audit_event(
        &auth_method,
        &requester,
        "get_collection",
        if result.is_ok() { Status::Success } else { Status::Failure },
        Some(name),
        None,
        Some(timing.elapsed().as_millis() as i64),
        result.as_ref().err().map(|e| format!("{}", e)),
    );

    helpers::time(async { result }).await
}

#[get("/collections/{name}/exists")]
async fn get_collection_existence(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    ActixAccessWithMethod { access, auth_method }: ActixAccessWithMethod,
    ActixRequesterContext(requester): ActixRequesterContext,
) -> HttpResponse {
    let timing = Instant::now();
    let name = collection.name.clone();

    log_audit_event(
        &auth_method,
        &requester,
        "get_collection_existence",
        Status::Accepted,
        Some(name.clone()),
        None,
        None,
        None,
    );

    // No request to verify
    let pass = new_unchecked_verification_pass();

    let result = do_collection_exists(
        dispatcher.toc(&access, &pass),
        access,
        &name,
    )
    .await;

    log_audit_event(
        &auth_method,
        &requester,
        "get_collection_existence",
        if result.is_ok() { Status::Success } else { Status::Failure },
        Some(name),
        None,
        Some(timing.elapsed().as_millis() as i64),
        result.as_ref().err().map(|e| format!("{}", e)),
    );

    helpers::time(async { result }).await
}

#[get("/collections/{name}/aliases")]
async fn get_collection_aliases(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    ActixAccessWithMethod { access, auth_method }: ActixAccessWithMethod,
    ActixRequesterContext(requester): ActixRequesterContext,
) -> HttpResponse {
    let timing = Instant::now();
    let name = collection.name.clone();

    log_audit_event(
        &auth_method,
        &requester,
        "get_collection_aliases",
        Status::Accepted,
        Some(name.clone()),
        None,
        None,
        None,
    );

    // No request to verify
    let pass = new_unchecked_verification_pass();

    let result = do_list_collection_aliases(
        dispatcher.toc(&access, &pass),
        access,
        &name,
    )
    .await;

    log_audit_event(
        &auth_method,
        &requester,
        "get_collection_aliases",
        if result.is_ok() { Status::Success } else { Status::Failure },
        Some(name),
        None,
        Some(timing.elapsed().as_millis() as i64),
        result.as_ref().err().map(|e| format!("{}", e)),
    );

    helpers::time(async { result }).await
}

#[put("/collections/{name}")]
async fn create_collection(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<StrictCollectionPath>,
    operation: Json<CreateCollection>,
    Query(query): Query<WaitTimeout>,
    ActixAccessWithMethod { access, auth_method }: ActixAccessWithMethod,
    ActixRequesterContext(requester): ActixRequesterContext,
) -> HttpResponse {
    let timing = Instant::now();
    let create_collection_op =
        CreateCollectionOperation::new(collection.name.clone(), operation.into_inner());

    log_audit_event(
        &auth_method,
        &requester,
        "create_collection",
        Status::Accepted,
        Some(collection.name.clone()),
        None,
        None,
        None,
    );

    let Ok(create_collection_op) = create_collection_op else {
        return process_response(create_collection_op, timing, None);
    };

    let response = dispatcher
        .submit_collection_meta_op(
            CollectionMetaOperations::CreateCollection(create_collection_op),
            access,
            query.timeout(),
        )
        .await;

    log_audit_event(
        &auth_method,
        &requester,
        "create_collection",
        if matches!(response, Ok(true)) { Status::Success } else { Status::Failure },
        Some(collection.name.clone()),
        None,
        Some(timing.elapsed().as_millis() as i64),
        response.as_ref().err().map(|e| format!("{}", e)), 
    );

    process_response(response, timing, None)
}

#[patch("/collections/{name}")]
async fn update_collection(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operation: Json<UpdateCollection>,
    Query(query): Query<WaitTimeout>,
    ActixAccessWithMethod { access, auth_method }: ActixAccessWithMethod,
    ActixRequesterContext(requester): ActixRequesterContext,
) -> impl Responder {
    let timing = Instant::now();
    let name = collection.name.clone();

    log_audit_event(
        &auth_method,
        &requester,
        "update_collection",
        Status::Accepted,
        Some(name.clone()),
        None,
        None,
        None,
    );

    let response = dispatcher
        .submit_collection_meta_op(
            CollectionMetaOperations::UpdateCollection(UpdateCollectionOperation::new(
                name.clone(),
                operation.into_inner(),
            )),
            access,
            query.timeout(),
        )
        .await;

    log_audit_event(
        &auth_method,
        &requester,
        "update_collection",
        if matches!(response, Ok(true)) { Status::Success } else { Status::Failure },
        Some(name),
        None,
        Some(timing.elapsed().as_millis() as i64),
        response.as_ref().err().map(|e| format!("{}", e)),
    );

    process_response(response, timing, None)
}

#[delete("/collections/{name}")]
async fn delete_collection(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    Query(query): Query<WaitTimeout>,
    ActixAccessWithMethod { access, auth_method }: ActixAccessWithMethod,
    ActixRequesterContext(requester): ActixRequesterContext,
) -> impl Responder {
    let timing = Instant::now();
    let name = collection.name.clone();

    log_audit_event(
        &auth_method,
        &requester,
        "delete_collection",
        Status::Accepted,
        Some(name.clone()),
        None,
        None,
        None,
    );

    let response = dispatcher
        .submit_collection_meta_op(
            CollectionMetaOperations::DeleteCollection(DeleteCollectionOperation(
                name.clone(),
            )),
            access,
            query.timeout(),
        )
        .await;

    log_audit_event(
        &auth_method,
        &requester,
        "delete_collection",
        if matches!(response, Ok(true)) { Status::Success } else { Status::Failure },
        Some(name),
        None,
        Some(timing.elapsed().as_millis() as i64),
        response.as_ref().err().map(|e| format!("{}", e)),
    );

    process_response(response, timing, None)
}

#[post("/collections/aliases")]
async fn update_aliases(
    dispatcher: web::Data<Dispatcher>,
    operation: Json<ChangeAliasesOperation>,
    Query(query): Query<WaitTimeout>,
    ActixAccessWithMethod { access, auth_method }: ActixAccessWithMethod,
    ActixRequesterContext(requester): ActixRequesterContext,
) -> impl Responder {
    let timing = Instant::now();

    log_audit_event(
        &auth_method,
        &requester,
        "update_aliases",
        Status::Accepted,
        None,
        None,
        None,
        None,
    );

    let response = dispatcher
        .submit_collection_meta_op(
            CollectionMetaOperations::ChangeAliases(operation.0),
            access,
            query.timeout(),
        )
        .await;

    log_audit_event(
        &auth_method,
        &requester,
        "update_aliases",
        if matches!(response, Ok(true)) { Status::Success } else { Status::Failure },
        None,
        None,
        Some(timing.elapsed().as_millis() as i64),
        response.as_ref().err().map(|e| format!("{}", e)),
    );

    process_response(response, timing, None)
}

#[get("/collections/{name}/cluster")]
async fn get_cluster_info(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    ActixAccessWithMethod { access, auth_method }: ActixAccessWithMethod,
    ActixRequesterContext(requester): ActixRequesterContext,
) -> impl Responder {
    let timing = Instant::now();
    let name = collection.name.clone();

    log_audit_event(
        &auth_method,
        &requester,
        "get_cluster_info",
        Status::Accepted,
        Some(name.clone()),
        None,
        None,
        None,
    );

    // No request to verify
    let pass = new_unchecked_verification_pass();

    let result = do_get_collection_cluster(
        dispatcher.toc(&access, &pass),
        access,
        &name,
    )
    .await;

    log_audit_event(
        &auth_method,
        &requester,
        "get_cluster_info",
        if result.is_ok() { Status::Success } else { Status::Failure },
        Some(name),
        None,
        Some(timing.elapsed().as_millis() as i64),
        result.as_ref().err().map(|e| format!("{}", e)),
    );

    helpers::time(async { result }).await
}

#[post("/collections/{name}/cluster")]
async fn update_collection_cluster(
    dispatcher: web::Data<Dispatcher>,
    collection: Path<CollectionPath>,
    operation: Json<ClusterOperations>,
    Query(query): Query<WaitTimeout>,
    ActixAccessWithMethod { access, auth_method }: ActixAccessWithMethod,
    ActixRequesterContext(requester): ActixRequesterContext,
) -> impl Responder {
    let timing = Instant::now();
    let wait_timeout = query.timeout();
    let name = collection.name.clone();

    log_audit_event(
        &auth_method,
        &requester,
        "update_collection_cluster",
        Status::Accepted,
        Some(name.clone()),
        None,
        None,
        None,
    );

    let response = do_update_collection_cluster(
        &dispatcher.into_inner(),
        name.clone(),
        operation.0,
        access,
        wait_timeout,
    )
    .await;

    log_audit_event(
        &auth_method,
        &requester,
        "update_collection_cluster",
        if matches!(response, Ok(true)) { Status::Success } else { Status::Failure },
        Some(name),
        None,
        Some(timing.elapsed().as_millis() as i64),
        response.as_ref().err().map(|e| format!("{}", e)),
    );

    process_response(response, timing, None)
}

// Configure services
pub fn config_collections_api(cfg: &mut web::ServiceConfig) {
    // Ordering of services is important for correct path pattern matching
    // See: <https://github.com/qdrant/qdrant/issues/3543>
    cfg.service(update_aliases)
        .service(get_collections)
        .service(get_collection)
        .service(get_collection_existence)
        .service(create_collection)
        .service(update_collection)
        .service(delete_collection)
        .service(get_aliases)
        .service(get_collection_aliases)
        .service(get_cluster_info)
        .service(update_collection_cluster);
}

#[cfg(test)]
mod tests {
    use actix_web::web::Query;

    use super::WaitTimeout;

    #[test]
    fn timeout_is_deserialized() {
        let timeout: WaitTimeout = Query::from_query("").unwrap().0;
        assert!(timeout.timeout.is_none());
        let timeout: WaitTimeout = Query::from_query("timeout=10").unwrap().0;
        assert_eq!(timeout.timeout, Some(10))
    }
}
