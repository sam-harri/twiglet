//! Legit the most simple auth impl
//!
//! One user, one password for now
//!
//! Expects in header Authorization: Basic <base64(username:password)>

use axum::{
    body::Body,
    extract::State,
    http::{Method, Request, StatusCode, header},
    middleware::Next,
    response::Response,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};

#[derive(Clone, Debug)]
pub struct BasicAuthConfig {
    pub username: String,
    pub password: String,
}

pub async fn basic_auth(
    State(config): State<BasicAuthConfig>,
    req: Request<Body>,
    next: Next,
) -> Response {
    if req.method() == Method::OPTIONS {
        return next.run(req).await;
    }

    let authorized = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(parse_basic_header)
        .is_some_and(|(username, password)| {
            username == config.username && password == config.password
        });

    if authorized {
        return next.run(req).await;
    }

    let mut response = Response::new(Body::from("Unauthorized"));
    *response.status_mut() = StatusCode::UNAUTHORIZED;
    response.headers_mut().insert(
        header::WWW_AUTHENTICATE,
        header::HeaderValue::from_static("Basic realm=\"twiglet\""),
    );
    response
}

fn parse_basic_header(header_value: &str) -> Option<(String, String)> {
    let encoded = header_value.strip_prefix("Basic ")?;
    let decoded = STANDARD.decode(encoded).ok()?;
    let as_utf8 = String::from_utf8(decoded).ok()?;
    let (username, password) = as_utf8.split_once(':')?;
    Some((username.to_string(), password.to_string()))
}

#[cfg(test)]
mod tests {
    use axum::{
        Router,
        body::Body,
        http::{Request, StatusCode, header},
        middleware,
        response::IntoResponse,
        routing::get,
    };
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use tower::ServiceExt;

    use super::{BasicAuthConfig, basic_auth};

    #[tokio::test]
    async fn middleware_should_reject_missing_or_invalid_credentials() {
        let app = Router::new()
            .route("/ok", get(|| async { "ok".into_response() }))
            .layer(middleware::from_fn_with_state(
                BasicAuthConfig {
                    username: "twigletadmin".to_string(),
                    password: "twigletadmin".to_string(),
                },
                basic_auth,
            ));

        let missing = app
            .clone()
            .oneshot(Request::builder().uri("/ok").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);

        let bad = app
            .oneshot(
                Request::builder()
                    .uri("/ok")
                    .header(header::AUTHORIZATION, "Basic invalid")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(bad.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn middleware_should_allow_valid_credentials() {
        let token = STANDARD.encode("twigletadmin:twigletadmin");
        let app = Router::new()
            .route("/ok", get(|| async { "ok".into_response() }))
            .layer(middleware::from_fn_with_state(
                BasicAuthConfig {
                    username: "twigletadmin".to_string(),
                    password: "twigletadmin".to_string(),
                },
                basic_auth,
            ));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/ok")
                    .header(header::AUTHORIZATION, format!("Basic {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn middleware_should_allow_options_without_auth() {
        let app = Router::new()
            .route("/ok", get(|| async { "ok".into_response() }))
            .layer(middleware::from_fn_with_state(
                BasicAuthConfig {
                    username: "twigletadmin".to_string(),
                    password: "twigletadmin".to_string(),
                },
                basic_auth,
            ));

        let response = app
            .oneshot(
                Request::builder()
                    .method("OPTIONS")
                    .uri("/ok")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    }
}
