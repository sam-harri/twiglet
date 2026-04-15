use twiglet_engine::http::openapi::ApiDoc;
use utoipa::OpenApi;

fn main() {
    let spec = ApiDoc::openapi()
        .to_pretty_json()
        .expect("failed to serialize OpenAPI spec");
    print!("{spec}");
}
