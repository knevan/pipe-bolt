use salvo::oapi::security::{Http, HttpAuthScheme, SecurityScheme};
use salvo::oapi::{Info, OpenApi};
use salvo::prelude::*;

pub fn attach_openapi(router: Router) -> Router {
    let doc = OpenApi::new("Pipe Bolt Management API", env!("CARGO_PKG_VERSION"))
        .info(
            Info::new("Pipe Bolt Management API", env!("CARGO_PKG_VERSION")).description(
                "Management API for project config, operational history, and runtime control.",
            ),
        )
        .add_security_scheme(
            "bearer_auth",
            SecurityScheme::Http(Http::new(HttpAuthScheme::Bearer)),
        )
        .merge_router(&router);

    router
        .unshift(doc.into_router("/api-doc/openapi.json"))
        .unshift(SwaggerUi::new("/api-doc/openapi.json").into_router("/swagger-ui"))
}
