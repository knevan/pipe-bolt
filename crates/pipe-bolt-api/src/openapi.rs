#[cfg(feature = "salvo-oapi")]
use salvo::oapi::security::{Http, HttpAuthScheme, SecurityScheme};
#[cfg(feature = "salvo-oapi")]
use salvo::oapi::{Info, OpenApi};
use salvo::prelude::*;

#[cfg(feature = "salvo-oapi")]
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
        .unshift(Scalar::new("/api-doc/openapi.json").into_router("/scalar"))
}

#[cfg(not(feature = "salvo-oapi"))]
pub fn attach_openapi(router: Router) -> Router {
    router
}
