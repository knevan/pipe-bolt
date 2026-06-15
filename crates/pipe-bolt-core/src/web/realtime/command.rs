use salvo::prelude::*;

use crate::command::CommandRequest;
use crate::web::realtime::state::RealtimeBridgeState;

#[handler]
pub async fn command_http(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<(), StatusError> {
    let state = depot
        .obtain::<RealtimeBridgeState>()
        .map_err(|_| StatusError::internal_server_error().brief("realtime bridge state missing"))?
        .clone();
    let request = req.parse_json::<CommandRequest>().await.map_err(|err| {
        StatusError::bad_request().brief(format!("invalid command request: {}", err))
    })?;

    match state.enqueue_command(request) {
        Ok(receipt) => {
            res.status_code(StatusCode::ACCEPTED);
            res.render(Json(serde_json::json!({
                "type": "command_queue",
                "data": receipt,
            })));
            Ok(())
        }
        Err(err) => Err(err.into_status_error()),
    }
}
