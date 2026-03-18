"""
Flow run retry endpoint.

POST /api/flow_runs/{id}/retry
Resets a failed or crashed flow run back to SCHEDULED state, optionally
overriding parameters for the new attempt.
"""
from uuid import UUID

from fastapi import Body, Depends, HTTPException, Path, status
from pydantic import BaseModel, Field

from prefect._internal.compatibility.starlette import APIRouter
from prefect.server.database.dependencies import provide_database_interface
from prefect.server.database.interface import PrefectDBInterface
from prefect.server.schemas.states import StateType

router = APIRouter(prefix="/flow_runs", tags=["Flow Run Retries"])

RETRYABLE_STATES = {
    StateType.FAILED,
    StateType.CRASHED,
    StateType.CANCELLED,
}


class RetryRequest(BaseModel):
    parameters: dict = Field(
        default_factory=dict,
        description="Optional parameter overrides for the retry attempt.",
    )
    reason: str = Field(
        default="Retried by user.",
        max_length=500,
    )


@router.post("/{id}/retry", status_code=status.HTTP_200_OK)
async def retry_flow_run(
    id: UUID = Path(..., description="The flow run ID to retry"),
    body: RetryRequest = Body(default_factory=RetryRequest),
    db: PrefectDBInterface = Depends(provide_database_interface),
):
    """Retry a failed, crashed, or cancelled flow run."""
    async with db.session_context(begin_transaction=True) as session:
        flow_run = await db.queries.read_flow_run(session=session, flow_run_id=id)
        if flow_run is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Flow run {id} not found.",
            )
        current_type = StateType(flow_run.state_type) if flow_run.state_type else None
        if current_type not in RETRYABLE_STATES:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"Flow run {id} is in state {flow_run.state_type!r} "
                    "and cannot be retried."
                ),
            )
        if body.parameters:
            merged = {**(flow_run.parameters or {}), **body.parameters}
            await db.queries.update_flow_run(
                session=session,
                flow_run_id=id,
                flow_run={"parameters": merged},
            )
        await db.queries.set_flow_run_state(
            session=session,
            flow_run_id=id,
            state={"type": "SCHEDULED", "message": body.reason},
        )

    return {"id": str(id), "state": "SCHEDULED", "message": body.reason}
