"""
Flow run cancellation endpoint.

POST /api/flow_runs/{id}/cancel
Transitions a PENDING, RUNNING, SCHEDULED, or PAUSED flow run to CANCELLING.
"""
from uuid import UUID

from fastapi import Body, Depends, HTTPException, Path, status
from pydantic import BaseModel, Field

from prefect._internal.compatibility.starlette import APIRouter
from prefect.server.database.dependencies import provide_database_interface
from prefect.server.database.interface import PrefectDBInterface
from prefect.server.schemas.states import StateType

router = APIRouter(prefix="/flow_runs", tags=["Flow Run Cancellations"])

CANCELLABLE_STATES = {
    StateType.PENDING,
    StateType.RUNNING,
    StateType.SCHEDULED,
    StateType.PAUSED,
}


class CancelRequest(BaseModel):
    reason: str = Field(default="Cancelled by user.", max_length=500)


@router.post("/{id}/cancel", status_code=status.HTTP_200_OK)
async def cancel_flow_run(
    id: UUID = Path(..., description="The flow run ID to cancel"),
    body: CancelRequest = Body(default_factory=CancelRequest),
    db: PrefectDBInterface = Depends(provide_database_interface),
):
    """Cancel a flow run by transitioning it to CANCELLING state."""
    async with db.session_context(begin_transaction=True) as session:
        flow_run = await db.queries.read_flow_run(session=session, flow_run_id=id)
        if flow_run is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Flow run {id} not found.",
            )
        current_type = StateType(flow_run.state_type) if flow_run.state_type else None
        if current_type not in CANCELLABLE_STATES:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"Flow run {id} is in state {flow_run.state_type!r} "
                    "and cannot be cancelled."
                ),
            )
        await db.queries.set_flow_run_state(
            session=session,
            flow_run_id=id,
            state={"type": "CANCELLING", "message": body.reason},
        )

    return {"id": str(id), "state": "CANCELLING", "message": body.reason}
