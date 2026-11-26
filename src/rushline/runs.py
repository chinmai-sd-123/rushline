"""Conversations, agent runs and payment approvals (ADR 004).

Users talk to the agent; each message becomes a run, queued through the outbox.
The agent worker reports progress with its own short-lived token. Before paying, the
agent asks for an approval bound to one hold and amount. Only a user token can decide
it, and the payment token goes from the user to the API, never through the agent.
"""

from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection

from rushline import outbox
from rushline.auth import Principal, require_actor
from rushline.deps import get_conn

router = APIRouter()
user_only = require_actor("user")
agent_only = require_actor("agent")

RunStatus = Literal["queued", "running", "waiting_approval", "completed", "failed"]


# --- Conversations and runs ---


class ConversationOut(BaseModel):
    conversation_id: int


class MessageIn(BaseModel):
    text: str = Field(min_length=1, max_length=2000)


class RunQueued(BaseModel):
    run_id: int


class RunOut(BaseModel):
    run_id: int
    conversation_id: int
    status: RunStatus
    input: str
    output: str | None
    tokens_in: int
    tokens_out: int
    cost_usd: Decimal
    updated_at: datetime


class RunUpdate(BaseModel):
    """Sent by the agent worker. Usage fields are added, not replaced."""

    status: RunStatus | None = None
    output: str | None = None
    tokens_in: int = Field(default=0, ge=0)
    tokens_out: int = Field(default=0, ge=0)
    cost_usd: Decimal = Field(default=Decimal(0), ge=0)


RUN_COLUMNS = """
    id AS run_id, conversation_id, status, input, output, tokens_in, tokens_out, cost_usd,
    updated_at
"""


@router.post("/conversations", status_code=status.HTTP_201_CREATED)
async def create_conversation(
    principal: Annotated[Principal, Depends(user_only)],
    conn: Annotated[AsyncConnection, Depends(get_conn)],
) -> ConversationOut:
    async with conn.begin():
        conversation_id = await conn.scalar(
            text("INSERT INTO conversations (user_id) VALUES (:user_id) RETURNING id"),
            {"user_id": principal.user_id},
        )
    return ConversationOut(conversation_id=conversation_id)


@router.post("/conversations/{conversation_id}/messages", status_code=status.HTTP_202_ACCEPTED)
async def post_message(
    conversation_id: int,
    body: MessageIn,
    principal: Annotated[Principal, Depends(user_only)],
    conn: Annotated[AsyncConnection, Depends(get_conn)],
) -> RunQueued:
    """Queue a run. Run and outbox event commit together, so a run is never lost."""
    async with conn.begin():
        owner = await conn.scalar(
            text("SELECT user_id FROM conversations WHERE id = :id"), {"id": conversation_id}
        )
        if owner != principal.user_id:
            raise HTTPException(404, "conversation not found")
        run_id = await conn.scalar(
            text("""
                INSERT INTO agent_runs (conversation_id, user_id, input)
                VALUES (:conversation_id, :user_id, :input)
                RETURNING id
            """),
            {"conversation_id": conversation_id, "user_id": principal.user_id, "input": body.text},
        )
        await outbox.add(
            conn,
            "run.queued",
            {"run_id": run_id, "conversation_id": conversation_id, "user_id": principal.user_id},
        )
    return RunQueued(run_id=run_id)


def _check_run_access(principal: Principal, run_id: int) -> None:
    if principal.actor == "agent" and principal.run_id != run_id:
        raise HTTPException(403, "agent token is for another run")


@router.get("/runs/{run_id}")
async def get_run(
    run_id: int,
    principal: Annotated[Principal, Depends(require_actor("user", "agent"))],
    conn: Annotated[AsyncConnection, Depends(get_conn)],
) -> RunOut:
    _check_run_access(principal, run_id)
    row = (
        await conn.execute(
            text(f"SELECT {RUN_COLUMNS} FROM agent_runs WHERE id = :id AND user_id = :user_id"),
            {"id": run_id, "user_id": principal.user_id},
        )
    ).one_or_none()
    if row is None:
        raise HTTPException(404, "run not found")
    return RunOut(**row._mapping)


@router.patch("/runs/{run_id}")
async def update_run(
    run_id: int,
    body: RunUpdate,
    principal: Annotated[Principal, Depends(agent_only)],
    conn: Annotated[AsyncConnection, Depends(get_conn)],
) -> RunOut:
    """The agent worker reports status, output and token usage (it can't touch the tables)."""
    _check_run_access(principal, run_id)
    async with conn.begin():
        row = (
            await conn.execute(
                text(f"""
                    UPDATE agent_runs SET
                        status = coalesce(:status, status),
                        output = coalesce(:output, output),
                        tokens_in = tokens_in + :tokens_in,
                        tokens_out = tokens_out + :tokens_out,
                        cost_usd = cost_usd + :cost_usd,
                        updated_at = now()
                    WHERE id = :id AND user_id = :user_id
                    RETURNING {RUN_COLUMNS}
                """),
                {"id": run_id, "user_id": principal.user_id, **body.model_dump()},
            )
        ).one_or_none()
    if row is None:
        raise HTTPException(404, "run not found")
    return RunOut(**row._mapping)


# --- Approvals ---


class ApprovalIn(BaseModel):
    hold_id: int


class ApprovalOut(BaseModel):
    approval_id: int
    run_id: int
    hold_id: int
    amount_paise: int
    status: str
    expires_at: datetime


class DecisionIn(BaseModel):
    decision: Literal["approve", "reject"]
    payment_token: str | None = Field(default=None, min_length=1, max_length=200)

    @model_validator(mode="after")
    def token_only_when_approving(self) -> "DecisionIn":
        if (self.decision == "approve") != (self.payment_token is not None):
            raise ValueError("payment_token is required to approve, and only then")
        return self


APPROVAL_COLUMNS = "id AS approval_id, run_id, hold_id, amount_paise, status, expires_at"


@router.post("/approvals", status_code=status.HTTP_201_CREATED)
async def request_approval(
    body: ApprovalIn,
    principal: Annotated[Principal, Depends(agent_only)],
    conn: Annotated[AsyncConnection, Depends(get_conn)],
) -> ApprovalOut:
    """The agent asks to pay for one active hold. Amount and expiry come from the hold."""
    params = {"hold_id": body.hold_id, "user_id": principal.user_id, "run_id": principal.run_id}
    try:
        async with conn.begin():
            row = (
                await conn.execute(
                    text(f"""
                        INSERT INTO approvals (run_id, user_id, hold_id, amount_paise, expires_at)
                        SELECT :run_id, h.user_id, h.id,
                               (SELECT sum(price_paise) FROM seats
                                WHERE hold_id = h.id AND status = 'held'),
                               h.expires_at
                        FROM holds h
                        WHERE h.id = :hold_id AND h.user_id = :user_id
                          AND h.status = 'active' AND h.expires_at > now()
                        RETURNING {APPROVAL_COLUMNS}
                    """),
                    params,
                )
            ).one_or_none()
    except IntegrityError as exc:
        raise HTTPException(409, "this hold already has an approval request") from exc
    if row is None:
        raise HTTPException(404, "no active hold with that id")
    return ApprovalOut(**row._mapping)


@router.get("/runs/{run_id}/approvals")
async def list_approvals(
    run_id: int,
    principal: Annotated[Principal, Depends(user_only)],
    conn: Annotated[AsyncConnection, Depends(get_conn)],
) -> list[ApprovalOut]:
    rows = await conn.execute(
        text(f"""
            SELECT {APPROVAL_COLUMNS} FROM approvals
            WHERE run_id = :run_id AND user_id = :user_id ORDER BY id
        """),
        {"run_id": run_id, "user_id": principal.user_id},
    )
    return [ApprovalOut(**row._mapping) for row in rows]


@router.post("/runs/{run_id}/approvals/{approval_id}")
async def decide_approval(
    run_id: int,
    approval_id: int,
    body: DecisionIn,
    principal: Annotated[Principal, Depends(user_only)],  # an agent can't approve itself
    conn: Annotated[AsyncConnection, Depends(get_conn)],
) -> ApprovalOut:
    approved = body.decision == "approve"
    params = {"id": approval_id, "run_id": run_id, "user_id": principal.user_id}
    async with conn.begin():
        row = (
            await conn.execute(
                text(f"""
                    UPDATE approvals
                    SET status = :status, payment_token = :token, decided_at = now()
                    WHERE id = :id AND run_id = :run_id AND user_id = :user_id
                      AND status = 'pending' AND expires_at > now()
                    RETURNING {APPROVAL_COLUMNS}
                """),
                {
                    **params,
                    "status": "approved" if approved else "rejected",
                    "token": body.payment_token,
                },
            )
        ).one_or_none()
        if row is not None:
            # Wake the agent run up again (through the outbox, like every queued run).
            await outbox.add(
                conn,
                "run.resume",
                {"run_id": run_id, "approval_id": approval_id, "decision": body.decision},
            )
            return ApprovalOut(**row._mapping)
        current = (
            await conn.execute(
                text("""
                    SELECT status, expires_at <= now() AS expired FROM approvals
                    WHERE id = :id AND run_id = :run_id AND user_id = :user_id
                """),
                params,
            )
        ).one_or_none()
    if current is None:
        raise HTTPException(404, "approval not found")
    if current.status == "pending" and current.expired:
        raise HTTPException(410, "approval expired with its hold")
    raise HTTPException(409, f"approval is {current.status}")


# Consume the approval and hand back its token in one statement. RETURNING shows new
# values, so the CTE reads (and locks) the old token before it's cleared.
USE_APPROVAL = text("""
    WITH a AS (
        SELECT id, payment_token, amount_paise FROM approvals
        WHERE id = :approval_id AND run_id = :run_id AND user_id = :user_id
          AND hold_id = :hold_id AND status = 'approved' AND expires_at > now()
        FOR UPDATE
    )
    UPDATE approvals SET status = 'used', used_at = now(), payment_token = NULL
    FROM a WHERE approvals.id = a.id
    RETURNING a.payment_token, a.amount_paise
""")


async def use_approval(
    conn: AsyncConnection, principal: Principal, approval_id: int, hold_id: int
) -> tuple[str, int] | None:
    """Inside the caller's transaction: (payment_token, amount) once, then never again."""
    row = (
        await conn.execute(
            USE_APPROVAL,
            {
                "approval_id": approval_id,
                "run_id": principal.run_id,
                "user_id": principal.user_id,
                "hold_id": hold_id,
            },
        )
    ).one_or_none()
    return None if row is None else (row.payment_token, row.amount_paise)
