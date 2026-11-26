"""Mock payment provider (ADR 003): authorize, capture, void and lookup by key.

Faults can be injected so the booking flow can be proven safe:
    POST /mock/faults {"op": "authorize", "faults": ["timeout_after"]}
or at random for load tests with MOCKPAY_TIMEOUT_RATE and MOCKPAY_DECLINE_RATE.

Run on its own: uv run uvicorn rushline.mockpay:app --port 8001
"""

import asyncio
import os
import random
import uuid
from collections import deque
from typing import Literal

from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel, Field

Op = Literal["authorize", "capture", "void", "lookup"]
Fault = Literal["decline", "timeout_before", "timeout_after", "fail", "expired"]
Status = Literal["authorized", "captured", "voided", "declined"]

HANG_SECONDS = 5.0  # longer than any client timeout
TIMEOUT_RATE = float(os.environ.get("MOCKPAY_TIMEOUT_RATE", "0"))
DECLINE_RATE = float(os.environ.get("MOCKPAY_DECLINE_RATE", "0"))
DECLINE_TOKEN = "tok_decline"  # always declined, for demos and tests

app = FastAPI(title="Mock payments")


class Payment(BaseModel):
    id: str
    key: str
    amount_paise: int
    status: Status


class AuthorizeIn(BaseModel):
    key: str = Field(min_length=1, max_length=255)  # our idempotency key
    amount_paise: int = Field(gt=0)
    token: str


class FaultsIn(BaseModel):
    op: Op
    faults: list[Fault]


payments: dict[str, Payment] = {}
ids_by_key: dict[str, str] = {}
queued: dict[str, deque[Fault]] = {op: deque() for op in ("authorize", "capture", "void", "lookup")}


def _next_fault(op: Op) -> Fault | None:
    if queued[op]:
        return queued[op].popleft()
    roll = random.random()
    if roll < TIMEOUT_RATE:
        # Both unknowns: never happened, or happened but the caller can't know.
        return random.choice(["timeout_before", "timeout_after"])
    if op == "authorize" and roll < TIMEOUT_RATE + DECLINE_RATE:
        return "decline"
    return None


async def _before(fault: Fault | None) -> None:
    if fault == "timeout_before":  # never processed
        await asyncio.sleep(HANG_SECONDS)
        raise HTTPException(504, "provider timeout")
    if fault == "fail":
        raise HTTPException(502, "provider error")


async def _after(fault: Fault | None) -> None:
    if fault == "timeout_after":  # processed, but the answer never arrives in time
        await asyncio.sleep(HANG_SECONDS)


def _get(payment_id: str) -> Payment:
    if payment_id not in payments:
        raise HTTPException(404, "unknown payment")
    return payments[payment_id]


@app.post("/payments/authorize", status_code=201)
async def authorize(body: AuthorizeIn, response: Response) -> Payment:
    fault = _next_fault("authorize")
    await _before(fault)
    if body.key in ids_by_key:  # idempotent: same key, same payment
        payment = payments[ids_by_key[body.key]]
    else:
        declined = fault == "decline" or body.token == DECLINE_TOKEN
        payment = Payment(
            id=f"pay_{uuid.uuid4().hex[:16]}",
            key=body.key,
            amount_paise=body.amount_paise,
            status="declined" if declined else "authorized",
        )
        payments[payment.id], ids_by_key[body.key] = payment, payment.id
    await _after(fault)
    if payment.status == "declined":
        response.status_code = 402
    return payment


@app.post("/payments/{payment_id}/capture")
async def capture(payment_id: str) -> Payment:
    fault = _next_fault("capture")
    await _before(fault)
    payment = _get(payment_id)
    if fault == "expired" and payment.status == "authorized":
        raise HTTPException(410, "authorization expired")
    if payment.status not in ("authorized", "captured"):
        raise HTTPException(409, f"payment is {payment.status}")
    payment.status = "captured"  # idempotent
    await _after(fault)
    return payment


@app.post("/payments/{payment_id}/void")
async def void(payment_id: str) -> Payment:
    fault = _next_fault("void")
    await _before(fault)
    payment = _get(payment_id)
    if payment.status == "captured":
        raise HTTPException(409, "payment is captured")
    if payment.status == "authorized":
        payment.status = "voided"
    await _after(fault)
    return payment


@app.get("/payments")
async def lookup(key: str) -> Payment:
    fault = _next_fault("lookup")
    await _before(fault)
    if key not in ids_by_key:
        raise HTTPException(404, "unknown key")
    await _after(fault)
    return payments[ids_by_key[key]]


@app.post("/mock/faults", status_code=204)
async def add_faults(body: FaultsIn) -> None:
    queued[body.op].extend(body.faults)


@app.delete("/mock/state", status_code=204)
async def reset() -> None:
    payments.clear()
    ids_by_key.clear()
    for faults in queued.values():
        faults.clear()
