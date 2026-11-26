"""ADR 004: the agent can hold seats, but pays only with an approval a user decided."""

import uuid

import pytest
from helpers import hold, query

from rushline import mockpay


def start_run(client, auth, user=1):
    conversation = client.post("/conversations", headers=auth(user)).json()["conversation_id"]
    response = client.post(
        f"/conversations/{conversation}/messages", json={"text": "book 2 seats"}, headers=auth(user)
    )
    assert response.status_code == 202
    return response.json()["run_id"]


def agent_book(client, headers, hold_id, approval_id=None, token=None):
    body = {"hold_id": hold_id}
    if approval_id is not None:
        body["approval_id"] = approval_id
    if token is not None:
        body["payment_token"] = token
    return client.post(
        "/bookings", json=body, headers={**headers, "Idempotency-Key": uuid.uuid4().hex}
    )


@pytest.fixture
def agent_flow(client, auth):
    """A run, its agent token, and a hold the agent placed for the user."""
    run_id = start_run(client, auth)
    agent = auth(1, "agent", run_id=run_id)
    hold_id = hold(client, agent, [1, 2])
    return run_id, agent, hold_id


def request_approval(client, agent, hold_id):
    response = client.post("/approvals", json={"hold_id": hold_id}, headers=agent)
    assert response.status_code == 201, response.text
    return response.json()


def test_message_queues_a_run_through_the_outbox(client, auth):
    run_id = start_run(client, auth)

    assert client.get(f"/runs/{run_id}", headers=auth()).json()["status"] == "queued"
    [(event_type, payload)] = query("SELECT event_type, payload FROM outbox")
    assert event_type == "run.queued" and payload["run_id"] == run_id


def test_cannot_post_into_someone_elses_conversation(client, auth):
    conversation = client.post("/conversations", headers=auth(1)).json()["conversation_id"]
    response = client.post(
        f"/conversations/{conversation}/messages", json={"text": "hi"}, headers=auth(2)
    )
    assert response.status_code == 404


def test_approved_agent_payment_books_and_uses_the_approval_once(client, auth, agent_flow):
    run_id, agent, hold_id = agent_flow
    approval = request_approval(client, agent, hold_id)
    assert approval["amount_paise"] == 100000

    decided = client.post(
        f"/runs/{run_id}/approvals/{approval['approval_id']}",
        json={"decision": "approve", "payment_token": "tok_ok"},
        headers=auth(),
    )
    booked = agent_book(client, agent, hold_id, approval_id=approval["approval_id"])

    assert decided.json()["status"] == "approved"
    assert booked.status_code == 201 and booked.json()["status"] == "confirmed"
    [(status, token)] = query("SELECT status, payment_token FROM approvals")
    assert (status, token) == ("used", None)  # the token doesn't outlive its use
    again = agent_book(client, agent, hold_id, approval_id=approval["approval_id"])
    assert again.status_code in (403, 409)


def test_agent_cannot_pay_without_an_approval(client, agent_flow):
    _, agent, hold_id = agent_flow

    assert agent_book(client, agent, hold_id, token="tok_ok").status_code == 403
    assert mockpay.payments == {}


def test_pending_approval_is_not_enough(client, agent_flow):
    _, agent, hold_id = agent_flow
    approval = request_approval(client, agent, hold_id)

    response = agent_book(client, agent, hold_id, approval_id=approval["approval_id"])

    assert response.status_code == 403
    assert response.json() == {"detail": "no valid approval for this hold"}


def test_agent_cannot_approve_its_own_payment(client, auth, agent_flow):
    run_id, agent, hold_id = agent_flow
    approval = request_approval(client, agent, hold_id)

    response = client.post(
        f"/runs/{run_id}/approvals/{approval['approval_id']}",
        json={"decision": "approve", "payment_token": "tok_ok"},
        headers=agent,
    )

    assert response.status_code == 403


def test_approval_is_bound_to_its_run(client, auth, agent_flow):
    run_id, agent, hold_id = agent_flow
    approval = request_approval(client, agent, hold_id)
    client.post(
        f"/runs/{run_id}/approvals/{approval['approval_id']}",
        json={"decision": "approve", "payment_token": "tok_ok"},
        headers=auth(),
    )
    other_run_agent = auth(1, "agent", run_id=start_run(client, auth))

    response = agent_book(client, other_run_agent, hold_id, approval_id=approval["approval_id"])

    assert response.status_code == 403


def test_mcp_tokens_cannot_pay_or_request_approvals(client, auth):
    hold_id = hold(client, auth(1, "mcp"), [3])

    assert (
        client.post("/approvals", json={"hold_id": hold_id}, headers=auth(1, "mcp")).status_code
        == 403
    )
    assert agent_book(client, auth(1, "mcp"), hold_id, token="tok_ok").status_code == 403


def test_rejecting_resumes_the_run_and_nothing_is_charged(client, auth, agent_flow):
    run_id, agent, hold_id = agent_flow
    approval = request_approval(client, agent, hold_id)

    response = client.post(
        f"/runs/{run_id}/approvals/{approval['approval_id']}",
        json={"decision": "reject"},
        headers=auth(),
    )

    assert response.json()["status"] == "rejected"
    events = [r.event_type for r in query("SELECT event_type FROM outbox ORDER BY id")]
    assert events[-1] == "run.resume"
    assert (
        agent_book(client, agent, hold_id, approval_id=approval["approval_id"]).status_code == 403
    )
    assert mockpay.payments == {}


def test_approval_expires_with_its_hold(client, auth, agent_flow):
    run_id, agent, hold_id = agent_flow
    approval = request_approval(client, agent, hold_id)
    query("UPDATE approvals SET expires_at = now() - interval '1 minute'")

    response = client.post(
        f"/runs/{run_id}/approvals/{approval['approval_id']}",
        json={"decision": "approve", "payment_token": "tok_ok"},
        headers=auth(),
    )

    assert response.status_code == 410


def test_one_live_approval_per_hold(client, agent_flow):
    _, agent, hold_id = agent_flow
    request_approval(client, agent, hold_id)

    assert client.post("/approvals", json={"hold_id": hold_id}, headers=agent).status_code == 409


def test_approve_needs_a_payment_token(client, auth, agent_flow):
    run_id, agent, hold_id = agent_flow
    approval = request_approval(client, agent, hold_id)

    response = client.post(
        f"/runs/{run_id}/approvals/{approval['approval_id']}",
        json={"decision": "approve"},
        headers=auth(),
    )

    assert response.status_code == 422


def test_agent_reports_usage_only_for_its_own_run(client, auth, agent_flow):
    run_id, agent, _ = agent_flow
    update = {"status": "running", "tokens_in": 120, "tokens_out": 30, "cost_usd": "0.0001"}

    first = client.patch(f"/runs/{run_id}", json=update, headers=agent).json()
    second = client.patch(f"/runs/{run_id}", json=update, headers=agent).json()
    other = client.patch(f"/runs/{run_id + 1}", json=update, headers=agent)

    assert (first["tokens_in"], second["tokens_in"]) == (120, 240)  # usage adds up
    assert other.status_code == 403
    assert client.patch(f"/runs/{run_id}", json=update, headers=auth()).status_code == 403
