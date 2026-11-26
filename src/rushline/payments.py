"""Client for the payment provider (ADR 003).

Every call has a hard timeout. A timeout or a 5xx means the outcome is UNKNOWN,
never that it failed: the payment may have gone through. Reconciliation looks it up.
"""

import asyncio
from dataclasses import dataclass

import httpx2

# Test seam: route provider calls to the in-process mock instead of the network.
transport_override: httpx2.AsyncBaseTransport | None = None


class PaymentUnknown(Exception):
    """Timed out, unreachable or 5xx: it may or may not have happened."""


class PaymentDeclined(Exception):
    pass


class PaymentRejected(Exception):
    """A definite no, e.g. capturing a voided payment (409) or an expired one (410)."""

    def __init__(self, status: int, detail: str):
        super().__init__(f"{status}: {detail}")
        self.status = status


@dataclass(frozen=True)
class Payment:
    id: str
    key: str
    amount_paise: int
    status: str  # authorized, captured, voided, declined


class PaymentClient:
    def __init__(self, base_url: str, timeout_seconds: float):
        self.http = httpx2.AsyncClient(base_url=base_url, transport=transport_override)
        self.timeout = timeout_seconds

    async def aclose(self) -> None:
        await self.http.aclose()

    async def _call(self, method: str, url: str, **kwargs) -> httpx2.Response:
        try:
            async with asyncio.timeout(self.timeout):
                response = await self.http.request(method, url, **kwargs)
        except (TimeoutError, httpx2.TransportError) as exc:
            raise PaymentUnknown(f"{method} {url}: {type(exc).__name__}") from exc
        if response.status_code >= 500:
            raise PaymentUnknown(f"{method} {url}: {response.status_code}")
        return response

    @staticmethod
    def _payment(response: httpx2.Response) -> Payment:
        return Payment(**response.json())

    async def authorize(self, key: str, amount_paise: int, token: str) -> Payment:
        response = await self._call(
            "POST",
            "/payments/authorize",
            json={"key": key, "amount_paise": amount_paise, "token": token},
        )
        if response.status_code == 402:
            raise PaymentDeclined(key)
        if response.status_code != 201:
            raise PaymentRejected(response.status_code, response.text)
        return self._payment(response)

    async def capture(self, payment_id: str) -> Payment:
        return await self._change(f"/payments/{payment_id}/capture")

    async def void(self, payment_id: str) -> Payment:
        return await self._change(f"/payments/{payment_id}/void")

    async def _change(self, url: str) -> Payment:
        response = await self._call("POST", url)
        if response.status_code != 200:
            raise PaymentRejected(response.status_code, response.json().get("detail", ""))
        return self._payment(response)

    async def lookup(self, key: str) -> Payment | None:
        response = await self._call("GET", "/payments", params={"key": key})
        if response.status_code == 404:
            return None
        return self._payment(response)
