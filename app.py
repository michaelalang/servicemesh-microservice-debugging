import os
import uuid
import random
import asyncio
from pathlib import Path
from datetime import datetime, timezone

from aiohttp import web, ClientSession, ClientTimeout

from opentelemetry import trace, baggage, context
from opentelemetry.context import Context
from opentelemetry.trace import StatusCode, Status
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.propagate import set_global_textmap, extract, inject
from opentelemetry.propagators.composite import CompositePropagator
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.baggage.propagation import W3CBaggagePropagator

SERVICE_NAME_ENV = os.getenv("SERVICE_NAME", "aiohttp-service")
PORT = int(os.getenv("PORT", "8080"))
NEXT_APP = os.getenv("NEXT_APP")
OTEL_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4317")
STATIC_DIR = Path(os.getenv("STATIC_DIR", Path(__file__).resolve().parent / "static"))

DEFAULT_CHAIN_SERVICES = (
    "frontend,api-gateway,auth-service,user-service,session-service,"
    "catalog-service,search-service,recommendation-service,cart-service,"
    "order-service,inventory-service,payment-service,billing-service,"
    "shipping-service,notification-service,review-service,analytics-service,"
    "reporting-service,audit-service,backend"
)
CHAIN_SERVICES = [
    s.strip()
    for s in os.getenv(
        "FAULT_SERVICES",
        os.getenv("CHAIN_SERVICES", DEFAULT_CHAIN_SERVICES),
    ).split(",")
    if s.strip()
]

# CQRS + Inbox/Outbox peer URLs (override per deployment as needed).
CQRS_COMMAND_URL = os.getenv("CQRS_COMMAND_URL", "http://order-service:8080")
CQRS_QUERY_URL = os.getenv("CQRS_QUERY_URL", "http://catalog-service:8080")
OUTBOX_TARGET_URL = os.getenv("OUTBOX_TARGET_URL", "http://audit-service:8080")

CQRS_PARTICIPANTS = [
    s.strip()
    for s in os.getenv(
        "CQRS_PARTICIPANTS",
        "frontend,order-service,catalog-service",
    ).split(",")
    if s.strip()
]
OUTBOX_PARTICIPANTS = [
    s.strip()
    for s in os.getenv(
        "OUTBOX_PARTICIPANTS",
        "frontend,audit-service",
    ).split(",")
    if s.strip()
]

FAULT_BAGGAGE_KEY = "fault.service"
DELAY_SERVICE_BAGGAGE_KEY = "delay.service"
DELAY_SECONDS_BAGGAGE_KEY = "delay.seconds"
DELAY_CONSUMED_BAGGAGE_KEY = "delay.consumed"
FAULT_CONSUMED_BAGGAGE_KEY = "fault.consumed"
# Chain: delay window (seconds); override via env for workshops that need longer waits.
CHAIN_DELAY_MIN_SECONDS = float(os.getenv("CHAIN_DELAY_MIN_SECONDS", "1.0"))
CHAIN_DELAY_MAX_SECONDS = float(os.getenv("CHAIN_DELAY_MAX_SECONDS", "60.0"))
# CQRS / Inbox-Outbox: long-running delay window (2–5 minutes)
PATTERN_DELAY_MIN_SECONDS = 120.0
PATTERN_DELAY_MAX_SECONDS = 300.0
# Keep HTTP client / gunicorn above the max pattern delay.
HTTP_TIMEOUT_SECONDS = float(os.getenv("HTTP_TIMEOUT_SECONDS", "360"))
WORKFLOW_BAGGAGE_KEY = "workflow.name"

# In-memory stores for CQRS / Inbox-Outbox demos (per pod).
WRITE_MODEL: dict[str, dict] = {}
READ_MODEL: dict[str, dict] = {}
OUTBOX: list[dict] = []
INBOX: dict[str, dict] = {}


def init_telemetry():
    """Configures global propagation (W3C TraceContext + Baggage) and OTLP exporter."""
    set_global_textmap(
        CompositePropagator([
            TraceContextTextMapPropagator(),
            W3CBaggagePropagator(),
        ])
    )

    resource = Resource.create({SERVICE_NAME: SERVICE_NAME_ENV})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=OTEL_ENDPOINT, insecure=True)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_user_baggage() -> str:
    current = baggage.get_baggage("user.id")
    if current:
        return current
    current = f"user-session-{random.randint(1000, 9999)}"
    context.attach(baggage.set_baggage("user.id", current))
    return current


def _set_baggage(key: str, value: str) -> None:
    context.attach(baggage.set_baggage(key, value))


def _mark_downstream_effects_consumed(result: dict | None) -> None:
    """Prevent repeat fault/delay on follow-up calls to the same downstream service.

    Consumed flags are set only in the downstream request context and are not
    returned to the caller, so orchestrators must mirror them locally after a
    response that already applied the demo effects.
    """
    if not isinstance(result, dict):
        return
    service = result.get("service")
    if not service:
        return
    if result.get("delay_triggered"):
        _set_baggage(DELAY_CONSUMED_BAGGAGE_KEY, service)
    if result.get("fault_triggered"):
        _set_baggage(FAULT_CONSUMED_BAGGAGE_KEY, service)


async def _http_json(method: str, url: str, payload: dict | None = None) -> dict:
    headers = {}
    inject(headers)
    headers["Content-Type"] = "application/json"
    timeout = ClientTimeout(total=HTTP_TIMEOUT_SECONDS)
    async with ClientSession(timeout=timeout) as session:
        async with session.request(method, url, json=payload, headers=headers) as resp:
            body = await resp.json(content_type=None)
            result = body if isinstance(body, dict) else {"data": body}
            if resp.status >= 400:
                # Demo faults intentionally return 5xx so Kiali/Istio show errors,
                # but the JSON payload must still propagate so workflows continue.
                if isinstance(result, dict) and result.get("fault_triggered"):
                    return result
                raise RuntimeError(f"{method} {url} failed ({resp.status}): {body}")
            return result


def _json_response(payload: dict, *, failed: bool = False) -> web.Response:
    """Return JSON; use HTTP 500 when a demo fault was triggered (visible in Kiali)."""
    return web.json_response(payload, status=500 if failed else 200)


@web.middleware
async def otel_middleware(request, handler):
    """Extracts traceparent and baggage from incoming request headers and attaches context."""
    # Start from an empty context so concurrent requests never inherit sibling baggage.
    extracted_ctx = extract(request.headers, context=Context())
    token = context.attach(extracted_ctx)
    tracer = trace.get_tracer(__name__)

    try:
        with tracer.start_as_current_span(
            f"HTTP {request.method} {request.path}",
            kind=trace.SpanKind.SERVER,
        ) as span:
            span.set_attribute("http.method", request.method)
            span.set_attribute("http.target", request.path)
            span.set_attribute("service.name", SERVICE_NAME_ENV)

            response = await handler(request)
            span.set_attribute("http.status_code", response.status)
            if response.status >= 400:
                span.set_status(Status(StatusCode.ERROR, f"HTTP {response.status}"))
            return response
    finally:
        context.detach(token)


def _apply_fault_delay_from_payload(
    payload: dict | None,
    candidates: list[str],
    delay_min: float,
    delay_max: float,
) -> tuple[str | None, str | None, float]:
    """Resolve fault/delay targets from request JSON/baggage or pick at random once.

    Explicit values:
      - "none": disable fault/delay for this workflow
      - "random": pick a candidate service once and propagate via baggage
      - <service>: pin fault/delay to that service
    """
    payload = payload or {}

    fault_service = baggage.get_baggage(FAULT_BAGGAGE_KEY)
    if not fault_service:
        fault_service = payload.get("fault_service")
        if fault_service in (None, ""):
            fault_service = "random"
        if fault_service == "none":
            fault_service = None
            _set_baggage(FAULT_BAGGAGE_KEY, "none")
        elif fault_service == "random":
            fault_service = random.choice(candidates) if candidates else None
            if fault_service:
                _set_baggage(FAULT_BAGGAGE_KEY, fault_service)
        elif fault_service:
            _set_baggage(FAULT_BAGGAGE_KEY, fault_service)
    elif fault_service == "none":
        fault_service = None

    delay_service = baggage.get_baggage(DELAY_SERVICE_BAGGAGE_KEY)
    delay_seconds_raw = baggage.get_baggage(DELAY_SECONDS_BAGGAGE_KEY)
    if not delay_service:
        delay_service = payload.get("delay_service")
        if delay_service in (None, ""):
            delay_service = "random"
        if delay_service == "none":
            delay_service = None
            delay_seconds = 0.0
            _set_baggage(DELAY_SERVICE_BAGGAGE_KEY, "none")
            _set_baggage(DELAY_SECONDS_BAGGAGE_KEY, "0")
        else:
            if delay_service == "random":
                delay_service = random.choice(candidates) if candidates else None
            delay_seconds = payload.get("delay_seconds")
            if delay_seconds is None:
                delay_seconds = round(random.uniform(delay_min, delay_max), 3)
            else:
                delay_seconds = float(delay_seconds)
                delay_seconds = min(delay_max, max(delay_min, delay_seconds))
            if delay_service:
                _set_baggage(DELAY_SERVICE_BAGGAGE_KEY, delay_service)
                _set_baggage(DELAY_SECONDS_BAGGAGE_KEY, str(delay_seconds))
            else:
                delay_seconds = 0.0
    elif delay_service == "none":
        delay_service = None
        delay_seconds = 0.0
    else:
        try:
            delay_seconds = float(delay_seconds_raw) if delay_seconds_raw else 0.0
        except (TypeError, ValueError):
            delay_seconds = 0.0

    return fault_service, delay_service, delay_seconds


async def _run_business_effects(fault_service, delay_service, delay_seconds):
    tracer = trace.get_tracer(__name__)
    fault_consumed = baggage.get_baggage(FAULT_CONSUMED_BAGGAGE_KEY)
    delay_consumed = baggage.get_baggage(DELAY_CONSUMED_BAGGAGE_KEY)
    should_fail = (
        fault_service == SERVICE_NAME_ENV and fault_consumed != SERVICE_NAME_ENV
    )
    should_delay = (
        delay_service == SERVICE_NAME_ENV
        and delay_seconds > 0
        and delay_consumed != SERVICE_NAME_ENV
    )

    with tracer.start_as_current_span("process_business_logic") as sub_span:
        sub_span.set_attribute("fault.service", fault_service or "")
        sub_span.set_attribute("fault.triggered", should_fail)
        sub_span.set_attribute("delay.service", delay_service or "")
        sub_span.set_attribute("delay.seconds", delay_seconds)
        sub_span.set_attribute("delay.triggered", should_delay)

        if should_delay:
            with tracer.start_as_current_span("artificial_delay") as delay_span:
                delay_span.set_attribute("delay.seconds", delay_seconds)
                await asyncio.sleep(delay_seconds)
            _set_baggage(DELAY_CONSUMED_BAGGAGE_KEY, SERVICE_NAME_ENV)

        if should_fail:
            try:
                raise RuntimeError(f"Random fault triggered in {SERVICE_NAME_ENV}")
            except Exception as err:
                sub_span.record_exception(err)
                sub_span.set_status(Status(StatusCode.ERROR, str(err)))
            _set_baggage(FAULT_CONSUMED_BAGGAGE_KEY, SERVICE_NAME_ENV)

    return should_fail, should_delay


def _effects_fields(fault_service, delay_service, delay_seconds, should_fail, should_delay):
    return {
        "fault_service": fault_service,
        "fault_triggered": should_fail,
        "delay_service": delay_service,
        "delay_seconds": delay_seconds,
        "delay_triggered": should_delay,
    }


async def handle_chain(request: web.Request) -> web.Response:
    """Synchronous multi-hop chain with optional fault/delay baggage."""
    tracer = trace.get_tracer(__name__)
    _ensure_user_baggage()
    _set_baggage(WORKFLOW_BAGGAGE_KEY, "chain")

    payload = {}
    if request.method == "POST":
        try:
            payload = await request.json()
        except Exception:
            payload = {}

    fault_service, delay_service, delay_seconds = _apply_fault_delay_from_payload(
        payload,
        CHAIN_SERVICES,
        CHAIN_DELAY_MIN_SECONDS + 0.01,
        CHAIN_DELAY_MAX_SECONDS,
    )
    should_fail, should_delay = await _run_business_effects(
        fault_service, delay_service, delay_seconds
    )

    downstream_response = None
    if NEXT_APP:
        next_url = f"{NEXT_APP.rstrip('/')}/api/chain"
        with tracer.start_as_current_span("call_downstream_service") as client_span:
            client_span.set_attribute("http.url", next_url)
            try:
                downstream_response = await _http_json("GET", next_url)
                downstream_failed = bool(
                    isinstance(downstream_response, dict)
                    and downstream_response.get("fault_triggered")
                )
                status_code = 500 if downstream_failed else 200
                client_span.set_attribute("http.status_code", status_code)
                if downstream_failed:
                    client_span.set_status(
                        Status(StatusCode.ERROR, "downstream fault triggered")
                    )
            except Exception as err:
                client_span.record_exception(err)
                client_span.set_status(Status(StatusCode.ERROR, str(err)))
                downstream_response = {"error": f"Downstream request failed: {str(err)}"}

    return _json_response({
        "workflow": "chain",
        "service": SERVICE_NAME_ENV,
        "baggage_user_id": baggage.get_baggage("user.id"),
        "fault_service": fault_service,
        "fault_triggered": should_fail,
        "delay_service": delay_service,
        "delay_seconds": delay_seconds,
        "delay_triggered": should_delay,
        "next_app": NEXT_APP,
        "downstream": downstream_response,
    }, failed=should_fail)


async def handle_cqrs_command_local(request: web.Request) -> web.Response:
    """Write-side: persist command in write model and project event to query service."""
    tracer = trace.get_tracer(__name__)
    _ensure_user_baggage()
    _set_baggage(WORKFLOW_BAGGAGE_KEY, "cqrs")
    payload = await request.json()

    fault_service, delay_service, delay_seconds = _apply_fault_delay_from_payload(
        payload,
        CQRS_PARTICIPANTS,
        PATTERN_DELAY_MIN_SECONDS,
        PATTERN_DELAY_MAX_SECONDS,
    )
    should_fail, should_delay = await _run_business_effects(
        fault_service, delay_service, delay_seconds
    )

    order_id = payload.get("order_id") or str(uuid.uuid4())
    item = payload.get("item", "demo-item")
    quantity = int(payload.get("quantity", 1))

    with tracer.start_as_current_span("cqrs.write_model.append") as span:
        record = {
            "order_id": order_id,
            "item": item,
            "quantity": quantity,
            "status": "accepted",
            "written_by": SERVICE_NAME_ENV,
            "written_at": _utcnow(),
        }
        WRITE_MODEL[order_id] = record
        span.set_attribute("cqrs.order_id", order_id)
        span.set_attribute("cqrs.side", "command")

    event = {
        "type": "OrderAccepted",
        "order_id": order_id,
        "item": item,
        "quantity": quantity,
        "source": SERVICE_NAME_ENV,
        "emitted_at": _utcnow(),
    }

    projection = None
    with tracer.start_as_current_span("cqrs.publish_event") as span:
        span.set_attribute("cqrs.event_type", event["type"])
        span.set_attribute("cqrs.query_url", CQRS_QUERY_URL)
        try:
            projection = await _http_json(
                "POST",
                f"{CQRS_QUERY_URL.rstrip('/')}/api/cqrs/project",
                event,
            )
        except Exception as err:
            span.record_exception(err)
            span.set_status(Status(StatusCode.ERROR, str(err)))
            projection = {"error": str(err)}

    return _json_response({
        "workflow": "cqrs",
        "side": "command",
        "service": SERVICE_NAME_ENV,
        "write_model": WRITE_MODEL.get(order_id),
        "event": event,
        "projection": projection,
        **_effects_fields(
            fault_service, delay_service, delay_seconds, should_fail, should_delay
        ),
    }, failed=should_fail)


async def handle_cqrs_project(request: web.Request) -> web.Response:
    """Read-side projection: apply command event into the query model."""
    tracer = trace.get_tracer(__name__)
    _ensure_user_baggage()
    _set_baggage(WORKFLOW_BAGGAGE_KEY, "cqrs")
    event = await request.json()
    order_id = event["order_id"]

    fault_service, delay_service, delay_seconds = _apply_fault_delay_from_payload(
        event,
        CQRS_PARTICIPANTS,
        PATTERN_DELAY_MIN_SECONDS,
        PATTERN_DELAY_MAX_SECONDS,
    )
    should_fail, should_delay = await _run_business_effects(
        fault_service, delay_service, delay_seconds
    )

    with tracer.start_as_current_span("cqrs.read_model.project") as span:
        view = {
            "order_id": order_id,
            "item": event.get("item"),
            "quantity": event.get("quantity"),
            "status": "projected",
            "projected_by": SERVICE_NAME_ENV,
            "projected_at": _utcnow(),
            "source_event": event.get("type"),
        }
        READ_MODEL[order_id] = view
        span.set_attribute("cqrs.order_id", order_id)
        span.set_attribute("cqrs.side", "query")

    return _json_response({
        "workflow": "cqrs",
        "side": "projection",
        "service": SERVICE_NAME_ENV,
        "read_model": view,
        **_effects_fields(
            fault_service, delay_service, delay_seconds, should_fail, should_delay
        ),
    }, failed=should_fail)


async def handle_cqrs_query(request: web.Request) -> web.Response:
    """Read-side query handler."""
    tracer = trace.get_tracer(__name__)
    _ensure_user_baggage()
    _set_baggage(WORKFLOW_BAGGAGE_KEY, "cqrs")
    order_id = request.match_info.get("order_id") or request.rel_url.query.get("order_id")

    fault_service, delay_service, delay_seconds = _apply_fault_delay_from_payload(
        {},
        CQRS_PARTICIPANTS,
        PATTERN_DELAY_MIN_SECONDS,
        PATTERN_DELAY_MAX_SECONDS,
    )
    should_fail, should_delay = await _run_business_effects(
        fault_service, delay_service, delay_seconds
    )

    with tracer.start_as_current_span("cqrs.read_model.query") as span:
        span.set_attribute("cqrs.order_id", order_id or "")
        span.set_attribute("cqrs.side", "query")
        view = READ_MODEL.get(order_id) if order_id else None
        if not view:
            span.set_status(Status(StatusCode.ERROR, "order not found"))
            raise web.HTTPNotFound(text=f"order {order_id} not found in read model")

    return _json_response({
        "workflow": "cqrs",
        "side": "query",
        "service": SERVICE_NAME_ENV,
        "read_model": view,
        **_effects_fields(
            fault_service, delay_service, delay_seconds, should_fail, should_delay
        ),
    }, failed=should_fail)


async def handle_cqrs_start(request: web.Request) -> web.Response:
    """Orchestrator: send command to write service, then query read service."""
    tracer = trace.get_tracer(__name__)
    _ensure_user_baggage()
    _set_baggage(WORKFLOW_BAGGAGE_KEY, "cqrs")
    payload = await request.json()

    fault_service, delay_service, delay_seconds = _apply_fault_delay_from_payload(
        payload,
        CQRS_PARTICIPANTS,
        PATTERN_DELAY_MIN_SECONDS,
        PATTERN_DELAY_MAX_SECONDS,
    )
    should_fail, should_delay = await _run_business_effects(
        fault_service, delay_service, delay_seconds
    )

    with tracer.start_as_current_span("cqrs.orchestrate") as span:
        command_result = await _http_json(
            "POST",
            f"{CQRS_COMMAND_URL.rstrip('/')}/api/cqrs/command",
            payload,
        )
        # Command may have delayed on order-service and/or catalog projection.
        _mark_downstream_effects_consumed(command_result)
        if isinstance(command_result, dict):
            _mark_downstream_effects_consumed(command_result.get("projection"))

        order_id = (
            command_result.get("write_model", {}).get("order_id")
            or payload.get("order_id")
        )
        span.set_attribute("cqrs.order_id", order_id or "")

        query_result = None
        if order_id:
            try:
                query_result = await _http_json(
                    "GET",
                    f"{CQRS_QUERY_URL.rstrip('/')}/api/cqrs/orders/{order_id}",
                )
            except Exception as err:
                span.record_exception(err)
                query_result = {"error": str(err)}

    return _json_response({
        "workflow": "cqrs",
        "service": SERVICE_NAME_ENV,
        "command_url": CQRS_COMMAND_URL,
        "query_url": CQRS_QUERY_URL,
        "command": command_result,
        "query": query_result,
        **_effects_fields(
            fault_service, delay_service, delay_seconds, should_fail, should_delay
        ),
    }, failed=should_fail)


async def handle_outbox_enqueue(request: web.Request) -> web.Response:
    """Transactional outbox: persist message, then relay to inbox consumer."""
    tracer = trace.get_tracer(__name__)
    _ensure_user_baggage()
    _set_baggage(WORKFLOW_BAGGAGE_KEY, "inbox-outbox")
    payload = await request.json()

    fault_service, delay_service, delay_seconds = _apply_fault_delay_from_payload(
        payload,
        OUTBOX_PARTICIPANTS,
        PATTERN_DELAY_MIN_SECONDS,
        PATTERN_DELAY_MAX_SECONDS,
    )
    should_fail, should_delay = await _run_business_effects(
        fault_service, delay_service, delay_seconds
    )

    message_id = payload.get("message_id") or str(uuid.uuid4())
    body = payload.get("body", {"hello": "outbox"})

    with tracer.start_as_current_span("outbox.append") as span:
        entry = {
            "message_id": message_id,
            "body": body,
            "status": "pending",
            "created_by": SERVICE_NAME_ENV,
            "created_at": _utcnow(),
        }
        OUTBOX.append(entry)
        span.set_attribute("outbox.message_id", message_id)

    delivery = None
    with tracer.start_as_current_span("outbox.relay") as span:
        span.set_attribute("outbox.target", OUTBOX_TARGET_URL)
        try:
            delivery = await _http_json(
                "POST",
                f"{OUTBOX_TARGET_URL.rstrip('/')}/api/inbox/receive",
                {"message_id": message_id, "body": body, "from": SERVICE_NAME_ENV},
            )
            entry["status"] = "dispatched"
            entry["dispatched_at"] = _utcnow()
        except Exception as err:
            entry["status"] = "failed"
            span.record_exception(err)
            span.set_status(Status(StatusCode.ERROR, str(err)))
            delivery = {"error": str(err)}

    return _json_response({
        "workflow": "inbox-outbox",
        "side": "outbox",
        "service": SERVICE_NAME_ENV,
        "outbox_entry": entry,
        "delivery": delivery,
        "outbox_size": len(OUTBOX),
        **_effects_fields(
            fault_service, delay_service, delay_seconds, should_fail, should_delay
        ),
    }, failed=should_fail)


async def handle_inbox_receive(request: web.Request) -> web.Response:
    """Inbox: idempotent accept + process of an outbox message."""
    tracer = trace.get_tracer(__name__)
    _ensure_user_baggage()
    _set_baggage(WORKFLOW_BAGGAGE_KEY, "inbox-outbox")
    payload = await request.json()
    message_id = payload["message_id"]

    fault_service, delay_service, delay_seconds = _apply_fault_delay_from_payload(
        payload,
        OUTBOX_PARTICIPANTS,
        PATTERN_DELAY_MIN_SECONDS,
        PATTERN_DELAY_MAX_SECONDS,
    )

    # Fast-path duplicates before fault/delay so the idempotent replay cannot
    # re-apply a multi-minute sleep (which would exceed HTTP/gunicorn timeouts
    # when CQRS and Outbox run concurrently).
    existing = INBOX.get(message_id)
    if existing:
        with tracer.start_as_current_span("inbox.receive") as span:
            span.set_attribute("inbox.message_id", message_id)
            span.set_attribute("inbox.duplicate", True)
        return _json_response({
            "workflow": "inbox-outbox",
            "side": "inbox",
            "service": SERVICE_NAME_ENV,
            "duplicate": True,
            "inbox_entry": existing,
            **_effects_fields(
                fault_service, delay_service, delay_seconds, False, False
            ),
        })

    should_fail, should_delay = await _run_business_effects(
        fault_service, delay_service, delay_seconds
    )

    with tracer.start_as_current_span("inbox.receive") as span:
        span.set_attribute("inbox.message_id", message_id)
        # Re-check after the delay window; a concurrent delivery may have won.
        existing = INBOX.get(message_id)
        if existing:
            span.set_attribute("inbox.duplicate", True)
            return _json_response({
                "workflow": "inbox-outbox",
                "side": "inbox",
                "service": SERVICE_NAME_ENV,
                "duplicate": True,
                "inbox_entry": existing,
                **_effects_fields(
                    fault_service, delay_service, delay_seconds, should_fail, should_delay
                ),
            }, failed=should_fail)

        with tracer.start_as_current_span("inbox.process") as process_span:
            process_span.set_attribute("inbox.message_id", message_id)
            entry = {
                "message_id": message_id,
                "body": payload.get("body"),
                "from": payload.get("from"),
                "status": "processed",
                "received_by": SERVICE_NAME_ENV,
                "received_at": _utcnow(),
            }
            INBOX[message_id] = entry

    return _json_response({
        "workflow": "inbox-outbox",
        "side": "inbox",
        "service": SERVICE_NAME_ENV,
        "duplicate": False,
        "inbox_entry": entry,
        "inbox_size": len(INBOX),
        **_effects_fields(
            fault_service, delay_service, delay_seconds, should_fail, should_delay
        ),
    }, failed=should_fail)


async def handle_outbox_start(request: web.Request) -> web.Response:
    """Orchestrator entry for inbox/outbox demo."""
    _ensure_user_baggage()
    _set_baggage(WORKFLOW_BAGGAGE_KEY, "inbox-outbox")
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    return await _outbox_orchestrate(payload)


async def _outbox_orchestrate(payload: dict) -> web.Response:
    tracer = trace.get_tracer(__name__)
    message_id = payload.get("message_id") or str(uuid.uuid4())
    body = payload.get("body", {"hello": "outbox", "note": payload.get("note", "demo")})

    fault_service, delay_service, delay_seconds = _apply_fault_delay_from_payload(
        payload,
        OUTBOX_PARTICIPANTS,
        PATTERN_DELAY_MIN_SECONDS,
        PATTERN_DELAY_MAX_SECONDS,
    )
    should_fail, should_delay = await _run_business_effects(
        fault_service, delay_service, delay_seconds
    )

    with tracer.start_as_current_span("outbox.append") as span:
        entry = {
            "message_id": message_id,
            "body": body,
            "status": "pending",
            "created_by": SERVICE_NAME_ENV,
            "created_at": _utcnow(),
        }
        OUTBOX.append(entry)
        span.set_attribute("outbox.message_id", message_id)

    delivery = None
    with tracer.start_as_current_span("outbox.relay") as span:
        span.set_attribute("outbox.target", OUTBOX_TARGET_URL)
        try:
            delivery = await _http_json(
                "POST",
                f"{OUTBOX_TARGET_URL.rstrip('/')}/api/inbox/receive",
                {"message_id": message_id, "body": body, "from": SERVICE_NAME_ENV},
            )
            entry["status"] = "dispatched"
            entry["dispatched_at"] = _utcnow()
            _mark_downstream_effects_consumed(delivery)
        except Exception as err:
            entry["status"] = "failed"
            span.record_exception(err)
            span.set_status(Status(StatusCode.ERROR, str(err)))
            delivery = {"error": str(err)}

    # Second delivery demonstrates inbox idempotency (no second multi-minute delay).
    duplicate = None
    with tracer.start_as_current_span("outbox.relay_duplicate") as span:
        try:
            duplicate = await _http_json(
                "POST",
                f"{OUTBOX_TARGET_URL.rstrip('/')}/api/inbox/receive",
                {"message_id": message_id, "body": body, "from": SERVICE_NAME_ENV},
            )
        except Exception as err:
            span.record_exception(err)
            duplicate = {"error": str(err)}

    return _json_response({
        "workflow": "inbox-outbox",
        "service": SERVICE_NAME_ENV,
        "target_url": OUTBOX_TARGET_URL,
        "outbox_entry": entry,
        "delivery": delivery,
        "duplicate_delivery": duplicate,
        "outbox_size": len(OUTBOX),
        **_effects_fields(
            fault_service, delay_service, delay_seconds, should_fail, should_delay
        ),
    }, failed=should_fail)


async def handle_meta(request: web.Request) -> web.Response:
    return web.json_response({
        "service": SERVICE_NAME_ENV,
        "chain_services": CHAIN_SERVICES,
        "cqrs_participants": CQRS_PARTICIPANTS,
        "outbox_participants": OUTBOX_PARTICIPANTS,
        "chain_delay_seconds": {
            "min": CHAIN_DELAY_MIN_SECONDS + 0.01,
            "max": CHAIN_DELAY_MAX_SECONDS,
        },
        "pattern_delay_seconds": {
            "min": PATTERN_DELAY_MIN_SECONDS,
            "max": PATTERN_DELAY_MAX_SECONDS,
        },
        "cqrs_command_url": CQRS_COMMAND_URL,
        "cqrs_query_url": CQRS_QUERY_URL,
        "outbox_target_url": OUTBOX_TARGET_URL,
        "next_app": NEXT_APP,
        "workflows": ["chain", "cqrs", "inbox-outbox"],
    })


async def handle_ui(request: web.Request) -> web.Response:
    index = STATIC_DIR / "index.html"
    if not index.exists():
        raise web.HTTPNotFound(text="UI not found")
    return web.FileResponse(index)


async def create_app():
    """Async app factory for Gunicorn / aiohttp."""
    init_telemetry()
    app = web.Application(middlewares=[otel_middleware])

    # UI + discovery
    app.router.add_get("/", handle_ui)
    app.router.add_get("/ui", handle_ui)
    app.router.add_get("/api/meta", handle_meta)
    if STATIC_DIR.exists():
        app.router.add_static("/static/", path=str(STATIC_DIR), name="static")

    # Existing multi-hop chain (fault + delay via baggage)
    app.router.add_get("/api/chain", handle_chain)
    app.router.add_post("/api/chain", handle_chain)
    # Backward-compatible chain entry used by downstream NEXT_APP hops
    app.router.add_get("/chain", handle_chain)

    # CQRS
    app.router.add_post("/api/cqrs/start", handle_cqrs_start)
    app.router.add_post("/api/cqrs/command", handle_cqrs_command_local)
    app.router.add_post("/api/cqrs/project", handle_cqrs_project)
    app.router.add_get("/api/cqrs/orders/{order_id}", handle_cqrs_query)

    # Inbox / Outbox
    app.router.add_post("/api/outbox/start", handle_outbox_start)
    app.router.add_post("/api/outbox/enqueue", handle_outbox_enqueue)
    app.router.add_post("/api/inbox/receive", handle_inbox_receive)

    return app


if __name__ == "__main__":
    web.run_app(create_app(), host="0.0.0.0", port=PORT)
