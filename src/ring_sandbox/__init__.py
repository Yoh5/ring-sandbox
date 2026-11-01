"""ring-sandbox: typed client + offline emulator for the Ring Partner API."""

from .client import PRODUCTION_BASE_URL, RingAPIError, RingClient, WhepSession
from .models import (
    AppIntegration,
    Capabilities,
    Device,
    DeviceBundle,
    HistoryEvent,
    MediaClip,
    MotionSubType,
    Snapshot,
    Status,
    Subscription,
    User,
    WebhookEvent,
    WebhookEventType,
)
from .webhooks import SignatureError, sign, verify

__all__ = [
    "PRODUCTION_BASE_URL",
    "AppIntegration",
    "Capabilities",
    "Device",
    "DeviceBundle",
    "HistoryEvent",
    "MediaClip",
    "MotionSubType",
    "RingAPIError",
    "RingClient",
    "SignatureError",
    "Snapshot",
    "Status",
    "Subscription",
    "User",
    "WebhookEvent",
    "WebhookEventType",
    "WhepSession",
    "sign",
    "verify",
]
__version__ = "0.5.0"
