"""Fail-closed Huaxiaobao adapter for XianyuAutoAgent.

This process deliberately exposes draft generation and durable operation state,
but never imports or calls the native websocket send path.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
import uuid
from collections.abc import Callable, Iterable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

REQUEST_SCHEMA = "foundry.huaxiaobao.tool-request.v1"
RESULT_SCHEMA = "foundry.huaxiaobao.tool-result.v1"
EXECUTOR_ID = "XianyuAutoAgent"
SENSITIVE_KEYS = ("cookie", "token", "password", "secret", "authorization")
SEVEN_COST_KEYS = frozenset(
    {
        "acquisition",
        "model",
        "data",
        "human",
        "delivery",
        "support",
        "risk",
    }
)
def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_utc(value: str, name: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, ValueError) as exc:
        raise ValueError(f"{name} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{name} must include a timezone")
    return parsed.astimezone(timezone.utc)


def _canonical_hash(value: Dict[str, Any]) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _stable_ref(kind: str, *parts: str) -> str:
    digest = hashlib.sha256("\x00".join(parts).encode("utf-8")).hexdigest()[:24]
    return f"xianyu:{kind}:{digest}"


def opaque_native_ref(kind: str, *parts: str) -> str:
    """Return an opaque reference for a native identifier kept inside the tool boundary."""

    return _stable_ref(f"native-{kind}", *parts)


def _contains_sensitive_key(value: Any) -> bool:
    if isinstance(value, dict):
        for key, child in value.items():
            normalized = str(key).lower()
            if any(fragment in normalized for fragment in SENSITIVE_KEYS):
                return True
            if _contains_sensitive_key(child):
                return True
    elif isinstance(value, list):
        return any(_contains_sensitive_key(child) for child in value)
    return False


def descriptor() -> Dict[str, Any]:
    return {
        "schema_version": "foundry.huaxiaobao.capability-descriptor.v1",
        "executor_id": EXECUTOR_ID,
        "capabilities": [
            {
                "id": "account.status",
                "side_effect": "none",
                "external_action": False,
                "retry_safe": True,
            },
            {
                "id": "inquiry.read",
                "side_effect": "none",
                "external_action": False,
                "retry_safe": True,
            },
            {
                "id": "reply.draft.generate",
                "side_effect": "external_model_data_egress",
                "external_action": False,
                "retry_safe": True,
            },
            {
                "id": "quote.constrain",
                "side_effect": "none",
                "external_action": False,
                "retry_safe": True,
            },
            {
                "id": "reply.send",
                "side_effect": "external_customer_message",
                "external_action": True,
                "available": False,
                "reason": "发送必须由 Huaxiaobao 的独立获批能力执行",
            },
            {
                "id": "operation.query",
                "side_effect": "none",
                "external_action": False,
                "retry_safe": True,
            },
        ],
    }


class OperationJournal:
    """Small durable idempotency journal; it contains no platform credentials."""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(str(path), timeout=5)
        self.connection.execute("PRAGMA busy_timeout = 5000")
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.execute("PRAGMA journal_mode = WAL")
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS huaxiaobao_operations (
                operation_id TEXT PRIMARY KEY,
                request_hash TEXT NOT NULL,
                result_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS xianyu_native_inquiries (
                event_identity TEXT PRIMARY KEY,
                event_hash TEXT NOT NULL,
                account_ref TEXT NOT NULL,
                conversation_ref TEXT NOT NULL,
                message_ref TEXT NOT NULL,
                message_revision TEXT NOT NULL,
                event_json TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(account_ref, conversation_ref, message_ref, message_revision)
            )
            """
        )
        self.connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_xianyu_native_inquiries_account_observed
            ON xianyu_native_inquiries (account_ref, observed_at)
            """
        )
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS xianyu_conversation_takeovers (
                account_ref TEXT NOT NULL,
                conversation_ref TEXT NOT NULL,
                paused INTEGER NOT NULL CHECK(paused IN (0, 1)),
                state_revision INTEGER NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(account_ref, conversation_ref)
            )
            """
        )
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS xianyu_inquiry_processing (
                event_identity TEXT PRIMARY KEY,
                account_ref TEXT NOT NULL,
                conversation_ref TEXT NOT NULL,
                message_ref TEXT NOT NULL,
                message_revision TEXT NOT NULL,
                status TEXT NOT NULL
                    CHECK(status IN ('OBSERVED', 'PROCESSING', 'PROCESSED')),
                processing_revision INTEGER NOT NULL DEFAULT 0,
                claimant_ref TEXT,
                lease_ref TEXT,
                lease_expires_at TEXT,
                ack_hash TEXT,
                ack_json TEXT,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(event_identity)
                    REFERENCES xianyu_native_inquiries(event_identity),
                UNIQUE(account_ref, conversation_ref, message_ref, message_revision)
            )
            """
        )
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS xianyu_takeover_commands (
                command_identity TEXT PRIMARY KEY,
                command_hash TEXT NOT NULL,
                account_ref TEXT NOT NULL,
                conversation_ref TEXT NOT NULL,
                message_ref TEXT NOT NULL,
                message_revision TEXT NOT NULL,
                desired_paused INTEGER NOT NULL CHECK(desired_paused IN (0, 1)),
                command_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(account_ref, message_ref, message_revision)
            )
            """
        )
        now = _utc_now()
        self.connection.execute(
            """
            INSERT OR IGNORE INTO xianyu_inquiry_processing (
                event_identity, account_ref, conversation_ref, message_ref,
                message_revision, status, processing_revision, updated_at
            )
            SELECT event_identity, account_ref, conversation_ref, message_ref,
                   message_revision, 'OBSERVED', 0, ?
            FROM xianyu_native_inquiries
            """,
            (now,),
        )
        self.connection.commit()

    def get(self, operation_id: str) -> Optional[Dict[str, Any]]:
        row = self.connection.execute(
            "SELECT request_hash, result_json FROM huaxiaobao_operations "
            "WHERE operation_id = ?",
            (operation_id,),
        ).fetchone()
        if row is None:
            return None
        return {"request_hash": row[0], "result": json.loads(row[1])}

    def put(self, operation_id: str, request_hash: str, result: Dict[str, Any]) -> None:
        self.connection.execute(
            "INSERT INTO huaxiaobao_operations "
            "(operation_id, request_hash, result_json, created_at) VALUES (?, ?, ?, ?)",
            (
                operation_id,
                request_hash,
                json.dumps(result, ensure_ascii=False, sort_keys=True),
                _utc_now(),
            ),
        )
        self.connection.commit()

    @staticmethod
    def _require_non_empty(value: str, name: str) -> None:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{name} must be a non-empty string")

    @staticmethod
    def _inquiry_event(
        *,
        account_ref: str,
        conversation_ref: str,
        message_ref: str,
        message_revision: str,
        sender_ref: str,
        item_ref: str,
        text: str,
        observed_at: str,
    ) -> dict[str, str]:
        event = {
            "account_ref": account_ref,
            "conversation_ref": conversation_ref,
            "message_ref": message_ref,
            "message_revision": message_revision,
            "sender_ref": sender_ref,
            "item_ref": item_ref,
            "text": text,
            "observed_at": observed_at,
        }
        for name, value in event.items():
            OperationJournal._require_non_empty(value, name)
        try:
            timestamp = datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("observed_at must be an ISO-8601 timestamp") from exc
        if timestamp.tzinfo is None:
            raise ValueError("observed_at must include a timezone")
        return event

    def record_native_inquiry(
        self,
        *,
        account_ref: str,
        conversation_ref: str,
        message_ref: str,
        message_revision: str,
        sender_ref: str,
        item_ref: str,
        text: str,
        observed_at: str,
    ) -> dict[str, Any]:
        event = self._inquiry_event(
            account_ref=account_ref,
            conversation_ref=conversation_ref,
            message_ref=message_ref,
            message_revision=message_revision,
            sender_ref=sender_ref,
            item_ref=item_ref,
            text=text,
            observed_at=observed_at,
        )
        event_identity = _stable_ref(
            "native-inquiry",
            account_ref,
            conversation_ref,
            message_ref,
            message_revision,
        )
        event_hash = _canonical_hash(event)
        serialized = json.dumps(event, ensure_ascii=False, sort_keys=True)

        self.connection.execute("BEGIN IMMEDIATE")
        try:
            existing = self.connection.execute(
                "SELECT event_hash, event_json FROM xianyu_native_inquiries "
                "WHERE event_identity = ?",
                (event_identity,),
            ).fetchone()
            if existing is not None:
                if existing[0] != event_hash:
                    raise NativeInquiryIdentityConflict(event_identity)
                self.connection.execute(
                    "INSERT OR IGNORE INTO xianyu_inquiry_processing "
                    "(event_identity, account_ref, conversation_ref, message_ref, "
                    "message_revision, status, processing_revision, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, 'OBSERVED', 0, ?)",
                    (
                        event_identity,
                        account_ref,
                        conversation_ref,
                        message_ref,
                        message_revision,
                        _utc_now(),
                    ),
                )
                self.connection.commit()
                return {
                    "event_identity": event_identity,
                    "event_hash": event_hash,
                    "event": json.loads(existing[1]),
                    "replayed": True,
                }

            conflicting = self.connection.execute(
                "SELECT event_identity FROM xianyu_native_inquiries "
                "WHERE message_ref = ? LIMIT 1",
                (message_ref,),
            ).fetchone()
            if conflicting is not None:
                raise NativeInquiryBindingConflict(conflicting[0])

            self.connection.execute(
                "INSERT INTO xianyu_native_inquiries "
                "(event_identity, event_hash, account_ref, conversation_ref, "
                "message_ref, message_revision, event_json, observed_at, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    event_identity,
                    event_hash,
                    account_ref,
                    conversation_ref,
                    message_ref,
                    message_revision,
                    serialized,
                    observed_at,
                    _utc_now(),
                ),
            )
            self.connection.execute(
                "INSERT INTO xianyu_inquiry_processing "
                "(event_identity, account_ref, conversation_ref, message_ref, "
                "message_revision, status, processing_revision, updated_at) "
                "VALUES (?, ?, ?, ?, ?, 'OBSERVED', 0, ?)",
                (
                    event_identity,
                    account_ref,
                    conversation_ref,
                    message_ref,
                    message_revision,
                    _utc_now(),
                ),
            )
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise
        return {
            "event_identity": event_identity,
            "event_hash": event_hash,
            "event": event,
            "replayed": False,
        }

    def get_native_inquiry(
        self,
        *,
        account_ref: str,
        conversation_ref: str,
        message_ref: str,
        message_revision: str,
    ) -> dict[str, Any] | None:
        event_identity = _stable_ref(
            "native-inquiry",
            account_ref,
            conversation_ref,
            message_ref,
            message_revision,
        )
        row = self.connection.execute(
            "SELECT event_hash, event_json FROM xianyu_native_inquiries "
            "WHERE event_identity = ?",
            (event_identity,),
        ).fetchone()
        if row is None:
            conflicting = self.connection.execute(
                "SELECT event_identity FROM xianyu_native_inquiries "
                "WHERE message_ref = ? LIMIT 1",
                (message_ref,),
            ).fetchone()
            if conflicting is not None:
                raise NativeInquiryBindingConflict(conflicting[0])
            return None
        return {
            "event_identity": event_identity,
            "event_hash": row[0],
            "event": json.loads(row[1]),
        }

    def _bound_native_inquiry(
        self,
        *,
        account_ref: str,
        conversation_ref: str,
        message_ref: str,
        message_revision: str,
    ) -> tuple[str, str, dict[str, Any]]:
        for name, value in {
            "account_ref": account_ref,
            "conversation_ref": conversation_ref,
            "message_ref": message_ref,
            "message_revision": message_revision,
        }.items():
            self._require_non_empty(value, name)
        event_identity = _stable_ref(
            "native-inquiry",
            account_ref,
            conversation_ref,
            message_ref,
            message_revision,
        )
        row = self.connection.execute(
            "SELECT event_identity, event_hash, event_json "
            "FROM xianyu_native_inquiries WHERE event_identity = ?",
            (event_identity,),
        ).fetchone()
        if row is not None:
            return row[0], row[1], json.loads(row[2])
        conflicting = self.connection.execute(
            "SELECT event_identity FROM xianyu_native_inquiries "
            "WHERE message_ref = ? LIMIT 1",
            (message_ref,),
        ).fetchone()
        if conflicting is not None:
            raise NativeInquiryBindingConflict(conflicting[0])
        raise NativeInquiryNotFound(event_identity)

    def inquiry_processing_state(
        self,
        *,
        account_ref: str,
        conversation_ref: str,
        message_ref: str,
        message_revision: str,
    ) -> dict[str, Any]:
        event_identity, _, _ = self._bound_native_inquiry(
            account_ref=account_ref,
            conversation_ref=conversation_ref,
            message_ref=message_ref,
            message_revision=message_revision,
        )
        row = self.connection.execute(
            "SELECT status, processing_revision, claimant_ref, lease_ref, "
            "lease_expires_at, ack_hash, ack_json, updated_at "
            "FROM xianyu_inquiry_processing WHERE event_identity = ?",
            (event_identity,),
        ).fetchone()
        if row is None:
            raise NativeInquiryNotFound(event_identity)
        return {
            "event_identity": event_identity,
            "status": row[0],
            "processing_revision": row[1],
            "claimant_ref": row[2],
            "lease_ref": row[3],
            "lease_expires_at": row[4],
            "ack_hash": row[5],
            "ack": json.loads(row[6]) if row[6] else None,
            "updated_at": row[7],
        }

    def claim_native_inquiry(
        self,
        *,
        account_ref: str,
        conversation_ref: str,
        message_ref: str,
        message_revision: str,
        claimant_ref: str,
        lease_seconds: int = 60,
        now: str | None = None,
    ) -> dict[str, Any]:
        self._require_non_empty(claimant_ref, "claimant_ref")
        if isinstance(lease_seconds, bool) or not isinstance(lease_seconds, int):
            raise TypeError("lease_seconds must be an integer")
        if not 1 <= lease_seconds <= 3600:
            raise ValueError("lease_seconds must be between 1 and 3600")
        claimed_at = _parse_utc(now or _utc_now(), "now")

        self.connection.execute("BEGIN IMMEDIATE")
        try:
            event_identity, event_hash, event = self._bound_native_inquiry(
                account_ref=account_ref,
                conversation_ref=conversation_ref,
                message_ref=message_ref,
                message_revision=message_revision,
            )
            paused = self.connection.execute(
                "SELECT paused FROM xianyu_conversation_takeovers "
                "WHERE account_ref = ? AND conversation_ref = ?",
                (account_ref, conversation_ref),
            ).fetchone()
            if paused is not None and bool(paused[0]):
                self.connection.commit()
                return {
                    "event_identity": event_identity,
                    "event_hash": event_hash,
                    "status": "PAUSED",
                    "claimed": False,
                    "replayed": False,
                }

            row = self.connection.execute(
                "SELECT status, processing_revision, claimant_ref, lease_ref, "
                "lease_expires_at, ack_hash, ack_json "
                "FROM xianyu_inquiry_processing WHERE event_identity = ?",
                (event_identity,),
            ).fetchone()
            if row is None:
                raise NativeInquiryNotFound(event_identity)
            status, revision, current_claimant, lease_ref, expires_at, ack_hash, ack_json = row
            if status == "PROCESSED":
                self.connection.commit()
                return {
                    "event_identity": event_identity,
                    "event_hash": event_hash,
                    "status": status,
                    "claimed": False,
                    "replayed": True,
                    "processing_revision": revision,
                    "ack_hash": ack_hash,
                    "ack": json.loads(ack_json) if ack_json else None,
                }
            if status == "PROCESSING" and expires_at:
                expiry = _parse_utc(expires_at, "lease_expires_at")
                if expiry > claimed_at:
                    same_claimant = current_claimant == claimant_ref
                    self.connection.commit()
                    return {
                        "event_identity": event_identity,
                        "event_hash": event_hash,
                        "status": status,
                        "claimed": same_claimant,
                        "replayed": same_claimant,
                        "busy": not same_claimant,
                        "processing_revision": revision,
                        "claimant_ref": current_claimant,
                        "lease_ref": lease_ref if same_claimant else None,
                        "lease_expires_at": expires_at,
                        "event": event if same_claimant else None,
                    }

            revision += 1
            expires = claimed_at + timedelta(seconds=lease_seconds)
            expires_text = expires.isoformat().replace("+00:00", "Z")
            lease_ref = _stable_ref(
                "inquiry-lease",
                event_identity,
                str(revision),
                uuid.uuid4().hex,
            )
            updated_at = claimed_at.isoformat().replace("+00:00", "Z")
            self.connection.execute(
                "UPDATE xianyu_inquiry_processing SET status = 'PROCESSING', "
                "processing_revision = ?, claimant_ref = ?, lease_ref = ?, "
                "lease_expires_at = ?, ack_hash = NULL, ack_json = NULL, updated_at = ? "
                "WHERE event_identity = ?",
                (
                    revision,
                    claimant_ref,
                    lease_ref,
                    expires_text,
                    updated_at,
                    event_identity,
                ),
            )
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise
        return {
            "event_identity": event_identity,
            "event_hash": event_hash,
            "status": "PROCESSING",
            "claimed": True,
            "replayed": False,
            "busy": False,
            "processing_revision": revision,
            "claimant_ref": claimant_ref,
            "lease_ref": lease_ref,
            "lease_expires_at": expires_text,
            "event": event,
        }

    def ack_native_inquiry(
        self,
        *,
        account_ref: str,
        conversation_ref: str,
        message_ref: str,
        message_revision: str,
        lease_ref: str,
        checkpoint_ref: str,
        checkpoint_sha256: str,
        outcome: str,
        now: str | None = None,
    ) -> dict[str, Any]:
        for name, value in {
            "lease_ref": lease_ref,
            "checkpoint_ref": checkpoint_ref,
            "checkpoint_sha256": checkpoint_sha256,
            "outcome": outcome,
        }.items():
            self._require_non_empty(value, name)
        digest = checkpoint_sha256.removeprefix("sha256:")
        if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
            raise ValueError("checkpoint_sha256 must be a lowercase SHA-256 digest")
        acknowledged_at = _parse_utc(now or _utc_now(), "now")

        self.connection.execute("BEGIN IMMEDIATE")
        try:
            event_identity, event_hash, _ = self._bound_native_inquiry(
                account_ref=account_ref,
                conversation_ref=conversation_ref,
                message_ref=message_ref,
                message_revision=message_revision,
            )
            ack = {
                "event_identity": event_identity,
                "event_hash": event_hash,
                "account_ref": account_ref,
                "conversation_ref": conversation_ref,
                "message_ref": message_ref,
                "message_revision": message_revision,
                "lease_ref": lease_ref,
                "checkpoint_ref": checkpoint_ref,
                "checkpoint_sha256": f"sha256:{digest}",
                "outcome": outcome,
            }
            ack_hash = _canonical_hash(ack)
            row = self.connection.execute(
                "SELECT status, processing_revision, lease_ref, lease_expires_at, "
                "ack_hash, ack_json FROM xianyu_inquiry_processing "
                "WHERE event_identity = ?",
                (event_identity,),
            ).fetchone()
            if row is None:
                raise NativeInquiryNotFound(event_identity)
            status, revision, current_lease, expires_at, stored_ack_hash, ack_json = row
            if status == "PROCESSED":
                if current_lease != lease_ref or stored_ack_hash != ack_hash:
                    raise NativeInquiryAckConflict(event_identity)
                self.connection.commit()
                return {
                    "status": "PROCESSED",
                    "processing_revision": revision,
                    "ack_hash": ack_hash,
                    "ack": json.loads(ack_json),
                    "replayed": True,
                }
            paused = self.connection.execute(
                "SELECT paused FROM xianyu_conversation_takeovers "
                "WHERE account_ref = ? AND conversation_ref = ?",
                (account_ref, conversation_ref),
            ).fetchone()
            if paused is not None and bool(paused[0]):
                raise NativeInquiryLeaseConflict(event_identity)
            if status != "PROCESSING" or current_lease != lease_ref or not expires_at:
                raise NativeInquiryLeaseConflict(event_identity)
            if _parse_utc(expires_at, "lease_expires_at") <= acknowledged_at:
                raise NativeInquiryLeaseConflict(event_identity)

            updated_at = acknowledged_at.isoformat().replace("+00:00", "Z")
            serialized = json.dumps(ack, ensure_ascii=False, sort_keys=True)
            self.connection.execute(
                "UPDATE xianyu_inquiry_processing SET status = 'PROCESSED', "
                "ack_hash = ?, ack_json = ?, updated_at = ? WHERE event_identity = ?",
                (ack_hash, serialized, updated_at, event_identity),
            )
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise
        return {
            "status": "PROCESSED",
            "processing_revision": revision,
            "ack_hash": ack_hash,
            "ack": ack,
            "replayed": False,
        }

    def conversation_pause_state(
        self, account_ref: str, conversation_ref: str
    ) -> dict[str, Any]:
        self._require_non_empty(account_ref, "account_ref")
        self._require_non_empty(conversation_ref, "conversation_ref")
        row = self.connection.execute(
            "SELECT paused, state_revision, updated_at "
            "FROM xianyu_conversation_takeovers "
            "WHERE account_ref = ? AND conversation_ref = ?",
            (account_ref, conversation_ref),
        ).fetchone()
        if row is None:
            return {"paused": False, "state_revision": 0, "updated_at": None}
        return {
            "paused": bool(row[0]),
            "state_revision": row[1],
            "updated_at": row[2],
        }

    def set_conversation_paused(
        self, account_ref: str, conversation_ref: str, paused: bool
    ) -> dict[str, Any]:
        if not isinstance(paused, bool):
            raise TypeError("paused must be a boolean")
        self._require_non_empty(account_ref, "account_ref")
        self._require_non_empty(conversation_ref, "conversation_ref")

        self.connection.execute("BEGIN IMMEDIATE")
        try:
            result = self._set_pause_in_transaction(
                account_ref, conversation_ref, paused
            )
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise
        return result

    def _set_pause_in_transaction(
        self, account_ref: str, conversation_ref: str, paused: bool
    ) -> dict[str, Any]:
        row = self.connection.execute(
            "SELECT paused, state_revision, updated_at "
            "FROM xianyu_conversation_takeovers "
            "WHERE account_ref = ? AND conversation_ref = ?",
            (account_ref, conversation_ref),
        ).fetchone()
        replayed = row is not None and bool(row[0]) is paused
        revision = 1 if row is None else row[1] + (0 if replayed else 1)
        updated_at = row[2] if replayed else _utc_now()
        if not replayed:
            self.connection.execute(
                "INSERT INTO xianyu_conversation_takeovers "
                "(account_ref, conversation_ref, paused, state_revision, updated_at) "
                "VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(account_ref, conversation_ref) DO UPDATE SET "
                "paused = excluded.paused, "
                "state_revision = excluded.state_revision, "
                "updated_at = excluded.updated_at",
                (account_ref, conversation_ref, int(paused), revision, updated_at),
            )
        revoked_leases = 0
        if paused:
            cursor = self.connection.execute(
                "UPDATE xianyu_inquiry_processing SET status = 'OBSERVED', "
                "processing_revision = processing_revision + 1, "
                "claimant_ref = NULL, lease_ref = NULL, lease_expires_at = NULL, "
                "updated_at = ? WHERE account_ref = ? AND conversation_ref = ? "
                "AND status = 'PROCESSING'",
                (_utc_now(), account_ref, conversation_ref),
            )
            revoked_leases = cursor.rowcount
        return {
            "paused": paused,
            "state_revision": revision,
            "updated_at": updated_at,
            "replayed": replayed,
            "revoked_leases": revoked_leases,
        }

    def apply_conversation_pause_command(
        self,
        *,
        account_ref: str,
        conversation_ref: str,
        message_ref: str,
        message_revision: str,
        desired_paused: bool,
    ) -> dict[str, Any]:
        if not isinstance(desired_paused, bool):
            raise TypeError("desired_paused must be a boolean")
        for name, value in {
            "account_ref": account_ref,
            "conversation_ref": conversation_ref,
            "message_ref": message_ref,
            "message_revision": message_revision,
        }.items():
            self._require_non_empty(value, name)
        command = {
            "account_ref": account_ref,
            "conversation_ref": conversation_ref,
            "message_ref": message_ref,
            "message_revision": message_revision,
            "desired_paused": desired_paused,
        }
        command_identity = _stable_ref(
            "takeover-command",
            account_ref,
            conversation_ref,
            message_ref,
            message_revision,
        )
        command_hash = _canonical_hash(command)

        self.connection.execute("BEGIN IMMEDIATE")
        try:
            existing = self.connection.execute(
                "SELECT command_hash, command_json FROM xianyu_takeover_commands "
                "WHERE command_identity = ?",
                (command_identity,),
            ).fetchone()
            if existing is not None:
                if existing[0] != command_hash:
                    raise TakeoverCommandIdentityConflict(command_identity)
                state = self.conversation_pause_state(account_ref, conversation_ref)
                self.connection.commit()
                return {
                    "command_identity": command_identity,
                    "command_hash": command_hash,
                    "desired_paused": desired_paused,
                    **state,
                    "command_replayed": True,
                    "revoked_leases": 0,
                }
            conflicting = self.connection.execute(
                "SELECT command_identity FROM xianyu_takeover_commands "
                "WHERE account_ref = ? AND message_ref = ? LIMIT 1",
                (account_ref, message_ref),
            ).fetchone()
            if conflicting is not None:
                raise TakeoverCommandBindingConflict(conflicting[0])
            self.connection.execute(
                "INSERT INTO xianyu_takeover_commands "
                "(command_identity, command_hash, account_ref, conversation_ref, "
                "message_ref, message_revision, desired_paused, command_json, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    command_identity,
                    command_hash,
                    account_ref,
                    conversation_ref,
                    message_ref,
                    message_revision,
                    int(desired_paused),
                    json.dumps(command, ensure_ascii=False, sort_keys=True),
                    _utc_now(),
                ),
            )
            state = self._set_pause_in_transaction(
                account_ref, conversation_ref, desired_paused
            )
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise
        return {
            "command_identity": command_identity,
            "command_hash": command_hash,
            "desired_paused": desired_paused,
            **state,
            "command_replayed": False,
        }

    def close(self) -> None:
        self.connection.close()


class NativeInquiryIdentityConflict(RuntimeError):
    def __init__(self, event_identity: str):
        super().__init__(
            f"native inquiry identity is already bound to different content: {event_identity}"
        )
        self.event_identity = event_identity


class NativeInquiryNotFound(RuntimeError):
    def __init__(self, event_identity: str):
        super().__init__(f"native inquiry was not observed: {event_identity}")
        self.event_identity = event_identity


class NativeInquiryBindingConflict(RuntimeError):
    def __init__(self, event_identity: str):
        super().__init__(
            f"native inquiry reference is bound to a different account, "
            f"conversation, or revision: {event_identity}"
        )
        self.event_identity = event_identity


class NativeInquiryLeaseConflict(RuntimeError):
    def __init__(self, event_identity: str):
        super().__init__(f"native inquiry lease is missing, expired, or revoked: {event_identity}")
        self.event_identity = event_identity


class NativeInquiryAckConflict(RuntimeError):
    def __init__(self, event_identity: str):
        super().__init__(f"native inquiry ACK conflicts with the durable result: {event_identity}")
        self.event_identity = event_identity


class TakeoverCommandIdentityConflict(RuntimeError):
    def __init__(self, command_identity: str):
        super().__init__(
            f"takeover command identity is already bound to different content: "
            f"{command_identity}"
        )
        self.command_identity = command_identity


class TakeoverCommandBindingConflict(RuntimeError):
    def __init__(self, command_identity: str):
        super().__init__(
            f"takeover command is bound to a different conversation: {command_identity}"
        )
        self.command_identity = command_identity


class SafeAdapter:
    def __init__(
        self,
        journal: OperationJournal,
        draft_generator: Optional[Callable[[str, str, Iterable[Dict[str, Any]]], str]] = None,
    ):
        self.journal = journal
        self.draft_generator = draft_generator

    def _result(
        self,
        request: Dict[str, Any],
        status: str,
        code: str,
        *,
        details: Optional[Dict[str, Any]] = None,
        retry_safe: bool = True,
    ) -> Dict[str, Any]:
        account_ref = str(request.get("account_ref", ""))
        operation_id = str(request.get("operation_id", ""))
        return {
            "schema_version": RESULT_SCHEMA,
            "operation_id": operation_id,
            "capability": request.get("capability"),
            "executor_id": EXECUTOR_ID,
            "status": status,
            "code": code,
            "account_object_ref": _stable_ref("account", account_ref),
            "operation_object_ref": _stable_ref("operation", operation_id),
            "retry_safe": retry_safe,
            "external_action_performed": False,
            "observed_at": _utc_now(),
            "details": details or {},
        }

    def execute(self, request: Dict[str, Any]) -> Dict[str, Any]:
        validation_error = self._validate(request)
        if validation_error:
            return self._result(request, "REJECTED", validation_error)

        capability = request["capability"]
        if capability == "operation.query":
            return self._query(request)

        request_hash = _canonical_hash(request)
        existing = self.journal.get(request["operation_id"])
        if existing:
            if existing["request_hash"] == request_hash:
                result = existing["result"]
                result["replayed"] = True
                return result
            return self._result(request, "REJECTED", "IDEMPOTENCY_CONFLICT")

        if capability == "account.status":
            result = self._account_status(request)
        elif capability == "inquiry.read":
            result = self._read_inquiry(request)
        elif capability == "reply.draft.generate":
            result = self._generate_draft(request)
        elif capability == "quote.constrain":
            result = self._constrain_quote(request)
        elif capability == "reply.send":
            draft_ref = request.get("payload", {}).get("draft_ref")
            if not isinstance(draft_ref, str) or not draft_ref.strip():
                result = self._result(request, "REJECTED", "MISSING_DRAFT_REF")
            else:
                result = self._result(
                    request,
                    "PAUSED",
                    "EXTERNAL_ACTION_APPROVAL_REQUIRED",
                    details={
                        "required_role": "named_approver",
                        "draft_object_ref": _stable_ref("draft", draft_ref),
                        "resume_via": "Huaxiaobao independent reply.send capability",
                    },
                    retry_safe=False,
                )
        else:
            result = self._result(request, "REJECTED", "CAPABILITY_NOT_ALLOWED")

        self.journal.put(request["operation_id"], request_hash, result)
        return result

    def _validate(self, request: Dict[str, Any]) -> Optional[str]:
        if not isinstance(request, dict):
            return "INVALID_REQUEST"
        if request.get("schema_version") != REQUEST_SCHEMA:
            return "UNSUPPORTED_SCHEMA_VERSION"
        if set(request) != {
            "schema_version", "operation_id", "capability", "account_ref", "payload"
        }:
            return "UNSUPPORTED_REQUEST_FIELDS"
        for field in ("operation_id", "capability", "account_ref"):
            if not isinstance(request.get(field), str) or not request[field].strip():
                return f"MISSING_{field.upper()}"
        if _contains_sensitive_key(request):
            return "CREDENTIAL_MATERIAL_FORBIDDEN"
        if not isinstance(request.get("payload", {}), dict):
            return "INVALID_PAYLOAD"
        return None

    def _read_inquiry(self, request: Dict[str, Any]) -> Dict[str, Any]:
        payload = request["payload"]
        required = {
            "conversation_ref",
            "message_ref",
            "message_revision",
            "sender_ref",
            "item_ref",
            "text",
            "observed_at",
        }
        if set(payload) != required:
            return self._result(request, "REJECTED", "INVALID_INQUIRY_FIELDS")
        if any(
            not isinstance(payload[field], str) or not payload[field].strip()
            for field in required
        ):
            return self._result(request, "REJECTED", "INVALID_INQUIRY_FIELDS")
        try:
            observed = datetime.fromisoformat(payload["observed_at"].replace("Z", "+00:00"))
        except ValueError:
            return self._result(request, "REJECTED", "INVALID_INQUIRY_TIMESTAMP")
        if observed.tzinfo is None:
            return self._result(request, "REJECTED", "INVALID_INQUIRY_TIMESTAMP")
        try:
            native = self.journal.get_native_inquiry(
                account_ref=request["account_ref"],
                conversation_ref=payload["conversation_ref"],
                message_ref=payload["message_ref"],
                message_revision=payload["message_revision"],
            )
        except NativeInquiryBindingConflict:
            return self._result(
                request,
                "REJECTED",
                "NATIVE_EVENT_BINDING_CONFLICT",
                details={"message_revision": payload["message_revision"]},
                retry_safe=False,
            )
        identity = _stable_ref(
            "message",
            request["account_ref"],
            payload["conversation_ref"],
            payload["message_ref"],
            payload["message_revision"],
        )
        if native is None:
            return self._result(
                request,
                "UNKNOWN",
                "INQUIRY_NOT_OBSERVED_BY_NATIVE_LISTENER",
                details={
                    "message_object_ref": identity,
                    "conversation_object_ref": _stable_ref(
                        "conversation", request["account_ref"], payload["conversation_ref"]
                    ),
                    "message_revision": payload["message_revision"],
                    "native_event_verified": False,
                },
            )

        expected_event = self.journal._inquiry_event(
            account_ref=request["account_ref"],
            conversation_ref=payload["conversation_ref"],
            message_ref=payload["message_ref"],
            message_revision=payload["message_revision"],
            sender_ref=payload["sender_ref"],
            item_ref=payload["item_ref"],
            text=payload["text"],
            observed_at=payload["observed_at"],
        )
        if native["event_hash"] != _canonical_hash(expected_event):
            return self._result(
                request,
                "REJECTED",
                "NATIVE_EVENT_IDENTITY_CONFLICT",
                details={
                    "message_object_ref": identity,
                    "message_revision": payload["message_revision"],
                },
                retry_safe=False,
            )
        return self._result(
            request,
            "SUCCEEDED",
            "NATIVE_INQUIRY_VERIFIED",
            details={
                "message_object_ref": identity,
                "conversation_object_ref": _stable_ref(
                    "conversation", request["account_ref"], payload["conversation_ref"]
                ),
                "sender_object_ref": _stable_ref(
                    "sender", request["account_ref"], payload["sender_ref"]
                ),
                "item_object_ref": _stable_ref(
                    "item", request["account_ref"], payload["item_ref"]
                ),
                "message_revision": payload["message_revision"],
                "text": payload["text"],
                "source_observed_at": payload["observed_at"],
                "native_event_ref": native["event_identity"],
                "native_event_hash": native["event_hash"],
                "native_event_verified": True,
            },
        )

    def _constrain_quote(self, request: Dict[str, Any]) -> Dict[str, Any]:
        payload = request["payload"]
        required = {
            "service_package_ref",
            "service_package_sha256",
            "supply_verification_ref",
            "supply_verification_sha256",
            "acceptance_ref",
            "currency",
            "proposed_quote_minor",
            "maximum_quote_minor",
            "minimum_margin_minor",
            "seven_costs_minor",
            "capacity_verified",
            "deadline_verified",
            "ai_use_allowed",
            "subcontracting_allowed",
        }
        if set(payload) != required:
            return self._result(request, "REJECTED", "INVALID_QUOTE_CONSTRAINT_FIELDS")
        for field in (
            "service_package_ref", "supply_verification_ref", "acceptance_ref", "currency"
        ):
            if not isinstance(payload[field], str) or not payload[field].strip():
                return self._result(request, "REJECTED", "INVALID_QUOTE_CONSTRAINT_FIELDS")
        for field in ("service_package_sha256", "supply_verification_sha256"):
            value = payload[field]
            if (
                not isinstance(value, str)
                or len(value.removeprefix("sha256:")) != 64
                or any(character not in "0123456789abcdef" for character in value.removeprefix("sha256:"))
            ):
                return self._result(request, "REJECTED", "INVALID_QUOTE_CONSTRAINT_HASH")
        costs = payload["seven_costs_minor"]
        if not isinstance(costs, dict) or set(costs) != SEVEN_COST_KEYS:
            return self._result(request, "REJECTED", "INVALID_SEVEN_COSTS")
        money_fields = {
            "proposed_quote_minor": payload["proposed_quote_minor"],
            "maximum_quote_minor": payload["maximum_quote_minor"],
            "minimum_margin_minor": payload["minimum_margin_minor"],
            **costs,
        }
        if any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in money_fields.values()):
            return self._result(request, "REJECTED", "INVALID_MONEY_BOUND")
        conditions = {
            field: payload[field]
            for field in (
                "capacity_verified", "deadline_verified", "ai_use_allowed",
                "subcontracting_allowed",
            )
        }
        if any(value is not True for value in conditions.values()):
            missing = sorted(field for field, value in conditions.items() if value is not True)
            return self._result(
                request,
                "BLOCKED",
                "SUPPLY_OR_TERMS_NOT_VERIFIED",
                details={"missing_or_denied": missing, "quote_allowed": False},
            )
        minimum_quote = sum(costs.values()) + payload["minimum_margin_minor"]
        proposed = payload["proposed_quote_minor"]
        maximum = payload["maximum_quote_minor"]
        allowed = minimum_quote <= proposed <= maximum
        return self._result(
            request,
            "SUCCEEDED" if allowed else "BLOCKED",
            "QUOTE_WITHIN_BOUNDS" if allowed else "QUOTE_OUTSIDE_BOUNDS",
            details={
                "quote_allowed": allowed,
                "currency": payload["currency"],
                "minimum_quote_minor": minimum_quote,
                "maximum_quote_minor": maximum,
                "proposed_quote_minor": proposed,
                "service_package_ref": payload["service_package_ref"],
                "service_package_sha256": payload["service_package_sha256"],
                "supply_verification_ref": payload["supply_verification_ref"],
                "supply_verification_sha256": payload["supply_verification_sha256"],
                "acceptance_ref": payload["acceptance_ref"],
                "commercial_approval_granted": False,
                "external_action_performed": False,
            },
        )

    def _account_status(self, request: Dict[str, Any]) -> Dict[str, Any]:
        configured = bool(os.getenv("COOKIES_STR", "").strip())
        if not configured:
            return self._result(
                request,
                "BLOCKED",
                "ACCOUNT_CREDENTIAL_MISSING",
                details={"required_role": "account_owner", "verified": False},
            )
        return self._result(
            request,
            "UNKNOWN",
            "ACCOUNT_CONFIGURED_NOT_VERIFIED",
            details={
                "required_role": "account_owner",
                "verified": False,
                "reason": "离线检查不建立闲鱼连接；须在 Huaxiaobao 隔离现场复查",
            },
        )

    def _generate_draft(self, request: Dict[str, Any]) -> Dict[str, Any]:
        payload = request.get("payload", {})
        message = payload.get("message")
        item_description = payload.get("item_description", "")
        context = payload.get("context", [])
        if not isinstance(message, str) or not message.strip():
            return self._result(request, "REJECTED", "MISSING_MESSAGE")
        if not isinstance(item_description, str) or not isinstance(context, list):
            return self._result(request, "REJECTED", "INVALID_DRAFT_INPUT")

        generator = self.draft_generator or self._native_generator
        try:
            draft = generator(message, item_description, context)
        except Exception as exc:
            return self._result(
                request,
                "UNKNOWN",
                "DRAFT_GENERATION_FAILED",
                details={"error_type": type(exc).__name__},
            )
        return self._result(
            request,
            "SUCCEEDED",
            "DRAFT_GENERATED",
            details={
                "draft": draft,
                "conversation_object_ref": _stable_ref(
                    "conversation", str(payload.get("conversation_ref", ""))
                ),
                "requires_external_action_approval": True,
            },
        )

    @staticmethod
    def _native_generator(
        message: str, item_description: str, context: Iterable[Dict[str, Any]]
    ) -> str:
        from XianyuAgent import XianyuReplyBot

        bot = XianyuReplyBot()
        return bot.generate_reply(message, item_description, list(context))

    def _query(self, request: Dict[str, Any]) -> Dict[str, Any]:
        target = request.get("payload", {}).get("target_operation_id")
        if not isinstance(target, str) or not target.strip():
            return self._result(request, "REJECTED", "MISSING_TARGET_OPERATION_ID")
        existing = self.journal.get(target)
        if existing is None:
            return self._result(
                request,
                "UNKNOWN",
                "OPERATION_NOT_FOUND",
                details={"target_operation_object_ref": _stable_ref("operation", target)},
            )
        result = existing["result"]
        return self._result(
            request,
            result["status"],
            "OPERATION_STATE_FOUND",
            details={
                "target_operation_object_ref": result["operation_object_ref"],
                "target_status": result["status"],
                "target_code": result["code"],
                "target_result": result,
            },
            retry_safe=result.get("retry_safe", True),
        )


def _load_request() -> Dict[str, Any]:
    value = json.load(sys.stdin)
    if not isinstance(value, dict):
        raise ValueError("request must be a JSON object")
    return value


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Safe Huaxiaobao adapter")
    parser.add_argument("command", choices=("describe", "execute"))
    args = parser.parse_args(list(argv) if argv is not None else None)
    if args.command == "describe":
        json.dump(descriptor(), sys.stdout, ensure_ascii=False, sort_keys=True)
        sys.stdout.write("\n")
        return 0

    state_path = Path(
        os.getenv("XIANYU_ADAPTER_STATE_PATH", "data/huaxiaobao_adapter.db")
    )
    journal = OperationJournal(state_path)
    try:
        result = SafeAdapter(journal).execute(_load_request())
    finally:
        journal.close()
    json.dump(result, sys.stdout, ensure_ascii=False, sort_keys=True)
    sys.stdout.write("\n")
    return 0 if result["status"] != "REJECTED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
