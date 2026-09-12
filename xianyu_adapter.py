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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Optional


REQUEST_SCHEMA = "foundry.huaxiaobao.tool-request.v1"
RESULT_SCHEMA = "foundry.huaxiaobao.tool-result.v1"
EXECUTOR_ID = "XianyuAutoAgent"
SENSITIVE_KEYS = ("cookie", "token", "password", "secret", "authorization")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical_hash(value: Dict[str, Any]) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _stable_ref(kind: str, *parts: str) -> str:
    digest = hashlib.sha256("\x00".join(parts).encode("utf-8")).hexdigest()[:24]
    return f"xianyu:{kind}:{digest}"


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
                "id": "reply.draft.generate",
                "side_effect": "external_model_data_egress",
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
        self.connection = sqlite3.connect(str(path))
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

    def close(self) -> None:
        self.connection.close()


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
        elif capability == "reply.draft.generate":
            result = self._generate_draft(request)
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
        for field in ("operation_id", "capability", "account_ref"):
            if not isinstance(request.get(field), str) or not request[field].strip():
                return f"MISSING_{field.upper()}"
        if _contains_sensitive_key(request):
            return "CREDENTIAL_MATERIAL_FORBIDDEN"
        if not isinstance(request.get("payload", {}), dict):
            return "INVALID_PAYLOAD"
        return None

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
