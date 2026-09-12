import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from xianyu_adapter import OperationJournal, SafeAdapter, descriptor


def request(operation_id, capability, payload=None):
    return {
        "schema_version": "foundry.huaxiaobao.tool-request.v1",
        "operation_id": operation_id,
        "capability": capability,
        "account_ref": "owner-account-1",
        "payload": payload or {},
    }


class SafeAdapterTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "operations.db"
        self.journal = OperationJournal(self.database)

    def tearDown(self):
        self.journal.close()
        self.temp_dir.cleanup()

    def test_descriptor_never_exposes_send_as_available(self):
        send = next(
            item for item in descriptor()["capabilities"] if item["id"] == "reply.send"
        )
        self.assertFalse(send["available"])
        self.assertTrue(send["external_action"])

    def test_draft_generation_is_idempotent_and_never_sends(self):
        calls = []

        def generate(message, item_description, context):
            calls.append((message, item_description, context))
            return "draft only"

        adapter = SafeAdapter(self.journal, generate)
        value = request(
            "draft-1",
            "reply.draft.generate",
            {
                "message": "is it available?",
                "item_description": "sample",
                "conversation_ref": "conversation-7",
                "context": [],
            },
        )
        first = adapter.execute(value)
        second = adapter.execute(value)

        self.assertEqual(first["status"], "UNKNOWN")
        self.assertEqual(first["details"]["draft"], "draft only")
        self.assertFalse(first["external_action_performed"])
        self.assertTrue(second["replayed"])
        self.assertEqual(len(calls), 1)

    def test_changed_request_with_same_operation_id_fails_closed(self):
        adapter = SafeAdapter(self.journal, lambda *_: "draft")
        first = request("same-id", "reply.draft.generate", {"message": "one"})
        changed = request("same-id", "reply.draft.generate", {"message": "two"})

        self.assertEqual(adapter.execute(first)["status"], "SUCCEEDED")
        conflict = adapter.execute(changed)
        self.assertEqual(conflict["status"], "REJECTED")
        self.assertEqual(conflict["code"], "IDEMPOTENCY_CONFLICT")

    def test_send_is_durably_paused_and_queryable_after_reopen(self):
        send = SafeAdapter(self.journal).execute(
            request("send-1", "reply.send", {"draft_ref": "draft-1"})
        )
        self.assertEqual(send["status"], "PAUSED")
        self.assertFalse(send["external_action_performed"])
        self.journal.close()

        reopened = OperationJournal(self.database)
        try:
            queried = SafeAdapter(reopened).execute(
                request(
                    "query-1",
                    "operation.query",
                    {"target_operation_id": "send-1"},
                )
            )
        finally:
            reopened.close()
        self.journal = OperationJournal(self.database)

        self.assertEqual(queried["status"], "PAUSED")
        self.assertEqual(queried["details"]["target_status"], "PAUSED")

    def test_account_status_never_returns_ready_without_live_verification(self):
        with patch.dict(os.environ, {}, clear=True):
            missing = SafeAdapter(self.journal).execute(
                request("account-1", "account.status")
            )
        self.assertEqual(missing["status"], "BLOCKED")

        with patch.dict(os.environ, {"COOKIES_STR": "not-exposed"}, clear=True):
            configured = SafeAdapter(self.journal).execute(
                request("account-2", "account.status")
            )
        self.assertEqual(configured["status"], "UNKNOWN")
        self.assertNotIn("not-exposed", str(configured))

    def test_credential_material_in_request_is_rejected(self):
        value = request("unsafe-1", "reply.draft.generate", {"message": "hello"})
        value["payload"]["cookie"] = "must-not-accept"
        result = SafeAdapter(self.journal).execute(value)
        self.assertEqual(result["code"], "CREDENTIAL_MATERIAL_FORBIDDEN")

    def test_inquiry_envelope_is_stable_deduplicated_and_restart_queryable(self):
        adapter = SafeAdapter(self.journal)
        value = request(
            "inquiry-1",
            "inquiry.read",
            {
                "conversation_ref": "conversation-native-7",
                "message_ref": "message-native-9",
                "sender_ref": "sender-native-3",
                "text": "Can this be delivered tomorrow?",
                "observed_at": "2026-09-12T12:00:00Z",
            },
        )
        first = adapter.execute(value)
        duplicate = adapter.execute(value)
        self.assertEqual(first["status"], "SUCCEEDED")
        self.assertTrue(duplicate["replayed"])
        self.assertFalse(first["details"]["native_event_verified"])
        self.journal.close()
        reopened = OperationJournal(self.database)
        try:
            queried = SafeAdapter(reopened).execute(
                request("query-inquiry-1", "operation.query", {"target_operation_id": "inquiry-1"})
            )
        finally:
            reopened.close()
        self.journal = OperationJournal(self.database)
        self.assertEqual(queried["details"]["target_status"], "UNKNOWN")

    def test_quote_constraint_requires_seven_costs_supply_and_terms(self):
        payload = {
            "service_package_ref": "service-package-1",
            "service_package_sha256": "a" * 64,
            "supply_verification_ref": "supply-check-1",
            "supply_verification_sha256": "b" * 64,
            "acceptance_ref": "acceptance-contract-1",
            "currency": "CNY",
            "proposed_quote_minor": 15000,
            "maximum_quote_minor": 20000,
            "minimum_margin_minor": 3000,
            "seven_costs_minor": {
                "acquisition": 1000,
                "model": 1000,
                "data": 1000,
                "human": 4000,
                "delivery": 1000,
                "support": 1000,
                "risk": 1000,
            },
            "capacity_verified": True,
            "deadline_verified": True,
            "ai_use_allowed": True,
            "subcontracting_allowed": True,
        }
        allowed = SafeAdapter(self.journal).execute(request("quote-1", "quote.constrain", payload))
        self.assertEqual(allowed["status"], "SUCCEEDED")
        self.assertTrue(allowed["details"]["quote_allowed"])
        self.assertFalse(allowed["details"]["commercial_approval_granted"])

        blocked_payload = dict(payload)
        blocked_payload["ai_use_allowed"] = None
        blocked = SafeAdapter(self.journal).execute(
            request("quote-2", "quote.constrain", blocked_payload)
        )
        self.assertEqual(blocked["status"], "BLOCKED")
        self.assertIn("ai_use_allowed", blocked["details"]["missing_or_denied"])


if __name__ == "__main__":
    unittest.main()
