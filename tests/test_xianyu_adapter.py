import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from xianyu_adapter import (
    NativeInquiryAckConflict,
    NativeInquiryBindingConflict,
    NativeInquiryIdentityConflict,
    NativeInquiryLeaseConflict,
    OperationJournal,
    SafeAdapter,
    TakeoverCommandBindingConflict,
    TakeoverCommandIdentityConflict,
    descriptor,
)


def request(operation_id, capability, payload=None):
    return {
        "schema_version": "foundry.huaxiaobao.tool-request.v1",
        "operation_id": operation_id,
        "capability": capability,
        "account_ref": "owner-account-1",
        "payload": payload or {},
    }


def inquiry_payload(text="Can this be delivered tomorrow?", revision="revision-4"):
    return {
        "conversation_ref": "conversation-native-7",
        "message_ref": "message-native-9",
        "message_revision": revision,
        "sender_ref": "sender-native-3",
        "item_ref": "item-native-5",
        "text": text,
        "observed_at": "2026-09-12T12:00:00Z",
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

        self.assertEqual(first["status"], "SUCCEEDED")
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

    def test_inquiry_requires_exact_native_event_and_is_restart_queryable(self):
        adapter = SafeAdapter(self.journal)
        payload = inquiry_payload()
        missing = adapter.execute(request("inquiry-missing", "inquiry.read", payload))
        self.assertEqual(missing["status"], "UNKNOWN")
        self.assertFalse(missing["details"]["native_event_verified"])

        native = self.journal.record_native_inquiry(
            account_ref="owner-account-1",
            **payload,
        )
        value = request("inquiry-verified", "inquiry.read", payload)
        first = adapter.execute(value)
        duplicate = adapter.execute(value)
        self.assertEqual(first["status"], "SUCCEEDED")
        self.assertTrue(duplicate["replayed"])
        self.assertTrue(first["details"]["native_event_verified"])
        self.assertEqual(first["details"]["native_event_ref"], native["event_identity"])
        self.assertEqual(first["details"]["message_revision"], "revision-4")
        self.journal.close()
        reopened = OperationJournal(self.database)
        try:
            queried = SafeAdapter(reopened).execute(
                request(
                    "query-inquiry-1",
                    "operation.query",
                    {"target_operation_id": "inquiry-verified"},
                )
            )
        finally:
            reopened.close()
        self.journal = OperationJournal(self.database)
        self.assertEqual(queried["details"]["target_status"], "SUCCEEDED")

    def test_native_inquiry_duplicate_is_idempotent_and_content_conflict_fails_closed(self):
        payload = inquiry_payload()
        first = self.journal.record_native_inquiry(
            account_ref="owner-account-1", **payload
        )
        duplicate = self.journal.record_native_inquiry(
            account_ref="owner-account-1", **payload
        )

        self.assertFalse(first["replayed"])
        self.assertTrue(duplicate["replayed"])
        self.assertEqual(first["event_hash"], duplicate["event_hash"])
        with self.assertRaises(NativeInquiryIdentityConflict):
            self.journal.record_native_inquiry(
                account_ref="owner-account-1",
                **inquiry_payload(text="tampered content"),
            )

        conflict = SafeAdapter(self.journal).execute(
            request(
                "inquiry-conflict",
                "inquiry.read",
                inquiry_payload(text="tampered content"),
            )
        )
        self.assertEqual(conflict["status"], "REJECTED")
        self.assertEqual(conflict["code"], "NATIVE_EVENT_IDENTITY_CONFLICT")
        self.assertFalse(conflict["retry_safe"])

        with self.assertRaises(NativeInquiryBindingConflict):
            self.journal.record_native_inquiry(
                account_ref="owner-account-1",
                **inquiry_payload(revision="stale-or-edited-revision"),
            )
        stale_read = SafeAdapter(self.journal).execute(
            request(
                "inquiry-stale-revision",
                "inquiry.read",
                inquiry_payload(revision="stale-or-edited-revision"),
            )
        )
        self.assertEqual(stale_read["status"], "REJECTED")
        self.assertEqual(stale_read["code"], "NATIVE_EVENT_BINDING_CONFLICT")
        self.assertFalse(stale_read["retry_safe"])

    def test_manual_pause_persists_and_only_changes_on_explicit_transition(self):
        account_ref = "owner-account-1"
        conversation_ref = "conversation-native-7"
        entered = self.journal.set_conversation_paused(
            account_ref, conversation_ref, True
        )
        replayed = self.journal.set_conversation_paused(
            account_ref, conversation_ref, True
        )
        self.assertEqual(entered["state_revision"], 1)
        self.assertTrue(replayed["replayed"])
        self.assertEqual(replayed["state_revision"], 1)

        self.journal.close()
        reopened = OperationJournal(self.database)
        try:
            persisted = reopened.conversation_pause_state(
                account_ref, conversation_ref
            )
            resumed = reopened.set_conversation_paused(account_ref, conversation_ref, False)
        finally:
            reopened.close()
        self.journal = OperationJournal(self.database)

        self.assertTrue(persisted["paused"])
        self.assertEqual(persisted["state_revision"], 1)
        self.assertFalse(resumed["paused"])
        self.assertEqual(resumed["state_revision"], 2)

    def test_processing_lease_recovers_after_crash_and_ack_replay_is_idempotent(self):
        payload = inquiry_payload()
        self.journal.record_native_inquiry(account_ref="owner-account-1", **payload)
        first = self.journal.claim_native_inquiry(
            account_ref="owner-account-1",
            conversation_ref=payload["conversation_ref"],
            message_ref=payload["message_ref"],
            message_revision=payload["message_revision"],
            claimant_ref="listener-instance-1",
            lease_seconds=10,
            now="2026-09-12T12:00:00Z",
        )
        replay = self.journal.claim_native_inquiry(
            account_ref="owner-account-1",
            conversation_ref=payload["conversation_ref"],
            message_ref=payload["message_ref"],
            message_revision=payload["message_revision"],
            claimant_ref="listener-instance-1",
            lease_seconds=10,
            now="2026-09-12T12:00:01Z",
        )
        self.assertTrue(first["claimed"])
        self.assertTrue(replay["replayed"])
        self.assertEqual(first["lease_ref"], replay["lease_ref"])

        self.journal.close()
        reopened = OperationJournal(self.database)
        try:
            busy = reopened.claim_native_inquiry(
                account_ref="owner-account-1",
                conversation_ref=payload["conversation_ref"],
                message_ref=payload["message_ref"],
                message_revision=payload["message_revision"],
                claimant_ref="listener-instance-2",
                lease_seconds=10,
                now="2026-09-12T12:00:05Z",
            )
            recovered = reopened.claim_native_inquiry(
                account_ref="owner-account-1",
                conversation_ref=payload["conversation_ref"],
                message_ref=payload["message_ref"],
                message_revision=payload["message_revision"],
                claimant_ref="listener-instance-2",
                lease_seconds=10,
                now="2026-09-12T12:00:11Z",
            )
            self.assertTrue(busy["busy"])
            self.assertFalse(busy["claimed"])
            self.assertTrue(recovered["claimed"])
            self.assertEqual(recovered["processing_revision"], 2)
            self.assertNotEqual(first["lease_ref"], recovered["lease_ref"])

            with self.assertRaises(NativeInquiryLeaseConflict):
                reopened.ack_native_inquiry(
                    account_ref="owner-account-1",
                    conversation_ref=payload["conversation_ref"],
                    message_ref=payload["message_ref"],
                    message_revision=payload["message_revision"],
                    lease_ref=first["lease_ref"],
                    checkpoint_ref="checkpoint-1",
                    checkpoint_sha256="a" * 64,
                    outcome="DRAFT_READY",
                    now="2026-09-12T12:00:12Z",
                )
            ack = reopened.ack_native_inquiry(
                account_ref="owner-account-1",
                conversation_ref=payload["conversation_ref"],
                message_ref=payload["message_ref"],
                message_revision=payload["message_revision"],
                lease_ref=recovered["lease_ref"],
                checkpoint_ref="checkpoint-1",
                checkpoint_sha256="a" * 64,
                outcome="DRAFT_READY",
                now="2026-09-12T12:00:12Z",
            )
            replayed_ack = reopened.ack_native_inquiry(
                account_ref="owner-account-1",
                conversation_ref=payload["conversation_ref"],
                message_ref=payload["message_ref"],
                message_revision=payload["message_revision"],
                lease_ref=recovered["lease_ref"],
                checkpoint_ref="checkpoint-1",
                checkpoint_sha256="a" * 64,
                outcome="DRAFT_READY",
                now="2026-09-12T12:00:30Z",
            )
            self.assertFalse(ack["replayed"])
            self.assertTrue(replayed_ack["replayed"])
            with self.assertRaises(NativeInquiryAckConflict):
                reopened.ack_native_inquiry(
                    account_ref="owner-account-1",
                    conversation_ref=payload["conversation_ref"],
                    message_ref=payload["message_ref"],
                    message_revision=payload["message_revision"],
                    lease_ref=recovered["lease_ref"],
                    checkpoint_ref="checkpoint-changed",
                    checkpoint_sha256="b" * 64,
                    outcome="DRAFT_READY",
                    now="2026-09-12T12:00:30Z",
                )
        finally:
            reopened.close()
        self.journal = OperationJournal(self.database)

    def test_observation_commit_before_processing_is_restart_claimable(self):
        payload = inquiry_payload()
        self.journal.record_native_inquiry(account_ref="owner-account-1", **payload)
        observed = self.journal.inquiry_processing_state(
            account_ref="owner-account-1",
            conversation_ref=payload["conversation_ref"],
            message_ref=payload["message_ref"],
            message_revision=payload["message_revision"],
        )
        self.assertEqual(observed["status"], "OBSERVED")
        self.journal.close()

        reopened = OperationJournal(self.database)
        try:
            claimed = reopened.claim_native_inquiry(
                account_ref="owner-account-1",
                conversation_ref=payload["conversation_ref"],
                message_ref=payload["message_ref"],
                message_revision=payload["message_revision"],
                claimant_ref="listener-after-restart",
                now="2026-09-12T12:00:01Z",
            )
        finally:
            reopened.close()
        self.journal = OperationJournal(self.database)
        self.assertTrue(claimed["claimed"])
        self.assertEqual(claimed["processing_revision"], 1)

    def test_pause_revokes_lease_and_desired_state_commands_resume_safely(self):
        payload = inquiry_payload()
        self.journal.record_native_inquiry(account_ref="owner-account-1", **payload)
        claimed = self.journal.claim_native_inquiry(
            account_ref="owner-account-1",
            conversation_ref=payload["conversation_ref"],
            message_ref=payload["message_ref"],
            message_revision=payload["message_revision"],
            claimant_ref="listener-instance-1",
            now="2026-09-12T12:00:00Z",
        )
        with self.assertRaises(NativeInquiryBindingConflict):
            self.journal.claim_native_inquiry(
                account_ref="wrong-account",
                conversation_ref=payload["conversation_ref"],
                message_ref=payload["message_ref"],
                message_revision=payload["message_revision"],
                claimant_ref="listener-instance-wrong",
                now="2026-09-12T12:00:00Z",
            )
        with self.assertRaises(NativeInquiryBindingConflict):
            self.journal.ack_native_inquiry(
                account_ref="owner-account-1",
                conversation_ref=payload["conversation_ref"],
                message_ref=payload["message_ref"],
                message_revision="old-revision",
                lease_ref=claimed["lease_ref"],
                checkpoint_ref="checkpoint-old-revision",
                checkpoint_sha256="a" * 64,
                outcome="DRAFT_READY",
                now="2026-09-12T12:00:01Z",
            )
        pause = self.journal.apply_conversation_pause_command(
            account_ref="owner-account-1",
            conversation_ref=payload["conversation_ref"],
            message_ref="control-message-1",
            message_revision="control-revision-1",
            desired_paused=True,
        )
        duplicate_pause = self.journal.apply_conversation_pause_command(
            account_ref="owner-account-1",
            conversation_ref=payload["conversation_ref"],
            message_ref="control-message-1",
            message_revision="control-revision-1",
            desired_paused=True,
        )
        self.assertEqual(pause["revoked_leases"], 1)
        self.assertTrue(duplicate_pause["command_replayed"])
        self.assertEqual(pause["state_revision"], duplicate_pause["state_revision"])
        revoked_state = self.journal.inquiry_processing_state(
            account_ref="owner-account-1",
            conversation_ref=payload["conversation_ref"],
            message_ref=payload["message_ref"],
            message_revision=payload["message_revision"],
        )
        self.assertEqual(revoked_state["status"], "OBSERVED")
        self.assertIsNone(revoked_state["lease_ref"])
        with self.assertRaises(NativeInquiryLeaseConflict):
            self.journal.ack_native_inquiry(
                account_ref="owner-account-1",
                conversation_ref=payload["conversation_ref"],
                message_ref=payload["message_ref"],
                message_revision=payload["message_revision"],
                lease_ref=claimed["lease_ref"],
                checkpoint_ref="checkpoint-revoked",
                checkpoint_sha256="a" * 64,
                outcome="DRAFT_READY",
                now="2026-09-12T12:00:01Z",
            )
        paused_claim = self.journal.claim_native_inquiry(
            account_ref="owner-account-1",
            conversation_ref=payload["conversation_ref"],
            message_ref=payload["message_ref"],
            message_revision=payload["message_revision"],
            claimant_ref="listener-instance-2",
            now="2026-09-12T12:00:02Z",
        )
        self.assertEqual(paused_claim["status"], "PAUSED")
        self.assertFalse(paused_claim["claimed"])

        with self.assertRaises(TakeoverCommandIdentityConflict):
            self.journal.apply_conversation_pause_command(
                account_ref="owner-account-1",
                conversation_ref=payload["conversation_ref"],
                message_ref="control-message-1",
                message_revision="control-revision-1",
                desired_paused=False,
            )
        with self.assertRaises(TakeoverCommandBindingConflict):
            self.journal.apply_conversation_pause_command(
                account_ref="owner-account-1",
                conversation_ref="wrong-conversation",
                message_ref="control-message-1",
                message_revision="stale-revision",
                desired_paused=True,
            )
        resume = self.journal.apply_conversation_pause_command(
            account_ref="owner-account-1",
            conversation_ref=payload["conversation_ref"],
            message_ref="control-message-2",
            message_revision="control-revision-2",
            desired_paused=False,
        )
        resumed_claim = self.journal.claim_native_inquiry(
            account_ref="owner-account-1",
            conversation_ref=payload["conversation_ref"],
            message_ref=payload["message_ref"],
            message_revision=payload["message_revision"],
            claimant_ref="listener-instance-2",
            now="2026-09-12T12:00:03Z",
        )
        self.assertFalse(resume["paused"])
        self.assertTrue(resumed_claim["claimed"])
        with self.assertRaises(NativeInquiryBindingConflict):
            self.journal.claim_native_inquiry(
                account_ref="owner-account-1",
                conversation_ref="wrong-conversation",
                message_ref=payload["message_ref"],
                message_revision=payload["message_revision"],
                claimant_ref="listener-instance-3",
                now="2026-09-12T12:00:04Z",
            )

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
