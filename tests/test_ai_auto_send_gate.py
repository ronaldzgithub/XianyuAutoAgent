import asyncio
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from main import XianyuLive, env_flag_enabled
from xianyu_adapter import NativeInquiryIdentityConflict, OperationJournal


class AiAutoSendGateTests(unittest.TestCase):
    def test_gate_is_fail_closed_by_default(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(env_flag_enabled("ENABLE_AI_AUTO_SEND"))

    def test_only_explicit_affirmative_value_enables_gate(self):
        for value in ("true", "TRUE", "1", "yes", "on"):
            with self.subTest(value=value):
                with patch.dict(os.environ, {"ENABLE_AI_AUTO_SEND": value}, clear=True):
                    self.assertTrue(env_flag_enabled("ENABLE_AI_AUTO_SEND"))

        for value in ("", "false", "0", "enabled", "maybe"):
            with self.subTest(value=value):
                with patch.dict(os.environ, {"ENABLE_AI_AUTO_SEND": value}, clear=True):
                    self.assertFalse(env_flag_enabled("ENABLE_AI_AUTO_SEND"))

    def test_send_is_blocked_without_explicit_enablement(self):
        live = object.__new__(XianyuLive)
        live.ai_auto_send_enabled = False
        websocket = AsyncMock()

        sent = asyncio.run(live.send_msg(websocket, "chat", "buyer", "draft"))

        self.assertFalse(sent)
        websocket.send.assert_not_awaited()

    def test_send_is_allowed_after_explicit_enablement(self):
        live = object.__new__(XianyuLive)
        live.ai_auto_send_enabled = True
        live.myid = "seller"
        websocket = AsyncMock()

        sent = asyncio.run(live.send_msg(websocket, "chat", "buyer", "approved text"))

        self.assertTrue(sent)
        websocket.send.assert_awaited_once()

    def test_live_manual_takeover_uses_persistent_adapter_journal(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            database = Path(temp_dir) / "operations.db"
            first = object.__new__(XianyuLive)
            first.account_ref = "account-opaque-1"
            first.operation_journal = OperationJournal(database)
            entered = first.enter_manual_mode("native-chat-7")
            self.assertTrue(entered["paused"])
            self.assertTrue(first.is_manual_mode("native-chat-7"))
            first.close()

            restarted = object.__new__(XianyuLive)
            restarted.account_ref = "account-opaque-1"
            restarted.operation_journal = OperationJournal(database)
            try:
                self.assertTrue(restarted.is_manual_mode("native-chat-7"))
                self.assertEqual(restarted.toggle_manual_mode("native-chat-7"), "auto")
                self.assertFalse(restarted.is_manual_mode("native-chat-7"))
            finally:
                restarted.close()

    def test_live_native_inquiry_is_written_before_adapter_consumption(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            live = object.__new__(XianyuLive)
            live.account_ref = "account-opaque-1"
            live.operation_journal = OperationJournal(
                Path(temp_dir) / "operations.db"
            )
            try:
                first = live.record_native_inquiry(
                    chat_id="native-chat-7",
                    sender_id="native-buyer-3",
                    item_id="native-item-5",
                    create_time_ms=1_757_678_400_000,
                    text="Is this available?",
                    native_message_id="native-message-9",
                    message_revision="4",
                )
                duplicate = live.record_native_inquiry(
                    chat_id="native-chat-7",
                    sender_id="native-buyer-3",
                    item_id="native-item-5",
                    create_time_ms=1_757_678_400_000,
                    text="Is this available?",
                    native_message_id="native-message-9",
                    message_revision="4",
                )
                with self.assertRaises(NativeInquiryIdentityConflict):
                    live.record_native_inquiry(
                        chat_id="native-chat-7",
                        sender_id="different-native-buyer",
                        item_id="native-item-5",
                        create_time_ms=1_757_678_400_000,
                        text="Is this available?",
                        native_message_id="native-message-9",
                        message_revision="4",
                    )
            finally:
                live.close()

        self.assertFalse(first["replayed"])
        self.assertTrue(duplicate["replayed"])
        self.assertTrue(first["event"]["account_ref"].startswith("account-opaque"))
        self.assertNotIn("native-chat-7", str(first))
        self.assertNotIn("native-buyer-3", str(first))

    def test_listener_skips_duplicate_native_inquiry_before_repeat_processing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            live = object.__new__(XianyuLive)
            live.account_ref = "account-opaque-1"
            live.operation_journal = OperationJournal(
                Path(temp_dir) / "operations.db"
            )
            live.myid = "native-seller"
            live.message_expire_time = 300_000
            live.toggle_keywords = "。"
            live.context_manager = MagicMock()
            live.enter_manual_mode("native-chat-7")
            websocket = AsyncMock()
            created_at = int(__import__("time").time() * 1000)
            decoded = {
                "1": {
                    "2": "native-chat-7@goofish",
                    "5": str(created_at),
                    "10": {
                        "messageId": "native-message-9",
                        "messageVersion": "4",
                        "reminderTitle": "buyer",
                        "senderUserId": "native-buyer-3",
                        "reminderContent": "Is this available?",
                        "reminderUrl": "https://www.goofish.com/im?itemId=native-item-5&x=1",
                    },
                }
            }
            envelope = {
                "headers": {"mid": "transport-1", "sid": "session-1"},
                "body": {"syncPushPackage": {"data": [{"data": "encrypted"}]}},
            }
            try:
                with patch("main.base64.b64decode", side_effect=ValueError), patch(
                    "main.decrypt", return_value=__import__("json").dumps(decoded)
                ):
                    asyncio.run(live.handle_message(envelope, websocket))
                    asyncio.run(live.handle_message(envelope, websocket))
            finally:
                live.close()

        live.context_manager.add_message_by_chat.assert_called_once()


if __name__ == "__main__":
    unittest.main()
