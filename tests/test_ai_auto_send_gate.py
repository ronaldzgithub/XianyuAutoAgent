import asyncio
import os
import unittest
from unittest.mock import AsyncMock, patch

from main import XianyuLive, env_flag_enabled


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


if __name__ == "__main__":
    unittest.main()
