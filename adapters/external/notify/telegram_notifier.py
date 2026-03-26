from __future__ import annotations

from telegram import Bot
from telegram.constants import ParseMode


class TelegramNotifier:
    """
    Telegram notifier used by api-signals.

    This notifier supports both text messages and photo uploads and applies
    Markdown parsing so execution reports keep the intended formatting.
    """

    def __init__(self, *, bot_token: str, chat_id: str) -> None:
        """
        Initialize the notifier.

        Args:
            bot_token: Telegram bot token.
            chat_id: Target chat identifier.
        """
        self._bot = Bot(token=str(bot_token))
        self._chat_id = str(chat_id)

    async def send_message(self, text: str) -> None:
        """
        Send one Telegram text message.

        Args:
            text: Message body.
        """
        async with self._bot:
            await self._bot.send_message(
                chat_id=self._chat_id,
                text=text,
                parse_mode=ParseMode.MARKDOWN,
                disable_web_page_preview=True,
            )

    async def send_photo(self, photo_path: str, caption: str | None = None) -> None:
        """
        Send one Telegram photo.

        Caption is sent without markdown parsing to avoid Telegram entity
        parsing errors for values like OPEN_LONG, CLOSE_SHORT, stream keys,
        ids, and other runtime strings containing underscores.
        """
        async with self._bot:
            with open(photo_path, "rb") as photo:
                await self._bot.send_photo(
                    chat_id=self._chat_id,
                    photo=photo,
                    caption=caption,
                )