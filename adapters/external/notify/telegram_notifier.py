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

        It first tries Markdown formatting. If Telegram rejects the message
        because of parsing issues, it retries using plain text so the alert
        is still delivered.
        """
        async with self._bot:
            try:
                await self._bot.send_message(
                    chat_id=self._chat_id,
                    text=text,
                    parse_mode=ParseMode.MARKDOWN,
                    disable_web_page_preview=True,
                )
            except Exception:
                await self._bot.send_message(
                    chat_id=self._chat_id,
                    text=text,
                    disable_web_page_preview=True,
                )

    async def send_photo(self, photo_path: str, caption: str | None = None) -> None:
        """
        Send one Telegram photo.

        Caption is sent without markdown parsing to avoid Telegram entity
        parsing errors for values like OPEN_LONG, CLOSE_SHORT, stream keys,
        ids, and other runtime strings containing underscores.

        If Telegram rejects the caption for any reason, the photo is retried
        without caption so the chart is still delivered.
        """
        async with self._bot:
            with open(photo_path, "rb") as photo:
                try:
                    await self._bot.send_photo(
                        chat_id=self._chat_id,
                        photo=photo,
                        caption=caption,
                    )
                except Exception:
                    photo.seek(0)
                    await self._bot.send_photo(
                        chat_id=self._chat_id,
                        photo=photo,
                    )