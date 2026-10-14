"""Telegram alerts for the miner. Never send formulas — alpha IDs and statistics only.

    python notify.py chats          list chats that have messaged the bot (to find TELEGRAM_CHAT_ID)
    python notify.py send "text"    send a message
    python notify.py file path.md   send a file's contents

Needs TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID (GitHub secrets in the cloud, Windows user variables locally).
If they're missing, every call is a silent no-op so the miner works without Telegram.
"""
import os
import sys

import requests

API = "https://api.telegram.org/bot{token}/{method}"
LIMIT = 4000  # Telegram allows 4096 characters per message


def _get(name):
    value = os.environ.get(name)
    if not value and sys.platform == "win32":
        import winreg
        try:
            with winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Environment") as key:
                value = winreg.QueryValueEx(key, name)[0]
        except OSError:
            value = None
    return (value or "").strip().lstrip("﻿").strip() or None


def send(text):
    """Send a message; returns True on success. Failures never interrupt the miner."""
    token, chat = _get("TELEGRAM_BOT_TOKEN"), _get("TELEGRAM_CHAT_ID")
    if not token or not chat:
        return False
    ok = True
    for i in range(0, len(text), LIMIT):
        try:
            r = requests.post(API.format(token=token, method="sendMessage"), timeout=30,
                              json={"chat_id": chat, "text": text[i:i + LIMIT], "disable_web_page_preview": True})
            ok = ok and r.ok
        except requests.RequestException:
            ok = False
    return ok


def chats():
    token = _get("TELEGRAM_BOT_TOKEN")
    if not token:
        sys.exit("TELEGRAM_BOT_TOKEN is not set")
    updates = requests.get(API.format(token=token, method="getUpdates"), timeout=30).json().get("result", [])
    seen = {}
    for u in updates:
        chat = (u.get("message") or u.get("my_chat_member") or {}).get("chat")
        if chat:
            seen[chat["id"]] = chat.get("username") or chat.get("title") or chat.get("first_name")
    for cid, name in seen.items():
        print(cid, name)
    if not seen:
        print("no chats yet: open the bot in Telegram and send it any message")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "chats":
        chats()
    elif cmd == "send":
        print("sent" if send(" ".join(sys.argv[2:])) else "not sent")
    elif cmd == "file":
        text = open(sys.argv[2], encoding="utf-8").read()
        print("sent" if send(text) else "not sent")
    else:
        sys.exit(__doc__)
