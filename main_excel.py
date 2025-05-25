
import pandas as pd
from aiogram import Bot, Dispatcher, types
from aiogram.types import Message
from aiogram.utils import executor
from dotenv import load_dotenv
import os

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
EXCEL_PATH = "client_features.xlsx"

bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher(bot)

# Загружаем Excel с признаками и сегментами
df_clients = pd.read_excel(EXCEL_PATH)

@dp.message_handler(commands=["start"])
async def start_handler(message: Message):
    await message.reply("👋 Привет! Напиши /segment <card_id> чтобы узнать сегмент клиента.")

@dp.message_handler(commands=["segment"])
async def segment_handler(message: Message):
    try:
        _, cid = message.text.strip().split()
        cid = int(cid)
        row = df_clients[df_clients["card_id"] == cid]
        if row.empty:
            await message.reply("❌ Клиент не найден.")
            return
        r = row.iloc[0]
        await message.reply(
            f"📇 Клиент: {cid}\n"
            f"- Транзакций: {r['total_txns']}\n"
            f"- Средний чек: {int(r['avg_txn_amt'])}₸\n"
            f"- Еда: {r['pct_food']*100:.1f}%\n"
            f"- Travel: {r['pct_travel']*100:.1f}%\n"
            f"- Городов: {int(r['unique_cities'])}\n"
            f"🧩 Сегмент: {r['segment_id']}"
        )
    except Exception as e:
        await message.reply("⚠️ Неверный формат. Используй /segment <card_id>")

if __name__ == "__main__":
    from aiogram import executor
    from aiogram.contrib.middlewares.logging import LoggingMiddleware
    dp.middleware.setup(LoggingMiddleware())
    executor.start_polling(dp)
