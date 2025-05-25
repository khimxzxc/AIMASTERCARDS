import os
import logging
import pandas as pd
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from dotenv import load_dotenv
from groq_client import get_segment_by_behavior
from insight_chart import plot_behavior

# Загрузка .env
load_dotenv()
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DATA_PATH = os.getenv("DATA_PATH", "clients.parquet")

# Логирование
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Глобальные данные
df_clients = pd.read_parquet(DATA_PATH)

SEGMENT_MAP = {
    0: ("Городской исследователь", "Активно изучает городскую среду, тратит на еду и развлечения."),
    1: ("Инвестирующий семьянин", "Стабильный доход, семья, меньше трат на путешествия."),
    2: ("Цифровой путешественник", "Активно перемещается, платит цифровыми кошельками.")
}

# /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Привет! Я бот по клиентской сегментации.\n\n"
        "📌 Доступные команды:\n"
        "/segment <card_id> — покажу сегмент и поведение клиента\n"
        "/insight <card_id> — сгенерирую инсайт через AI и график\n"
        "/clients — список всех клиентов (в виде файла)\n"
        "/random — покажу случайного клиента\n"
        "/segments — статистика по сегментам"
    )

# /segment
async def segment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) != 1:
        await update.message.reply_text("Используй: /segment <card_id>")
        return
    try:
        card_id = int(context.args[0])
        row = df_clients[df_clients["card_id"] == card_id]
        if row.empty:
            await update.message.reply_text("⚠️ Клиент не найден.")
            return

        feats = row.iloc[0].to_dict()
        seg_id = feats["segment_id"]
        seg_name, seg_desc = SEGMENT_MAP.get(seg_id, ("Неизвестно", "Описание отсутствует."))

        msg = f"📇 Клиент: {card_id}\n\n"
        msg += f"📊 Поведение:\n"
        msg += f"- Транзакций: {feats['total_txns']}\n"
        msg += f"- Средний чек: {feats['avg_txn_amt']:.0f}₸\n"
        msg += f"- Еда: {feats['pct_food']*100:.1f}%\n"
        msg += f"- Travel: {feats['pct_travel']*100:.1f}%\n"
        msg += f"- Кошелёк: {'Да' if feats['pct_wallet_use'] > 0.5 else 'Нет'}\n"
        msg += f"- Зарплата: {'Да' if feats['salary_flag'] else 'Нет'}\n"
        msg += f"- Уник. города: {feats['unique_cities']}\n\n"
        msg += f"🧩 Сегмент: {seg_name}\n📌 {seg_desc}"

        await update.message.reply_text(msg)
    except Exception as e:
        logger.error(e)
        await update.message.reply_text("❌ Ошибка при выводе информации о клиенте.")

# /insight
async def insight(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) != 1:
        await update.message.reply_text("Используй: /insight <card_id>")
        return
    try:
        card_id = int(context.args[0])
        row = df_clients[df_clients["card_id"] == card_id]
        if row.empty:
            await update.message.reply_text("⚠️ Клиент не найден.")
            return

        feats = row.iloc[0].drop("card_id").to_dict()
        await update.message.reply_text("🤖 Отправляю запрос в LLaMA3...")
        result = get_segment_by_behavior(feats)

        msg = f"📌 Сегмент: {result['segment_name']}\n"
        msg += f"🧠 Обоснование: {result['explanation']}\n"

        if result['metrics_markdown']:
            msg += f"\n📊 Метрики:\n"
            msg += f"```\n{result['metrics_markdown']}\n```"

        if result['recommendation']:
            msg += f"\n✅ Рекомендации:\n"
            for r in result['recommendation']:
                msg += f"• {r}\n"

        await update.message.reply_text(msg, parse_mode="Markdown")

        chart_path = plot_behavior(card_id, feats)
        await update.message.reply_photo(photo=open(chart_path, "rb"))
    except Exception as e:
        logger.error(e)
        await update.message.reply_text("❌ Ошибка при генерации инсайта.")

# /clients
async def clients(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        path = "clients_list.txt"
        df_clients["card_id"].to_csv(path, index=False, header=False)
        await update.message.reply_document(document=open(path, "rb"))
    except Exception as e:
        logger.error(e)
        await update.message.reply_text("Ошибка при отправке списка клиентов.")

# /random
async def random_client(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        row = df_clients.sample(1).iloc[0]
        seg_name, _ = SEGMENT_MAP.get(row["segment_id"], ("Неизвестно", ""))
        await update.message.reply_text(f"🎲 Случайный клиент: {row['card_id']}\nСегмент: {seg_name}")
    except Exception as e:
        logger.error(e)
        await update.message.reply_text("Ошибка при выборе случайного клиента.")

# /segments
async def segments(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        counts = df_clients["segment_id"].value_counts().sort_index()
        msg = "📊 Распределение по сегментам:\n"
        for seg_id, count in counts.items():
            name, _ = SEGMENT_MAP.get(seg_id, ("?", ""))
            msg += f"{name} (ID {seg_id}): {count} клиентов\n"
        await update.message.reply_text(msg)
    except Exception as e:
        logger.error(e)
        await update.message.reply_text("Ошибка при подсчёте сегментов.")

# Запуск бота
if __name__ == "__main__":
    if not BOT_TOKEN:
        raise ValueError("❌ TELEGRAM_BOT_TOKEN не найден в .env")

    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("segment", segment))
    app.add_handler(CommandHandler("insight", insight))
    app.add_handler(CommandHandler("clients", clients))
    app.add_handler(CommandHandler("random", random_client))
    app.add_handler(CommandHandler("segments", segments))

    print("✅ Бот запущен.")
    app.run_polling()
