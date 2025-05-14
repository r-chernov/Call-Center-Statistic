from flask import Flask, jsonify, render_template
import requests
from datetime import datetime
import pytz
from collections import Counter, defaultdict
from telegram import Bot
from apscheduler.schedulers.background import BackgroundScheduler
import asyncio

app = Flask(__name__)

# === Конфигурация API и бота ===
API_BASE        = "https://crm23.sipspeak.ru/api/shared"
CALL_LIST_URL   = f"{API_BASE}/call/list"
USER_REPORT_URL = f"{API_BASE}/user_report/list"
API_TOKEN       = "sdsa1232313"
HEADERS         = {"Authorization": API_TOKEN, "Accept": "application/json"}

BOT_TOKEN = "7657704358:AAHby9X8__-T0Hbvao3H0HQi5OdncyGoAJQ"
CHAT_ID   = 758234101  # ваш пользовательский или групповой chat_id
bot = Bot(token=BOT_TOKEN)

# Операторы
OPERATORS = {
    "25": "Рыжов Сергей Дмитриевич",
    "24": "Чумакина Светлана Анатольевна",
    "23": "Котманова Ольга Юрьевна",
    "22": "Драгомиров Олег Юрьевич",
    "21": "Конакова Людмила Викторовна",
    "20": "Подосиновская Алла Евгеньевна",
    "18": "Миридонов Дмитрий Михайлович",
    "17": "Булдаковская Анна Сергеевна"
}

# Статусы
STATUSES_FULL = ["8","9","10","11","13","14","15","16","20","21","22","23","24","25"]
CS8    = ["8"]   # «Согласие на встречу»
CS20   = ["20"]  # «Перевод»
ALL_CALLS_PARAMS = [("page",1), ("limit",10000)]

def build_base_params():
    tz   = pytz.timezone("Europe/Samara")
    now  = datetime.now(tz)
    date = now.strftime("%d-%m-%Y")
    params = [
        ("start_at", f"{date} 00:00"),
        ("end_at",   f"{date} 23:59")
    ]
    for op_id in OPERATORS:
        params.append(("operators[]", op_id))
    return params

def fetch_counts(status_list):
    params = build_base_params() + ALL_CALLS_PARAMS
    for st in status_list:
        params.append(("client_statuses[]", st))
    r = requests.get(CALL_LIST_URL, params=params, headers=HEADERS)
    r.raise_for_status()
    cnt = Counter()
    for item in r.json().get("items", []):
        oid = str(item.get("operator", {}).get("id") or "")
        if oid in OPERATORS:
            cnt[oid] += 1
    return cnt

def fetch_all_counts():
    params = build_base_params() + ALL_CALLS_PARAMS
    r = requests.get(CALL_LIST_URL, params=params, headers=HEADERS)
    r.raise_for_status()
    cnt = Counter()
    for item in r.json().get("items", []):
        oid = str(item.get("operator", {}).get("id") or "")
        if oid in OPERATORS:
            cnt[oid] += 1
    return cnt

def fetch_all_calls_details():
    params = build_base_params() + ALL_CALLS_PARAMS
    r = requests.get(CALL_LIST_URL, params=params, headers=HEADERS)
    r.raise_for_status()
    return r.json().get("items", [])

def fetch_current_status():
    params = build_base_params() + [("page",1), ("limit",1000)]
    r = requests.get(USER_REPORT_URL, params=params, headers=HEADERS)
    r.raise_for_status()
    status = {}
    for ev in r.json().get("items", []):
        oid   = str(ev.get("id") or "")
        event = ev.get("event")
        if oid in OPERATORS and event:
            status[oid] = event
    return status

def send_report():
    # собираем метрики
    total = fetch_counts(STATUSES_FULL)
    cs8   = fetch_counts(CS8)
    cs20  = fetch_counts(CS20)
    allc  = fetch_all_counts()

    calls = fetch_all_calls_details()
    sums, cnts = defaultdict(int), defaultdict(int)
    for c in calls:
        oid = str(c.get("operator",{}).get("id") or "")
        if oid in OPERATORS:
            td = c.get("talk_duration") or 0
            sums[oid] += td
            cnts[oid] += 1
    avg = {oid:(sums[oid]//cnts[oid] if cnts[oid] else 0) for oid in OPERATORS}

    # дата для заголовка
    tz    = pytz.timezone("Europe/Samara")
    today = datetime.now(tz).strftime("%d.%m.%Y")

    # формируем текст
    lines = [
        f"📊 *Отчёт КЦ за {today}*",
        "",
        "🧑‍💼 *Оператор* | 📞 *Всего* | 🤝 *Согласие* | 🔄 *Перевод* | ✅ *Сост.* | ⏱ *Ср., сек*",
    ]
    for oid, name in OPERATORS.items():
        lines.append(
            f"{name} | {allc.get(oid,0)} | {cs8.get(oid,0)} | {cs20.get(oid,0)} | "
            f"{total.get(oid,0)} | {avg.get(oid,0)}"
        )

    text = "\n".join(lines)
    # отправляем
    asyncio.run(
        bot.send_message(chat_id=CHAT_ID, text=text, parse_mode="Markdown")
    )

# планировщик: ежедневно в 18:30 по Самаре
sched = BackgroundScheduler(timezone="Europe/Samara")
sched.add_job(send_report, 'cron', hour=18, minute=30)
sched.start()

@app.route('/')
def index():
    tz    = pytz.timezone("Europe/Samara")
    today = datetime.now(tz).strftime("%d.%m.%Y")
    return render_template('index.html', operators=OPERATORS, today=today)

@app.route('/stats')
def stats():
    total = fetch_counts(STATUSES_FULL)
    cs8   = fetch_counts(CS8)
    cs20  = fetch_counts(CS20)
    allc  = fetch_all_counts()

    calls = fetch_all_calls_details()
    sums, cnts = defaultdict(int), defaultdict(int)
    for c in calls:
        oid = str(c.get("operator",{}).get("id") or "")
        if oid in OPERATORS:
            td = c.get("talk_duration") or 0
            sums[oid] += td
            cnts[oid] += 1
    avg = {oid:(sums[oid]//cnts[oid] if cnts[oid] else 0) for oid in OPERATORS}

    status = fetch_current_status()

    return jsonify({
        "operators": OPERATORS,
        "status":    status,
        "all":       allc,
        "cs8":       cs8,
        "cs20":      cs20,
        "total":     total,
        "avg":       avg
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)