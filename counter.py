from flask import Flask, jsonify, render_template
import requests
from datetime import datetime
import pytz
from collections import Counter, defaultdict
from telegram import Bot
from apscheduler.schedulers.background import BackgroundScheduler
import asyncio
import os

app = Flask(__name__)

# === Конфигурация API ===
API_BASE         = "https://crm23.sipspeak.ru/api/shared"
CALL_LIST_URL    = f"{API_BASE}/call/list"
HIST_URL         = f"{API_BASE}/user_report/list/history"
LIST_URL         = f"{API_BASE}/user_report/list"
CONTACT_LIST_URL = f"{API_BASE}/contact/list"      # для новых номеров
API_TOKEN        = "sdsa1232313"
HEADERS          = {"Authorization": API_TOKEN, "Accept": "application/json"}

# === Telegram Bot ===
BOT_TOKEN = "7657704358:AAHby9X8__-T0Hbvao3H0HQi5OdncyGoAJQ"
CHAT_ID   = [758234101, 453163837, 1906635370, 1930885085]
bot = Bot(token=BOT_TOKEN)

# === Операторы и статус-маппинг ===
OPERATORS = {
    "24": "Чумакина Светлана Анатольевна",
    "23": "Котманова Ольга Юрьевна",
    "21": "Конакова Людмила Викторовна",
    "20": "Подосиновская Алла Евгеньевна",
}

STATUS_MAP = {
    "queue":      "В очереди",
    "active":     "Активный",
    "no_active":  "Неактивный",
    "pause":      "Перерыв",
    "call":       "Разговор",
    "card":       "Карточка",
    "dnd":        "Не беспокоить",
    "incoming":   "Входящий",
    "ringing":    "Звонит",
    "working_day":"Рабочий день"
}

ALL_CALLS_PARAMS = [("page",1), ("limit",10000)]
STAT_FULL = ["8","9","10","11","13","14","15","16","20","21","22","23","24","25"]
CS8        = ["8"]
CS20       = ["20"]
CS22       = ["22"]

# для тестирования локально:
TEST_DATE = os.getenv("TEST_DATE")  # e.g. "14-05-2025"

def build_base_params():
    if TEST_DATE:
        date = TEST_DATE
    else:
        tz   = pytz.timezone("Europe/Samara")
        date = datetime.now(tz).strftime("%d-%m-%Y")
    params = [
        ("start_at", f"{date} 00:00"),
        ("end_at",   f"{date} 23:59")
    ]
    for op in OPERATORS:
        params.append(("operators[]", op))
    return params


def fetch_counts(status_list):
    params = build_base_params() + ALL_CALLS_PARAMS
    for s in status_list:
        params.append(("client_statuses[]", s))
    r = requests.get(CALL_LIST_URL, params=params, headers=HEADERS)
    r.raise_for_status()
    cnt = Counter()
    for it in r.json().get("items", []):
        oid = str(it.get("operator",{}).get("id") or "")
        if oid in OPERATORS:
            cnt[oid] += 1
    return cnt


def fetch_all_counts():
    return fetch_counts([])


def fetch_all_calls_details():
    params = build_base_params() + ALL_CALLS_PARAMS
    r = requests.get(CALL_LIST_URL, params=params, headers=HEADERS)
    r.raise_for_status()
    return r.json().get("items", [])


def fetch_current_status():
    params = build_base_params() + [("page",1),("limit",1000)]
    r1 = requests.get(HIST_URL, params=params, headers=HEADERS); r1.raise_for_status()
    r2 = requests.get(LIST_URL, params=params, headers=HEADERS); r2.raise_for_status()
    status = {}
    for ev in r1.json().get("items", []):
        oid = str(ev.get("id") or "")
        if oid in OPERATORS and ev.get("event"):
            status[oid] = ev["event"]
    for ev in r2.json().get("items", []):
        oid = str(ev.get("id") or "")
        if oid in OPERATORS and ev.get("event"):
            status[oid] = ev["event"]
    return {oid: STATUS_MAP.get(st, st) for oid,st in status.items()}


def fetch_new_numbers_total():
    params = [
        ("statuses[]", "1"),
        ("campaign_ids[]", "67"),
        ("campaign_ids[]", "70"),
        ("page", 1),
        ("limit", 1),
    ]
    r = requests.get(CONTACT_LIST_URL, params=params, headers=HEADERS)
    r.raise_for_status()
    data = r.json()
    return data.get("totalCount", len(data.get("items", [])))


def send_report():
    print(f"Starting report generation at {datetime.now(pytz.timezone('Europe/Samara'))}")
    total = fetch_counts(STAT_FULL)
    cs8   = fetch_counts(CS8)
    cs20  = fetch_counts(CS20)
    cs22  = fetch_counts(CS22)
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

    tz    = pytz.timezone("Europe/Samara")
    today = datetime.now(tz).strftime("%d.%m.%Y")
    lines = [f"*Отчёт КЦ за {today}*"]
    for oid,name in OPERATORS.items():
        lines.append(
            f"{name}\n"
            f"Всего звонков: {allc.get(oid,0)}\n"
            f"Диалогов:        {total.get(oid,0)}\n"
            f"Согласие:        {cs8.get(oid,0)}\n"
            f"Перевод:         {cs20.get(oid,0)}\n"
            f"Агент. Согласие:       {cs22.get(oid,0)}\n"
            f"Средн. время:    {avg.get(oid,0)}"
        )
    text = "\n\n".join(lines)
    for cid in CHAT_ID:
        try:
            print(f"Sending report to chat {cid}")
            asyncio.run(bot.send_message(chat_id=cid, text=text, parse_mode="Markdown"))
            print(f"Successfully sent report to chat {cid}")
        except Exception as e:
            print(f"Ошибка отправки в чат {cid}: {str(e)}")
    print("Report generation and sending completed")

sched = None

def init_scheduler():
    global sched
    if sched is None:
        sched = BackgroundScheduler(timezone="Europe/Samara")
        sched.add_job(lambda: requests.get('http://82.97.249.124/send_report'), 'cron', hour=18, minute=30)
        sched.start()

@app.route('/')
def index():
    today = TEST_DATE or datetime.now(pytz.timezone("Europe/Samara")).strftime("%d.%m.%Y")
    return render_template('index.html', today=today)

@app.route('/stats')
def stats():
    total   = fetch_counts(STAT_FULL)
    cs8     = fetch_counts(CS8)
    cs20    = fetch_counts(CS20)
    cs22    = fetch_counts(CS22)
    allc    = fetch_all_counts()
    new_tot = fetch_new_numbers_total()
    calls   = fetch_all_calls_details()
    sums, cnts = defaultdict(int), defaultdict(int)
    for c in calls:
        oid = str(c.get("operator",{}).get("id") or "")
        if oid in OPERATORS:
            td = c.get("talk_duration") or 0
            sums[oid] += td
            cnts[oid] += 1
    avg    = {oid:(sums[oid]//cnts[oid] if cnts[oid] else 0) for oid in OPERATORS}
    status = fetch_current_status()

    return jsonify({
        "operators": OPERATORS,
        "status":    status,
        "all":       allc,
        "total":     total,
        "cs8":       cs8,
        "cs20":      cs20,
        "cs22":      cs22,
        "avg":       avg,
        "new":       new_tot
    })

@app.route('/send_report')
def trigger_report():
    try:
        send_report()
        return jsonify({"status": "success", "message": "Отчет успешно отправлен"})
    except Exception as e:
        return jsonify({"status": "error", "message": f"Ошибка при отправке отчета: {str(e)}"}), 500

if __name__ == '__main__':
    # Инициализируем планировщик только в основном процессе Flask (избегая двойного запуска при debug)
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        init_scheduler()
    app.run(host='0.0.0.0', port=8000)
