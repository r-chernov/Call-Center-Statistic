from flask import Flask, jsonify, render_template
import requests
from datetime import datetime, timedelta
import pytz
from collections import Counter, defaultdict
from telegram import Bot
from apscheduler.schedulers.background import BackgroundScheduler
import asyncio
import os
import inspect
import re

app = Flask(__name__)

# === Конфигурация API ===
API_BASE         = "https://crm23.sipspeak.ru/api/shared"
CALL_LIST_URL    = f"{API_BASE}/call/list"
HIST_URL         = f"{API_BASE}/user_report/list/history"
LIST_URL         = f"{API_BASE}/user_report/list"
CONTACT_LIST_URL = f"{API_BASE}/contact/list"      # для новых номеров
CAMPAIGN_LIST_URL = f"{API_BASE}/campaign/list"    # для получения активных проектов
API_TOKEN        = "sdsa1232313"
HEADERS          = {"Authorization": API_TOKEN, "Accept": "application/json"}
REQUEST_TIMEOUT  = 60  # таймаут в секундах
PAGE_SIZE        = 500  # размер страницы

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

# Глобальная переменная для хранения активных проектов
active_campaigns = []

def fetch_active_campaigns():
    """Получает список активных проектов"""
    global active_campaigns
    try:
        print("Получение списка активных проектов...")
        params = [("active", "true")]
        r = requests.get(CAMPAIGN_LIST_URL, params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        campaigns = r.json().get("items", [])
        active_campaigns = [str(camp.get("id")) for camp in campaigns if camp.get("id")]
        print(f"Получено {len(active_campaigns)} активных проектов: {active_campaigns}")
        return active_campaigns
    except Exception as e:
        print(f"Ошибка при получении активных проектов: {str(e)}")
        return []

def fetch_new_numbers_total_by_active():
    """Получает количество новых номеров для активных проектов"""
    if not active_campaigns:
        fetch_active_campaigns()
    
    params = [("statuses[]", "1")]
    for campaign_id in active_campaigns:
        params.append(("campaign_ids[]", campaign_id))
    params.append(("page", 1))
    params.append(("limit", 1))

    try:
        r = requests.get(CONTACT_LIST_URL, params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        return data.get("totalCount", len(data.get("items", [])))
    except Exception as e:
        print(f"Ошибка при получении количества новых номеров: {str(e)}")
        return 0

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
    r = requests.get(CALL_LIST_URL, params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT)
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
    r = requests.get(CALL_LIST_URL, params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json().get("items", [])


def fetch_current_status():
    params = build_base_params() + [("page",1),("limit",1000)]
    r1 = requests.get(HIST_URL, params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT); r1.raise_for_status()
    r2 = requests.get(LIST_URL, params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT); r2.raise_for_status()
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


def fetch_new_numbers_total_by_noactive():
    # Получаем исходный код функции fetch_new_numbers_total_by_active
    source = inspect.getsource(fetch_new_numbers_total_by_active)
    
    # Ищем все campaign_ids[] в формате ("campaign_ids[]", "<число>")
    active_ids = set(map(int, re.findall(r'\("campaign_ids\[\]",\s*"(\d+)"\)', source)))

    # Генерируем ID от 0 до 100, кроме найденных
    all_ids = set(range(0, 101))
    noactive_ids = sorted(all_ids - active_ids)

    # Собираем параметры запроса
    params = [("statuses[]", "1")]
    for campaign_id in noactive_ids:
        params.append(("campaign_ids[]", str(campaign_id)))
    params.append(("page", 1))
    params.append(("limit", 1))

    r = requests.get(CONTACT_LIST_URL, params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT)
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

def build_monthly_params():
    tz = pytz.timezone("Europe/Samara")
    now = datetime.now(tz)
    # Получаем первый день текущего месяца
    first_day = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    # Получаем последний день текущего месяца
    if now.month == 12:
        last_day = now.replace(year=now.year + 1, month=1, day=1) - timedelta(days=1)
    else:
        last_day = now.replace(month=now.month + 1, day=1) - timedelta(days=1)
    
    params = [
        ("start_at", first_day.strftime("%d-%m-%Y 00:00")),
        ("end_at", last_day.strftime("%d-%m-%Y 23:59"))
    ]
    for op in OPERATORS:
        params.append(("operators[]", op))
    return params

def send_monthly_report():
    print(f"Starting monthly report generation at {datetime.now(pytz.timezone('Europe/Samara'))}")
    params = build_monthly_params()
    
    # Получаем все звонки за месяц
    all_calls = []
    page = 1
    while True:
        current_params = params + [("page", page), ("limit", 1000)]
        r = requests.get(CALL_LIST_URL, params=current_params, headers=HEADERS)
        r.raise_for_status()
        items = r.json().get("items", [])
        if not items:
            break
        all_calls.extend(items)
        page += 1

    # Считаем статистику
    total = Counter()
    cs8 = Counter()
    cs20 = Counter()
    cs22 = Counter()
    allc = Counter()
    sums = defaultdict(int)
    cnts = defaultdict(int)

    for call in all_calls:
        oid = str(call.get("operator", {}).get("id") or "")
        if oid in OPERATORS:
            allc[oid] += 1
            status = str(call.get("client_status", {}).get("id") or "")
            if status in STAT_FULL:
                total[oid] += 1
            if status in CS8:
                cs8[oid] += 1
            if status in CS20:
                cs20[oid] += 1
            if status in CS22:
                cs22[oid] += 1
            
            td = call.get("talk_duration") or 0
            sums[oid] += td
            cnts[oid] += 1

    avg = {oid: (sums[oid] // cnts[oid] if cnts[oid] else 0) for oid in OPERATORS}

    tz = pytz.timezone("Europe/Samara")
    now = datetime.now(tz)
    month_name = now.strftime("%B").lower()
    lines = [f"*Месячный отчёт КЦ за {month_name} {now.year}*"]
    
    for oid, name in OPERATORS.items():
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
            print(f"Sending monthly report to chat {cid}")
            asyncio.run(bot.send_message(chat_id=cid, text=text, parse_mode="Markdown"))
            print(f"Successfully sent monthly report to chat {cid}")
        except Exception as e:
            print(f"Ошибка отправки месячного отчета в чат {cid}: {str(e)}")
    print("Monthly report generation and sending completed")

def build_monthly_params_for_date(year, month):
    tz = pytz.timezone("Europe/Samara")
    # Получаем первый день указанного месяца
    first_day = datetime(year, month, 1, tzinfo=tz)
    # Получаем последний день указанного месяца
    if month == 12:
        last_day = datetime(year + 1, 1, 1, tzinfo=tz) - timedelta(days=1)
    else:
        last_day = datetime(year, month + 1, 1, tzinfo=tz) - timedelta(days=1)
    
    params = [
        ("start_at", first_day.strftime("%d-%m-%Y 00:00")),
        ("end_at", last_day.strftime("%d-%m-%Y 23:59"))
    ]
    for op in OPERATORS:
        params.append(("operators[]", op))
    return params

def send_monthly_report_for_date(year, month):
    print(f"Starting monthly report generation for {month}/{year} at {datetime.now(pytz.timezone('Europe/Samara'))}")
    params = build_monthly_params_for_date(year, month)
    
    # Инициализируем счетчики
    total = Counter()
    cs8 = Counter()
    cs20 = Counter()
    cs22 = Counter()
    allc = Counter()
    sums = defaultdict(int)
    cnts = defaultdict(int)
    
    # Получаем данные постранично
    page = 1
    while True:
        try:
            print(f"Получение страницы {page}...")
            current_params = params + [("page", page), ("limit", PAGE_SIZE)]
            r = requests.get(CALL_LIST_URL, params=current_params, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            items = r.json().get("items", [])
            
            if not items:
                print(f"Страница {page} пуста, завершаем получение данных")
                break
                
            print(f"Обработка {len(items)} звонков на странице {page}")
            
            # Обрабатываем звонки на текущей странице
            for call in items:
                oid = str(call.get("operator", {}).get("id") or "")
                if oid in OPERATORS:
                    allc[oid] += 1
                    status = str(call.get("client_status", {}).get("id") or "")
                    if status in STAT_FULL:
                        total[oid] += 1
                    if status in CS8:
                        cs8[oid] += 1
                    if status in CS20:
                        cs20[oid] += 1
                    if status in CS22:
                        cs22[oid] += 1
                    
                    td = call.get("talk_duration") or 0
                    sums[oid] += td
                    cnts[oid] += 1
            
            page += 1
            
        except requests.exceptions.Timeout:
            print(f"Таймаут при получении страницы {page}")
            break
        except Exception as e:
            print(f"Ошибка при получении страницы {page}: {str(e)}")
            break

    if not any(allc.values()):
        error_msg = "Не удалось получить данные о звонках"
        print(error_msg)
        return error_msg

    avg = {oid: (sums[oid] // cnts[oid] if cnts[oid] else 0) for oid in OPERATORS}

    month_names = {
        1: "январь", 2: "февраль", 3: "март", 4: "апрель",
        5: "май", 6: "июнь", 7: "июль", 8: "август",
        9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь"
    }
    
    lines = [f"*Месячный отчёт КЦ за {month_names[month]} {year}*"]
    
    for oid, name in OPERATORS.items():
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
    
    # Отправляем отчет в Telegram
    success = False
    for cid in CHAT_ID:
        try:
            print(f"Sending monthly report to chat {cid}")
            asyncio.run(bot.send_message(chat_id=cid, text=text, parse_mode="Markdown"))
            print(f"Successfully sent monthly report to chat {cid}")
            success = True
        except Exception as e:
            print(f"Ошибка отправки месячного отчета в чат {cid}: {str(e)}")
    
    if not success:
        error_msg = "Не удалось отправить отчет ни в один из чатов"
        print(error_msg)
        return error_msg
        
    print("Monthly report generation and sending completed")
    return text

sched = None

def init_scheduler():
    global sched
    if sched is None:
        sched = BackgroundScheduler(timezone="Europe/Samara")
        # Ежедневный отчет в 18:30
        sched.add_job(send_report, 'cron', hour=18, minute=30)
        # Месячный отчет в последний день месяца в 19:00
        sched.add_job(send_monthly_report, 'cron', day='last', hour=19, minute=0)
        # Обновление списка активных проектов в 10:00
        sched.add_job(fetch_active_campaigns, 'cron', hour=10, minute=0)
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
    new_tot = fetch_new_numbers_total_by_active()
    new_noactive_tot = fetch_new_numbers_total_by_noactive()
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
        "new":       new_tot,
        "new_noactive": new_noactive_tot
    })

@app.route('/send_report')
def trigger_report():
    try:
        send_report()
        return jsonify({"status": "success", "message": "Отчет успешно отправлен"})
    except Exception as e:
        return jsonify({"status": "error", "message": f"Ошибка при отправке отчета: {str(e)}"}), 500

@app.route('/send_monthly_report/<int:year>/<int:month>')
def trigger_monthly_report(year, month):
    try:
        print(f"Начало обработки запроса месячного отчета для {year}/{month}")
        
        if not (1 <= month <= 12):
            error_msg = "Месяц должен быть от 1 до 12"
            print(error_msg)
            return jsonify({"status": "error", "message": error_msg}), 400
        
        current_year = datetime.now().year
        if not (2020 <= year <= current_year + 1):
            error_msg = f"Год должен быть от 2020 до {current_year + 1}"
            print(error_msg)
            return jsonify({"status": "error", "message": error_msg}), 400
            
        print("Параметры валидны, начинаем формирование отчета")
        text = send_monthly_report_for_date(year, month)
        
        if text.startswith("Не удалось получить данные"):
            return jsonify({"status": "error", "message": text}), 500
            
        print("Отчет успешно сформирован и отправлен")
        return jsonify({
            "status": "success", 
            "message": "Месячный отчет успешно отправлен",
            "report": text
        })
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"Ошибка при отправке месячного отчета: {str(e)}")
        print(f"Детали ошибки:\n{error_details}")
        return jsonify({
            "status": "error", 
            "message": f"Ошибка при отправке месячного отчета: {str(e)}",
            "details": error_details
        }), 500

if __name__ == '__main__':
    init_scheduler()
    app.run(host='0.0.0.0', port=8000)
