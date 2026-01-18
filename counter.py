from flask import Flask, jsonify, render_template, request, send_file, redirect
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
import time
import sqlite3
import io
from openpyxl import Workbook

app = Flask(__name__)

def load_env():
    env_path = os.path.join(app.root_path, ".env")
    if not os.path.exists(env_path):
        return
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value

load_env()

# === Конфигурация API ===
API_BASE         = "https://crm23.sipspeak.ru/api/shared"
CALL_LIST_URL    = f"{API_BASE}/call/list"
HIST_URL         = f"{API_BASE}/user_report/list/history"
LIST_URL         = f"{API_BASE}/user_report/list"
CONTACT_LIST_URL = f"{API_BASE}/contact/list"      # для новых номеров
CAMPAIGN_LIST_URL = f"{API_BASE}/campaign/list"    # для получения активных проектов
USERS_LIST_URL   = f"{API_BASE}/user/list"         # для получения операторов
API_TOKEN        = "cc7g45ybc7g5ync84umc9gmu5c9g4mucc"
HEADERS          = {"Authorization": API_TOKEN, "Accept": "application/json"}
REQUEST_TIMEOUT  = 60  # таймаут в секундах
PAGE_SIZE        = 500  # размер страницы

# === База данных ===
DB_PATH = os.getenv("CALLCENTER_DB_PATH", os.path.join(app.root_path, "data", "callcenter.db"))
ADMIN_TOKEN = os.getenv("CALLCENTER_ADMIN_TOKEN", "")

# === Telegram Bot ===
BOT_TOKEN = "7657704358:AAHby9X8__-T0Hbvao3H0HQi5OdncyGoAJQ"
CHAT_ID   = [758234101, 453163837, 1906635370, 1930885085, 424502959]
bot = Bot(token=BOT_TOKEN)

# === Операторы и статус-маппинг ===
OPERATORS = {}

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

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_operator_stats (
                date TEXT NOT NULL,
                operator_id TEXT NOT NULL,
                operator_name TEXT NOT NULL,
                all_calls INTEGER NOT NULL,
                total_calls INTEGER NOT NULL,
                cs8_calls INTEGER NOT NULL,
                cs20_calls INTEGER NOT NULL,
                cs22_calls INTEGER NOT NULL,
                talk_sum INTEGER NOT NULL,
                talk_count INTEGER NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (date, operator_id)
            )
            """
        )
        conn.commit()
    finally:
        conn.close()

def db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def parse_date(date_str):
    return datetime.strptime(date_str, "%d-%m-%Y").date()

def format_date(d):
    return d.strftime("%d-%m-%Y")

def list_saved_dates():
    conn = db_connection()
    try:
        rows = conn.execute("SELECT DISTINCT date FROM daily_operator_stats ORDER BY date").fetchall()
        return [row["date"] for row in rows]
    finally:
        conn.close()

def to_db_key(date_str):
    return parse_date(date_str).strftime("%Y%m%d")

def get_db_range():
    conn = db_connection()
    try:
        row = conn.execute(
            """
            SELECT date FROM daily_operator_stats
            ORDER BY (substr(date,7,4)||substr(date,4,2)||substr(date,1,2)) ASC
            LIMIT 1
            """
        ).fetchone()
        row_max = conn.execute(
            """
            SELECT date FROM daily_operator_stats
            ORDER BY (substr(date,7,4)||substr(date,4,2)||substr(date,1,2)) DESC
            LIMIT 1
            """
        ).fetchone()
        return (row["date"] if row else None, row_max["date"] if row_max else None)
    finally:
        conn.close()

def get_report_data(start_date=None, end_date=None):
    db_start, db_end = get_db_range()
    if not db_start or not db_end:
        return {
            "range": None,
            "rows": [],
            "totals": {}
        }
    if not start_date or not end_date:
        start_date, end_date = db_start, db_end
    if parse_date(start_date) > parse_date(end_date):
        start_date, end_date = end_date, start_date

    start_key = to_db_key(start_date)
    end_key = to_db_key(end_date)

    conn = db_connection()
    try:
        rows = conn.execute(
            """
            SELECT
                operator_id,
                operator_name,
                SUM(all_calls) AS all_calls,
                SUM(total_calls) AS total_calls,
                SUM(cs8_calls) AS cs8_calls,
                SUM(cs20_calls) AS cs20_calls,
                SUM(cs22_calls) AS cs22_calls,
                SUM(talk_sum) AS talk_sum,
                SUM(talk_count) AS talk_count
            FROM daily_operator_stats
            WHERE (substr(date,7,4)||substr(date,4,2)||substr(date,1,2)) BETWEEN ? AND ?
            GROUP BY operator_id, operator_name
            ORDER BY operator_name
            """,
            (start_key, end_key)
        ).fetchall()
    finally:
        conn.close()

    result_rows = []
    totals = {
        "all": 0,
        "total": 0,
        "cs8": 0,
        "cs20": 0,
        "cs22": 0,
        "talk_sum": 0,
        "talk_count": 0
    }
    for row in rows:
        talk_count = row["talk_count"] or 0
        talk_sum = row["talk_sum"] or 0
        avg = int(talk_sum / talk_count) if talk_count else 0
        result_rows.append({
            "operator_id": row["operator_id"],
            "operator_name": row["operator_name"],
            "all": row["all_calls"] or 0,
            "total": row["total_calls"] or 0,
            "cs8": row["cs8_calls"] or 0,
            "cs20": row["cs20_calls"] or 0,
            "cs22": row["cs22_calls"] or 0,
            "avg": avg
        })
        totals["all"] += row["all_calls"] or 0
        totals["total"] += row["total_calls"] or 0
        totals["cs8"] += row["cs8_calls"] or 0
        totals["cs20"] += row["cs20_calls"] or 0
        totals["cs22"] += row["cs22_calls"] or 0
        totals["talk_sum"] += talk_sum
        totals["talk_count"] += talk_count

    avg_total = int(totals["talk_sum"] / totals["talk_count"]) if totals["talk_count"] else 0
    reach = round((totals["total"] / totals["all"]) * 100, 1) if totals["all"] else 0

    return {
        "range": {"start": start_date, "end": end_date},
        "rows": result_rows,
        "totals": {
            "all": totals["all"],
            "total": totals["total"],
            "cs8": totals["cs8"],
            "cs20": totals["cs20"],
            "cs22": totals["cs22"],
            "avg": avg_total,
            "reach": reach
        }
    }
def fetch_calls_for_date(date_str, operators_map=None):
    params_base = build_base_params(requested_date=date_str, operators_map=operators_map)
    page = 1
    limit = 1000
    items = []
    while True:
        params = params_base + [("page", page), ("limit", limit)]
        r = requests.get(CALL_LIST_URL, params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        chunk = r.json().get("items", [])
        if not chunk:
            break
        items.extend(chunk)
        if len(chunk) < limit:
            break
        page += 1
    return items

def aggregate_calls(calls, operators_map=None):
    stats = defaultdict(lambda: {
        "all": 0,
        "total": 0,
        "cs8": 0,
        "cs20": 0,
        "cs22": 0,
        "talk_sum": 0,
        "talk_count": 0,
        "name": ""
    })
    for call in calls:
        op = call.get("operator") or {}
        oid = str(op.get("id") or "")
        if not oid:
            continue
        if operators_map and oid not in operators_map:
            continue
        name = op.get("full_name") or op.get("fullName") or operators_map.get(oid, "") if operators_map else ""
        status_obj = call.get("client_status") or {}
        status = str(status_obj.get("id") or "")
        td = call.get("talk_duration") or 0
        stats[oid]["all"] += 1
        if status in STAT_FULL:
            stats[oid]["total"] += 1
        if status in CS8:
            stats[oid]["cs8"] += 1
        if status in CS20:
            stats[oid]["cs20"] += 1
        if status in CS22:
            stats[oid]["cs22"] += 1
        stats[oid]["talk_sum"] += int(td)
        stats[oid]["talk_count"] += 1
        if name:
            stats[oid]["name"] = name
    return stats

def upsert_daily_stats(date_str, stats):
    now = datetime.now(pytz.timezone("Europe/Samara")).strftime("%Y-%m-%d %H:%M:%S")
    conn = db_connection()
    try:
        for oid, data in stats.items():
            name = data["name"] or oid
            conn.execute(
                """
                INSERT INTO daily_operator_stats
                (date, operator_id, operator_name, all_calls, total_calls, cs8_calls, cs20_calls, cs22_calls, talk_sum, talk_count, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(date, operator_id) DO UPDATE SET
                    operator_name=excluded.operator_name,
                    all_calls=excluded.all_calls,
                    total_calls=excluded.total_calls,
                    cs8_calls=excluded.cs8_calls,
                    cs20_calls=excluded.cs20_calls,
                    cs22_calls=excluded.cs22_calls,
                    talk_sum=excluded.talk_sum,
                    talk_count=excluded.talk_count,
                    updated_at=excluded.updated_at
                """,
                (date_str, oid, name, data["all"], data["total"], data["cs8"], data["cs20"], data["cs22"], data["talk_sum"], data["talk_count"], now)
            )
        conn.commit()
    finally:
        conn.close()

def sync_day(date_str):
    fetch_operators()
    calls = fetch_calls_for_date(date_str, operators_map=OPERATORS)
    operators_from_calls = extract_operators_from_calls(calls)
    operators_map = OPERATORS or operators_from_calls
    stats = aggregate_calls(calls, operators_map=operators_map)
    upsert_daily_stats(date_str, stats)
    return {
        "date": date_str,
        "operators": len(stats),
        "calls": sum(item["all"] for item in stats.values())
    }

def sync_range(start_str, end_str):
    start = parse_date(start_str)
    end = parse_date(end_str)
    if start > end:
        start, end = end, start
    out = []
    d = start
    while d <= end:
        out.append(sync_day(format_date(d)))
        d += timedelta(days=1)
    return out

def fetch_operators():
    """Получает список операторов из API"""
    global OPERATORS
    operators = {}
    page = 1
    limit = 500
    try:
        while True:
            params = [("page", page), ("limit", limit), ("removed", "false")]
            r = requests.get(USERS_LIST_URL, params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            items = r.json().get("items", [])
            if not items:
                break
            for user in items:
                if user.get("role") == "ROLE_OPERATOR":
                    uid = str(user.get("id") or "")
                    name = user.get("full_name") or user.get("fullName") or ""
                    if uid and name:
                        operators[uid] = name
            if len(items) < limit:
                break
            page += 1
    except Exception as e:
        print(f"Ошибка при получении списка операторов: {str(e)}")
        return OPERATORS
    if operators:
        OPERATORS = operators
    return OPERATORS

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

def build_base_params(requested_date=None, operators_map=None):
    if requested_date:
        date = requested_date
    elif TEST_DATE:
        date = TEST_DATE
    else:
        tz   = pytz.timezone("Europe/Samara")
        date = datetime.now(tz).strftime("%d-%m-%Y")
    params = [
        ("start_at", f"{date} 00:00"),
        ("end_at",   f"{date} 23:59")
    ]
    if operators_map is None:
        operators_map = OPERATORS
    if operators_map:
        for op in operators_map:
            params.append(("operators[]", op))
    return params


def fetch_counts(status_list, requested_date=None, operators_map=None):
    if operators_map is None:
        operators_map = OPERATORS
    params = build_base_params(requested_date=requested_date, operators_map=operators_map) + ALL_CALLS_PARAMS
    for s in status_list:
        params.append(("client_statuses[]", s))
    r = requests.get(CALL_LIST_URL, params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    cnt = Counter()
    for it in r.json().get("items", []):
        oid = str(it.get("operator",{}).get("id") or "")
        if not operators_map or oid in operators_map:
            cnt[oid] += 1
    return cnt


def fetch_all_counts(requested_date=None, operators_map=None):
    return fetch_counts([], requested_date=requested_date, operators_map=operators_map)


def fetch_all_calls_details(requested_date=None, operators_map=None):
    params = build_base_params(requested_date=requested_date, operators_map=operators_map) + ALL_CALLS_PARAMS
    r = requests.get(CALL_LIST_URL, params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json().get("items", [])


def fetch_current_status(requested_date=None, operators_map=None):
    if operators_map is None:
        operators_map = OPERATORS
    params = build_base_params(requested_date=requested_date, operators_map=operators_map) + [("page",1),("limit",1000)]
    r1 = requests.get(HIST_URL, params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT); r1.raise_for_status()
    r2 = requests.get(LIST_URL, params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT); r2.raise_for_status()
    status = {}
    for ev in r1.json().get("items", []):
        oid = str(ev.get("id") or "")
        if (not operators_map or oid in operators_map) and ev.get("event"):
            status[oid] = ev["event"]
    for ev in r2.json().get("items", []):
        oid = str(ev.get("id") or "")
        if (not operators_map or oid in operators_map) and ev.get("event"):
            status[oid] = ev["event"]
    return {oid: STATUS_MAP.get(st, st) for oid,st in status.items()}


def extract_operators_from_calls(calls):
    operators = {}
    for call in calls:
        op = call.get("operator") or {}
        oid = str(op.get("id") or "")
        name = op.get("full_name") or op.get("fullName") or ""
        if oid and name:
            operators[oid] = name
    return operators


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
    if not OPERATORS:
        fetch_operators()
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
    if not OPERATORS:
        fetch_operators()
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
    if not OPERATORS:
        fetch_operators()
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

def sync_yesterday():
    tz = pytz.timezone("Europe/Samara")
    day = datetime.now(tz).date() - timedelta(days=1)
    try:
        result = sync_day(format_date(day))
        print(f"Daily sync completed: {result}")
    except Exception as e:
        print(f"Daily sync failed: {str(e)}")

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
        # Автосинхронизация вчерашнего дня в 00:10
        sched.add_job(sync_yesterday, 'cron', hour=0, minute=10)
        sched.start()

def render_dashboard():
    today = TEST_DATE or datetime.now(pytz.timezone("Europe/Samara")).strftime("%d.%m.%Y")
    return render_template('index.html', today=today)

@app.route('/')
def index():
    return redirect('/dashboard')

@app.route('/dashboard')
def dashboard():
    return render_dashboard()

@app.route('/reports')
def reports():
    return send_file(os.path.join(app.root_path, 'templates', 'reports.html'))

@app.route('/report/data')
def report_data():
    start = request.args.get("start")
    end = request.args.get("end")
    return jsonify(get_report_data(start, end))

@app.route('/report/export')
def report_export():
    fmt = (request.args.get("format") or "xlsx").lower()
    if fmt != "xlsx":
        return jsonify({"error": "format not supported"}), 400
    start = request.args.get("start")
    end = request.args.get("end")
    data = get_report_data(start, end)
    wb = Workbook()
    ws = wb.active
    ws.title = "Report"
    ws.append(["Оператор", "Всего", "Диалогов", "Перевод", "Согласие", "Агент. согласие", "Среднее, сек"])
    for row in data["rows"]:
        ws.append([
            row["operator_name"],
            row["all"],
            row["total"],
            row["cs20"],
            row["cs8"],
            row["cs22"],
            row["avg"]
        ])
    totals = data.get("totals") or {}
    if totals:
        ws.append([])
        ws.append([
            "ИТОГО",
            totals.get("all", 0),
            totals.get("total", 0),
            totals.get("cs20", 0),
            totals.get("cs8", 0),
            totals.get("cs22", 0),
            totals.get("avg", 0)
        ])
    period = data.get("range")
    filename = "report.xlsx"
    if period:
        filename = f"report_{period['start']}_{period['end']}.xlsx"
    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    return send_file(
        output,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

@app.route('/admin')
def admin_page():
    if not require_admin():
        return jsonify({"error": "unauthorized"}), 401
    return send_file(os.path.join(app.root_path, 'templates', 'admin.html'))

def require_admin():
    token = request.args.get("token") or ""
    # Reload from env to avoid stale token after .env edits
    env_token = os.getenv("CALLCENTER_ADMIN_TOKEN") or ""
    if not env_token:
        load_env()
        env_token = os.getenv("CALLCENTER_ADMIN_TOKEN") or ""
    if not env_token or token != env_token:
        return False
    return True

@app.route('/admin/db')
def admin_db():
    if not require_admin():
        return jsonify({"error": "unauthorized"}), 401
    start = request.args.get("start")
    end = request.args.get("end")
    available = list_saved_dates()
    missing = []
    if start and end:
        start_date = parse_date(start)
        end_date = parse_date(end)
        if start_date > end_date:
            start_date, end_date = end_date, start_date
        available_set = set(available)
        d = start_date
        while d <= end_date:
            ds = format_date(d)
            if ds not in available_set:
                missing.append(ds)
            d += timedelta(days=1)
    return jsonify({
        "available": available,
        "missing": missing
    })

@app.route('/admin/sync')
def admin_sync():
    if not require_admin():
        return jsonify({"error": "unauthorized"}), 401
    date = request.args.get("date")
    start = request.args.get("start")
    end = request.args.get("end")
    try:
        if date:
            result = sync_day(date)
            return jsonify({"status": "ok", "result": result})
        if start and end:
            result = sync_range(start, end)
            return jsonify({"status": "ok", "result": result})
        return jsonify({"error": "date or start/end required"}), 400
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"Admin sync failed: {str(e)}")
        print(error_details)
        return jsonify({"error": str(e), "details": error_details}), 500

@app.route('/stats')
def stats():
    requested_date = request.args.get("date")
    fetch_operators()
    calls   = fetch_all_calls_details(requested_date=requested_date, operators_map=OPERATORS)
    operators_from_calls = extract_operators_from_calls(calls)
    operators_map = OPERATORS or operators_from_calls
    total   = fetch_counts(STAT_FULL, requested_date=requested_date, operators_map=operators_map)
    cs8     = fetch_counts(CS8, requested_date=requested_date, operators_map=operators_map)
    cs20    = fetch_counts(CS20, requested_date=requested_date, operators_map=operators_map)
    cs22    = fetch_counts(CS22, requested_date=requested_date, operators_map=operators_map)
    allc    = fetch_all_counts(requested_date=requested_date, operators_map=operators_map)
    new_tot = fetch_new_numbers_total_by_active()
    new_noactive_tot = fetch_new_numbers_total_by_noactive()
    sums, cnts = defaultdict(int), defaultdict(int)
    for c in calls:
        oid = str(c.get("operator",{}).get("id") or "")
        if not operators_map or oid in operators_map:
            td = c.get("talk_duration") or 0
            sums[oid] += td
            cnts[oid] += 1
    avg    = {oid:(sums[oid]//cnts[oid] if cnts[oid] else 0) for oid in operators_map}
    status = fetch_current_status(requested_date=requested_date, operators_map=operators_map)

    active_operator_ids = {oid for oid, count in allc.items() if count > 0}
    operators_filtered = {oid: name for oid, name in operators_map.items() if oid in active_operator_ids}

    return jsonify({
        "operators": operators_filtered,
        "status":    {oid: status.get(oid) for oid in operators_filtered},
        "all":       {oid: allc.get(oid, 0) for oid in operators_filtered},
        "total":     {oid: total.get(oid, 0) for oid in operators_filtered},
        "cs8":       {oid: cs8.get(oid, 0) for oid in operators_filtered},
        "cs20":      {oid: cs20.get(oid, 0) for oid in operators_filtered},
        "cs22":      {oid: cs22.get(oid, 0) for oid in operators_filtered},
        "avg":       {oid: avg.get(oid, 0) for oid in operators_filtered},
        "new":       new_tot,
        "new_noactive": new_noactive_tot,
        "server_time": int(time.time() * 1000)
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
    init_db()
    init_scheduler()
    app.run(host='0.0.0.0', port=8000)
