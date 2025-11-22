from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, Any
import sqlite3
import time
import json
import re

DB_PATH = "events.db"

app = FastAPI(title="Shopify Tracking Server (Captain Version v2 + Geo)")

# ------- CORS -------
origins = [
    "https://4pytkr-hy.myshopify.com",
    "https://www.4pytkr-hy.myshopify.com",
    "https://4pytkr-hy.myshopify.com/",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# -------- دوال مساعدة لقاعدة البيانات --------
def get_conn():
    return sqlite3.connect(DB_PATH)


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    # جدول الأجهزة (الهيكل الأساسي)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS devices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id TEXT UNIQUE,
            first_seen INTEGER,
            last_seen INTEGER,
            is_whatsapp INTEGER DEFAULT 0
        )
        """
    )

    # إضافة أعمدة معلومات الجهاز لو الداتابيس قديمة
    extra_device_columns = [
        "device_type",
        "device_brand",
        "device_model",
        "os_name",
        "os_version",
        "browser_name",
        "browser_version",
    ]
    for col in extra_device_columns:
        try:
            cur.execute(f"ALTER TABLE devices ADD COLUMN {col} TEXT")
        except sqlite3.OperationalError:
            # العمود موجود من قبل
            pass

    # جدول الجلسات
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT UNIQUE,
            device_id TEXT,
            first_seen INTEGER,
            last_seen INTEGER,
            traffic_source TEXT,
            utm_source TEXT,
            utm_medium TEXT,
            utm_campaign TEXT,
            utm_content TEXT,
            referrer_first TEXT,
            user_agent_first TEXT
        )
        """
    )

    # جدول الأحداث (مع أعمدة geo و session stats)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event TEXT,
            session_id TEXT,
            device_id TEXT,
            url TEXT,
            referrer TEXT,
            user_agent TEXT,
            traffic_source TEXT,
            utm_source TEXT,
            utm_medium TEXT,
            utm_campaign TEXT,
            utm_content TEXT,
            created_at INTEGER,
            meta TEXT,
            geo_country TEXT,
            geo_city TEXT,
            session_pages INTEGER,
            session_duration_ms INTEGER,
            template_name TEXT
        )
        """
    )

    # لو في داتابيس قديمة بدون الأعمدة الإضافية
    extra_event_columns = [
        ("geo_country", "TEXT"),
        ("geo_city", "TEXT"),
        ("session_pages", "INTEGER"),
        ("session_duration_ms", "INTEGER"),
        ("template_name", "TEXT"),
    ]
    for col, col_type in extra_event_columns:
        try:
            cur.execute(f"ALTER TABLE events ADD COLUMN {col} {col_type}")
        except sqlite3.OperationalError:
            # العمود موجود من قبل
            pass

    conn.commit()
    conn.close()


# استدعاء إنشاء / تحديث الجداول عند تشغيل السيرفر
init_db()


# -------- نماذج البيانات (Pydantic) --------
class EventIn(BaseModel):
    event: str
    session_id: str
    device_id: str

    url: Optional[str] = None
    referrer: Optional[str] = None
    user_agent: Optional[str] = None

    traffic_source: Optional[str] = None  # direct / referral / whatsapp / etc
    utm_source: Optional[str] = None
    utm_medium: Optional[str] = None
    utm_campaign: Optional[str] = None
    utm_content: Optional[str] = None

    # حقول إضافية من السكربت
    geo_country: Optional[str] = None
    geo_city: Optional[str] = None
    session_pages: Optional[int] = None
    session_duration_ms: Optional[int] = None
    template_name: Optional[str] = None
    timestamp: Optional[int] = None  # لو حابب تستقبله من الفرونت فقط، بدون تخزين

    meta: Optional[Dict[str, Any]] = None  # أي بيانات إضافية (product_id, value...)


# -------- تحليل user_agent لاستخراج نوع الجهاز والنظام والمتصفح --------
def parse_user_agent(ua: Optional[str]) -> Dict[str, Optional[str]]:
    info = {
        "device_type": None,
        "device_brand": None,
        "device_model": None,
        "os_name": None,
        "os_version": None,
        "browser_name": None,
        "browser_version": None,
    }
    if not ua:
        return info

    ua_l = ua.lower()

    # نوع الجهاز + الماركة الأساسية
    if "iphone" in ua_l:
        info["device_type"] = "Phone"
        info["device_brand"] = "Apple"
        info["device_model"] = "iPhone"
        info["os_name"] = "iOS"
    elif "ipad" in ua_l:
        info["device_type"] = "Tablet"
        info["device_brand"] = "Apple"
        info["device_model"] = "iPad"
        info["os_name"] = "iPadOS"
    elif "android" in ua_l:
        # موبايل أو تابلت
        if "mobile" in ua_l:
            info["device_type"] = "Phone"
        elif "tablet" in ua_l:
            info["device_type"] = "Tablet"
        else:
            info["device_type"] = "Android Device"
        info["os_name"] = "Android"

        # محاولة اكتشاف الماركة
        if "samsung" in ua_l or "sm-" in ua_l:
            info["device_brand"] = "Samsung"
        elif "huawei" in ua_l:
            info["device_brand"] = "Huawei"
        elif "xiaomi" in ua_l or "redmi" in ua_l or "mi " in ua_l:
            info["device_brand"] = "Xiaomi"
        elif "oppo" in ua_l:
            info["device_brand"] = "Oppo"
        elif "vivo" in ua_l:
            info["device_brand"] = "Vivo"
        elif "realme" in ua_l:
            info["device_brand"] = "Realme"
        elif "infinix" in ua_l:
            info["device_brand"] = "Infinix"
        elif "tecno" in ua_l:
            info["device_brand"] = "Tecno"
        elif "motorola" in ua_l or "moto g" in ua_l:
            info["device_brand"] = "Motorola"

        # محاولة بسيطة لاستخراج موديل (مثلاً SM-A146P)
        m = re.search(r"(sm-[a-z0-9]+)", ua_l)
        if m:
            info["device_model"] = m.group(1).upper()
    else:
        # Desktop / Laptop
        if "windows" in ua_l:
            info["device_type"] = "Desktop"
            info["os_name"] = "Windows"
        elif "macintosh" in ua_l or "mac os" in ua_l:
            info["device_type"] = "Desktop"
            info["os_name"] = "macOS"
            info["device_brand"] = "Apple"
        elif "linux" in ua_l:
            info["device_type"] = "Desktop"
            info["os_name"] = "Linux"

    # استخراج نسخة النظام (بسيطة جداً)
    if info["os_name"] == "Android":
        m = re.search(r"android\s+([\d\.]+)", ua_l)
        if m:
            info["os_version"] = m.group(1)
    elif info["os_name"] in ("iOS", "iPadOS"):
        m = re.search(r"os\s+([\d\_]+)", ua_l)
        if m:
            info["os_version"] = m.group(1).replace("_", ".")

    # المتصفح
    browser_name = None
    if "edg" in ua_l:
        browser_name = "Edge"
    elif "opr" in ua_l or "opera" in ua_l:
        browser_name = "Opera"
    elif "chrome" in ua_l and "safari" in ua_l:
        browser_name = "Chrome"
    elif "safari" in ua_l and "chrome" not in ua_l:
        browser_name = "Safari"
    elif "firefox" in ua_l:
        browser_name = "Firefox"

    info["browser_name"] = browser_name

    if browser_name:
        # محاولة جلب النسخة
        pattern_map = {
            "Chrome": r"chrome/([\d\.]+)",
            "Safari": r"version/([\d\.]+)",
            "Firefox": r"firefox/([\d\.]+)",
            "Edge": r"edg/([\d\.]+)",
            "Opera": r"(?:opr|opera)/([\d\.]+)",
        }
        pat = pattern_map.get(browser_name)
        if pat:
            m = re.search(pat, ua_l)
            if m:
                info["browser_version"] = m.group(1)

    return info


# -------- منطق التتبع الداخلي --------
def upsert_device(
    cur,
    device_id: str,
    now_ts: int,
    traffic_source: Optional[str],
    user_agent: Optional[str],
):
    # حاول تجيب الجهاز
    cur.execute("SELECT id, is_whatsapp FROM devices WHERE device_id = ?", (device_id,))
    row = cur.fetchone()

    is_whatsapp = 1 if (traffic_source == "whatsapp") else 0
    ua_info = parse_user_agent(user_agent)

    if row is None:
        # جهاز جديد
        cur.execute(
            """
            INSERT INTO devices (
                device_id,
                first_seen,
                last_seen,
                is_whatsapp,
                device_type,
                device_brand,
                device_model,
                os_name,
                os_version,
                browser_name,
                browser_version
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                device_id,
                now_ts,
                now_ts,
                is_whatsapp,
                ua_info["device_type"],
                ua_info["device_brand"],
                ua_info["device_model"],
                ua_info["os_name"],
                ua_info["os_version"],
                ua_info["browser_name"],
                ua_info["browser_version"],
            ),
        )
    else:
        # تحديث جهاز موجود
        current_is_whatsapp = row[1] or 0
        new_is_whatsapp = 1 if (current_is_whatsapp == 1 or is_whatsapp == 1) else 0
        cur.execute(
            """
            UPDATE devices
            SET last_seen = ?,
                is_whatsapp = ?,
                device_type = ?,
                device_brand = ?,
                device_model = ?,
                os_name = ?,
                os_version = ?,
                browser_name = ?,
                browser_version = ?
            WHERE device_id = ?
            """,
            (
                now_ts,
                new_is_whatsapp,
                ua_info["device_type"],
                ua_info["device_brand"],
                ua_info["device_model"],
                ua_info["os_name"],
                ua_info["os_version"],
                ua_info["browser_name"],
                ua_info["browser_version"],
                device_id,
            ),
        )


def upsert_session(
    cur,
    session_id: str,
    device_id: str,
    now_ts: int,
    traffic_source: Optional[str],
    utm_source: Optional[str],
    utm_medium: Optional[str],
    utm_campaign: Optional[str],
    utm_content: Optional[str],
    referrer: Optional[str],
    user_agent: Optional[str],
):
    cur.execute("SELECT id FROM sessions WHERE session_id = ?", (session_id,))
    row = cur.fetchone()

    if row is None:
        # جلسة جديدة
        cur.execute(
            """
            INSERT INTO sessions (
                session_id, device_id,
                first_seen, last_seen,
                traffic_source,
                utm_source, utm_medium, utm_campaign, utm_content,
                referrer_first, user_agent_first
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                session_id,
                device_id,
                now_ts,
                now_ts,
                traffic_source,
                utm_source,
                utm_medium,
                utm_campaign,
                utm_content,
                referrer,
                user_agent,
            ),
        )
    else:
        # تحديث جلسة موجودة (فقط last_seen)
        cur.execute(
            """
            UPDATE sessions
            SET last_seen = ?
            WHERE session_id = ?
            """,
            (now_ts, session_id),
        )


# -------- Endpoint: استقبال الأحداث من شوبفاي --------
@app.post("/track")
def track_event(payload: EventIn):
    now_ts = int(time.time())
    conn = get_conn()
    cur = conn.cursor()

    try:
        # 1) تحديث / إضافة الجهاز (مع user_agent)
        upsert_device(
            cur,
            payload.device_id,
            now_ts,
            payload.traffic_source,
            payload.user_agent,
        )

        # 2) تحديث / إضافة الجلسة
        upsert_session(
            cur,
            payload.session_id,
            payload.device_id,
            now_ts,
            payload.traffic_source,
            payload.utm_source,
            payload.utm_medium,
            payload.utm_campaign,
            payload.utm_content,
            payload.referrer,
            payload.user_agent,
        )

        # 3) تخزين الحدث نفسه
        meta_json = json.dumps(payload.meta or {}, ensure_ascii=False)

        cur.execute(
            """
            INSERT INTO events (
                event, session_id, device_id,
                url, referrer, user_agent,
                traffic_source,
                utm_source, utm_medium, utm_campaign, utm_content,
                created_at, meta,
                geo_country, geo_city,
                session_pages, session_duration_ms,
                template_name
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload.event,
                payload.session_id,
                payload.device_id,
                payload.url,
                payload.referrer,
                payload.user_agent,
                payload.traffic_source,
                payload.utm_source,
                payload.utm_medium,
                payload.utm_campaign,
                payload.utm_content,
                now_ts,
                meta_json,
                payload.geo_country,
                payload.geo_city,
                payload.session_pages,
                payload.session_duration_ms,
                payload.template_name,
            ),
        )

        conn.commit()
        return {"status": "ok"}

    except Exception as e:
        conn.rollback()
        return {"status": "error", "detail": str(e)}

    finally:
        conn.close()


# -------- Endpoint: تقرير عام --------
@app.get("/stats/overview")
def stats_overview():
    conn = get_conn()
    cur = conn.cursor()

    # total_events
    cur.execute("SELECT COUNT(*) FROM events")
    total_events = cur.fetchone()[0] or 0

    # total_sessions
    cur.execute("SELECT COUNT(DISTINCT session_id) FROM events")
    total_sessions = cur.fetchone()[0] or 0

    # total_devices
    cur.execute("SELECT COUNT(DISTINCT device_id) FROM events")
    total_devices = cur.fetchone()[0] or 0

    # by_source
    cur.execute(
        """
        SELECT traffic_source, COUNT(*)
        FROM events
        WHERE traffic_source IS NOT NULL
        GROUP BY traffic_source
        """
    )
    rows = cur.fetchall()
    by_source = [
        {"traffic_source": r[0], "count": r[1]} for r in rows if r[0] is not None
    ]

    conn.close()

    return {
        "total_events": total_events,
        "total_sessions": total_sessions,
        "total_devices": total_devices,
        "by_source": by_source,
    }


# -------- Endpoint: إحصائيات واتساب --------
@app.get("/stats/whatsapp")
def stats_whatsapp():
    conn = get_conn()
    cur = conn.cursor()

    # عدد الأجهزة القادمة من واتساب (is_whatsapp = 1)
    cur.execute(
        """
        SELECT COUNT(DISTINCT device_id)
        FROM devices
        WHERE is_whatsapp = 1
        """
    )
    total_whatsapp_devices = cur.fetchone()[0] or 0

    # عدد الأجهزة من واتساب بدون شراء (لا يوجد لها event = 'purchase')
    cur.execute(
        """
        SELECT COUNT(DISTINCT d.device_id)
        FROM devices d
        WHERE d.is_whatsapp = 1
        AND d.device_id NOT IN (
            SELECT DISTINCT device_id FROM events WHERE event = 'purchase'
        )
        """
    )
    whatsapp_no_purchase_devices = cur.fetchone()[0] or 0

    conn.close()

    return {
        "total_whatsapp_devices": total_whatsapp_devices,
        "whatsapp_no_purchase_devices": whatsapp_no_purchase_devices,
    }


# -------- Endpoint: إحصائيات الأجهزة والشراء --------
@app.get("/stats/devices")
def stats_devices():
    conn = get_conn()
    cur = conn.cursor()

    # إجمالي الأجهزة التي ظهر لها أي حدث
    cur.execute("SELECT COUNT(DISTINCT device_id) FROM events")
    total_devices = cur.fetchone()[0] or 0

    # الأجهزة التي قامت بالشراء (event = 'purchase')
    cur.execute(
        """
        SELECT COUNT(DISTINCT device_id)
        FROM events
        WHERE event = 'purchase'
        """
    )
    purchased_devices = cur.fetchone()[0] or 0

    no_purchase_devices = max(total_devices - purchased_devices, 0)

    conn.close()

    return {
        "total_devices": total_devices,
        "purchased_devices": purchased_devices,
        "no_purchase_devices": no_purchase_devices,
    }


# -------- Endpoint: Funnel (Overall + By Source + By Product) --------
@app.get("/stats/funnel")
def stats_funnel():
    conn = get_conn()
    cur = conn.cursor()

    FUNNEL_STEPS = [
        "product_view",
        "add_to_cart",
        "cart_view",
        "begin_checkout",
        "purchase",
    ]

    # نجيب كل الأحداث المتعلقة بالفانل
    placeholders = ",".join(["?"] * len(FUNNEL_STEPS))
    cur.execute(
        f"""
        SELECT event, session_id, traffic_source, meta
        FROM events
        WHERE event IN ({placeholders})
        """,
        FUNNEL_STEPS,
    )
    rows = cur.fetchall()
    conn.close()

    # overall: step → set(session_id)
    overall_sets = {step: set() for step in FUNNEL_STEPS}

    # by_source: src → step → set(session_id)
    source_sets = {}

    # by_product: (product_id, title) → step → set(session_id)
    product_sets = {}

    for event, session_id, traffic_source, meta_json in rows:
        if not session_id:
            continue

        # overall
        overall_sets[event].add(session_id)

        # by_source
        src = traffic_source or "unknown"
        if src not in source_sets:
            source_sets[src] = {step: set() for step in FUNNEL_STEPS}
        source_sets[src][event].add(session_id)

        # by_product (لو في product_id داخل meta)
        try:
            meta = json.loads(meta_json or "{}")
        except Exception:
            meta = {}

        product_id = meta.get("product_id")
        product_title = meta.get("product_title") or meta.get("title")

        if product_id is not None:
            key = (str(product_id), str(product_title) if product_title else None)
            if key not in product_sets:
                product_sets[key] = {step: set() for step in FUNNEL_STEPS}
            product_sets[key][event].add(session_id)

    # helper لتحويل sets إلى أرقام (counts)
    def convert_nested(obj):
        if isinstance(obj, set):
            return len(obj)
        if isinstance(obj, dict):
            return {k: convert_nested(v) for k, v in obj.items()}
        return obj

    overall = convert_nested(overall_sets)
    by_source = {src: convert_nested(steps) for src, steps in source_sets.items()}
    by_product = {
        f"{pid} | {title if title else 'No Title'}": convert_nested(steps)
        for (pid, title), steps in product_sets.items()
    }

    return {
        "overall": overall,
        "by_source": by_source,
        "by_product": by_product,
    }


# -------- Endpoint: ملخص أنواع الأجهزة وأنظمتها --------
@app.get("/stats/device-types")
def stats_device_types():
    """
    يرجع توزيع الأجهزة حسب:
    - نوع الجهاز (device_type)
    - الماركة (device_brand)
    - النظام (os_name)
    - المتصفح (browser_name)
    يعتمد على جدول devices حيث يتم تحديث المعلومات من user_agent.
    """
    conn = get_conn()
    cur = conn.cursor()

    def agg(query: str):
        cur.execute(query)
        rows = cur.fetchall()
        return [
            {
                "value": r[0] if r[0] not in (None, "") else "unknown",
                "count": r[1],
            }
            for r in rows
        ]

    by_type = agg(
        """
        SELECT device_type, COUNT(DISTINCT device_id)
        FROM devices
        GROUP BY device_type
        """
    )

    by_brand = agg(
        """
        SELECT device_brand, COUNT(DISTINCT device_id)
        FROM devices
        GROUP BY device_brand
        """
    )

    by_os = agg(
        """
        SELECT os_name, COUNT(DISTINCT device_id)
        FROM devices
        GROUP BY os_name
        """
    )

    by_browser = agg(
        """
        SELECT browser_name, COUNT(DISTINCT device_id)
        FROM devices
        GROUP BY browser_name
        """
    )

    conn.close()

    return {
        "by_device_type": by_type,
        "by_brand": by_brand,
        "by_os": by_os,
        "by_browser": by_browser,
    }


# -------- Endpoint: إحصائيات Realtime (جلسات/أجهزة نشطة آخر X دقيقة) --------
@app.get("/stats/realtime")
def stats_realtime(window_minutes: int = 5):
    """
    يعطي نظرة لحظية:
    - عدد الجلسات النشطة في آخر window_minutes دقيقة
    - عدد الأجهزة التي شوهدت في آخر window_minutes دقيقة
    - عدد الأحداث في آخر window_minutes دقيقة
    """
    now_ts = int(time.time())
    threshold = now_ts - window_minutes * 60

    conn = get_conn()
    cur = conn.cursor()

    # جلسات نشطة
    cur.execute(
        """
        SELECT COUNT(DISTINCT session_id)
        FROM sessions
        WHERE last_seen >= ?
        """,
        (threshold,),
    )
    active_sessions = cur.fetchone()[0] or 0

    # أجهزة نشطة
    cur.execute(
        """
        SELECT COUNT(DISTINCT device_id)
        FROM devices
        WHERE last_seen >= ?
        """,
        (threshold,),
    )
    active_devices = cur.fetchone()[0] or 0

    # أحداث حديثة
    cur.execute(
        """
        SELECT COUNT(*)
        FROM events
        WHERE created_at >= ?
        """,
        (threshold,),
    )
    recent_events = cur.fetchone()[0] or 0

    conn.close()

    return {
        "window_minutes": window_minutes,
        "active_sessions": active_sessions,
        "active_devices": active_devices,
        "recent_events": recent_events,
    }


# -------- Endpoint: عدد الأحداث لكل يوم (لآخر 30 يوم) --------
@app.get("/stats/events-daily")
def stats_events_daily(limit_days: int = 30):
    """
    يرجع عدد الأحداث لكل يوم (للاستخدام في الرسوم البيانية).
    """
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT strftime('%Y-%m-%d', datetime(created_at, 'unixepoch')) AS day,
               COUNT(*) as cnt
        FROM events
        GROUP BY day
        ORDER BY day DESC
        LIMIT ?
        """,
        (limit_days,),
    )
    rows = cur.fetchall()
    conn.close()

    return [
        {"day": r[0], "count": r[1]}
        for r in rows
        if r[0] is not None
    ]


# -------- Endpoint: إحصائيات جغرافية بسيطة --------
@app.get("/stats/geo")
def stats_geo():
    """
    يرجع توزيع الجلسات حسب الدولة والمدينة (حسب ما متوفر).
    """
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT geo_country, COUNT(DISTINCT session_id)
        FROM events
        WHERE geo_country IS NOT NULL AND geo_country <> ''
        GROUP BY geo_country
        ORDER BY COUNT(DISTINCT session_id) DESC
        """
    )
    by_country = [
        {"country": row[0], "sessions": row[1]}
        for row in cur.fetchall()
    ]

    cur.execute(
        """
        SELECT geo_city, COUNT(DISTINCT session_id)
        FROM events
        WHERE geo_city IS NOT NULL AND geo_city <> ''
        GROUP BY geo_city
        ORDER BY COUNT(DISTINCT session_id) DESC
        """
    )
    by_city = [
        {"city": row[0], "sessions": row[1]}
        for row in cur.fetchall()
    ]

    conn.close()
    return {"by_country": by_country, "by_city": by_city}
