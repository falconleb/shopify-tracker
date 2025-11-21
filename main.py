from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, Any
import sqlite3
import time
import json

DB_PATH = "events.db"

app = FastAPI(title="Shopify Tracking Server (Captain Version)")

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

    # جدول الأجهزة
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

    # جدول الأحداث
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
            meta TEXT
        )
        """
    )

    conn.commit()
    conn.close()


# استدعاء إنشاء الجداول عند تشغيل السيرفر
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

    meta: Optional[Dict[str, Any]] = None  # أي بيانات إضافية (product_id, value...)


# -------- منطق التتبع الداخلي --------
def upsert_device(cur, device_id: str, now_ts: int, traffic_source: Optional[str]):
    # حاول تجيب الجهاز
    cur.execute("SELECT id, is_whatsapp FROM devices WHERE device_id = ?", (device_id,))
    row = cur.fetchone()

    is_whatsapp = 1 if (traffic_source == "whatsapp") else 0

    if row is None:
        # جهاز جديد
        cur.execute(
            """
            INSERT INTO devices (device_id, first_seen, last_seen, is_whatsapp)
            VALUES (?, ?, ?, ?)
            """,
            (device_id, now_ts, now_ts, is_whatsapp),
        )
    else:
        # تحديث جهاز موجود
        current_is_whatsapp = row[1] or 0
        new_is_whatsapp = 1 if (current_is_whatsapp == 1 or is_whatsapp == 1) else 0
        cur.execute(
            """
            UPDATE devices
            SET last_seen = ?, is_whatsapp = ?
            WHERE device_id = ?
            """,
            (now_ts, new_is_whatsapp, device_id),
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
        # 1) تحديث / إضافة الجهاز
        upsert_device(cur, payload.device_id, now_ts, payload.traffic_source)

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
                created_at, meta
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    """
    يرجع 3 تقارير فانل:
    - overall: عدد الجلسات التي وصلت لكل مرحلة
    - by_source: فانل لكل traffic_source
    - by_product: فانل لكل product_id (من داخل meta.product_id)
    """
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
