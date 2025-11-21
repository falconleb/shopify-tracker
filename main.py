from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import sqlite3
import time

DB_PATH = "events.db"

app = FastAPI(title="Shopify Tracking Server")

# ------- CORS -------
# الأفضل تحط دومين متجرك الحقيقي هون بدل * لما تستقر الأمور
origins = [
    "https://4pytkr-hy.myshopify.com",
    "https://www.4pytkr-hy.myshopify.com",
    "https://4pytkr-hy.myshopify.com/",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,      # ممكن مؤقتاً تخليها ["*"] بس للأمان الأفضل نحدد الدومين
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------- قاعدة البيانات --------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event TEXT,
            session_id TEXT,
            url TEXT,
            referrer TEXT,
            user_agent TEXT,
            traffic_source TEXT,
            utm_source TEXT,
            utm_medium TEXT,
            utm_campaign TEXT,
            utm_content TEXT,
            geo_country TEXT,
            geo_city TEXT,
            product_id TEXT,
            product_title TEXT,
            cart_token TEXT,
            items_count INTEGER,
            ts INTEGER
        )
        """
    )
    conn.commit()
    conn.close()

@app.on_event("startup")
def startup_event():
    init_db()

# -------- نموذج البيانات --------
class TrackEvent(BaseModel):
    event: str
    session_id: Optional[str] = None
    url: Optional[str] = None
    referrer: Optional[str] = None
    user_agent: Optional[str] = None
    traffic_source: Optional[str] = None
    utm_source: Optional[str] = None
    utm_medium: Optional[str] = None
    utm_campaign: Optional[str] = None
    utm_content: Optional[str] = None
    geo_country: Optional[str] = None
    geo_city: Optional[str] = None
    product_id: Optional[str] = None
    product_title: Optional[str] = None
    cart_token: Optional[str] = None
    items_count: Optional[int] = None
    timestamp: Optional[int] = None

# -------- /track endpoint --------
@app.post("/track")
def track(event: TrackEvent):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    ts = event.timestamp or int(time.time() * 1000)

    cur.execute(
        """
        INSERT INTO events (
            event, session_id, url, referrer, user_agent,
            traffic_source, utm_source, utm_medium, utm_campaign, utm_content,
            geo_country, geo_city, product_id, product_title,
            cart_token, items_count, ts
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event.event,
            event.session_id,
            event.url,
            event.referrer,
            event.user_agent,
            event.traffic_source,
            event.utm_source,
            event.utm_medium,
            event.utm_campaign,
            event.utm_content,
            event.geo_country,
            event.geo_city,
            event.product_id,
            event.product_title,
            event.cart_token,
            event.items_count,
            ts,
        ),
    )

    conn.commit()
    conn.close()

    return {"status": "ok"}

# -------- تحليل بسيط (ملخّص عام) --------
@app.get("/analytics/overview")
def analytics_overview():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM events")
    total_events = cur.fetchone()[0]

    cur.execute("SELECT COUNT(DISTINCT session_id) FROM events WHERE session_id IS NOT NULL")
    total_sessions = cur.fetchone()[0]

    cur.execute("""
        SELECT traffic_source, COUNT(*)
        FROM events
        WHERE traffic_source IS NOT NULL
        GROUP BY traffic_source
    """)
    by_source = [
        {"traffic_source": row[0], "count": row[1]}
        for row in cur.fetchall()
    ]

    conn.close()
    return {
        "total_events": total_events,
        "total_sessions": total_sessions,
        "by_source": by_source,
    }

# -------- تفاصيل جلسة معيّنة (يشمل المنتجات + البلد + المدينة + المصدر) --------
@app.get("/analytics/session/{session_id}")
def session_details(session_id: str):
    """
    يرجّع كل الأحداث المرتبطة بـ session_id واحد:
    - نوع الحدث (page_view, product_view, add_to_cart ... لاحقاً)
    - الرابط
    - المنتج (id + title لو موجود)
    - البلد / المدينة
    - مصدر الزيارة
    - الترتيب الزمني
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        """
        SELECT
            event,
            url,
            product_id,
            product_title,
            geo_country,
            geo_city,
            traffic_source,
            ts
        FROM events
        WHERE session_id = ?
        ORDER BY ts ASC
        """,
        (session_id,)
    )

    rows = cur.fetchall()
    conn.close()

    events = []
    for r in rows:
        events.append({
            "event": r[0],
            "url": r[1],
            "product_id": r[2],
            "product_title": r[3],
            "country": r[4],
            "city": r[5],
            "source": r[6],
            "timestamp": r[7],
        })

    return {
        "session_id": session_id,
        "events": events
    }
