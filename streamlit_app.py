import os
import signal
import subprocess
import time
from pathlib import Path
from typing import Optional

import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime, timezone

APP_DIR = Path(__file__).resolve().parent
PIDS_DIR = APP_DIR / ".pids"
PIDS_DIR.mkdir(exist_ok=True)
LOG_DIR = APP_DIR


def pid_path(name: str) -> Path:
    return PIDS_DIR / f"{name}.pid"


def read_pid(name: str) -> Optional[int]:
    p = pid_path(name)
    if p.exists():
        try:
            return int(p.read_text().strip())
        except Exception:
            return None
    return None


def is_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def write_env(entries: dict):
    env_file = APP_DIR / ".env"
    lines = []
    if env_file.exists():
        lines = env_file.read_text().splitlines()
    keys = set(entries.keys())
    new_lines = []
    for l in lines:
        if '=' in l:
            k = l.split('=', 1)[0]
            if k in keys:
                new_lines.append(f"{k}={entries[k]}")
                keys.remove(k)
            else:
                new_lines.append(l)
        else:
            new_lines.append(l)
    for k in keys:
        new_lines.append(f"{k}={entries[k]}")
    env_file.write_text('\n'.join(new_lines) + '\n')


def start_process(name: str, extra_env: dict = None):
    if read_pid(name) and is_running(read_pid(name)):
        return False, f"{name} already running (pid={read_pid(name)})"

    script = APP_DIR / f"{name}.py"
    if not script.exists():
        return False, f"{script} not found"

    env = os.environ.copy()
    if extra_env:
        env.update({k: str(v) for k, v in extra_env.items()})

    # Open log file in append mode with line buffering for real-time updates
    logf = open(LOG_DIR / f"{name}.log", "a", buffering=1)  # Line buffering
    # Start process detached from this Streamlit server
    proc = subprocess.Popen(
        ["python3", str(script)],
        stdout=logf,
        stderr=subprocess.STDOUT,  # Redirect stderr to stdout
        env=env,
        start_new_session=True,
    )
    pid = proc.pid
    pid_path(name).write_text(str(pid))
    # Give it a moment to start
    time.sleep(0.5)
    return True, f"✅ Started {name} (pid={pid})"


def stop_process(name: str):
    pid = read_pid(name)
    if not pid:
        return False, f"{name} not running"
    try:
        os.kill(pid, signal.SIGINT)
    except Exception:
        pass
    # wait briefly for graceful shutdown
    for _ in range(30):
        if not is_running(pid):
            break
        time.sleep(0.1)
    if is_running(pid):
        try:
            os.kill(pid, signal.SIGTERM)
        except Exception:
            pass
    # final check
    if is_running(pid):
        return False, f"failed to stop {name} (pid={pid})"
    pid_path(name).unlink(missing_ok=True)
    return True, f"stopped {name}"


def tail_log(name: str, n: int = 200) -> str:
    p = LOG_DIR / f"{name}.log"
    if not p.exists():
        return "(no log yet)"
    try:
        with p.open('rb') as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            block = 1024
            data = b''
            while size > 0 and len(data) < block * n:
                size = max(0, size - block)
                f.seek(size)
                data = f.read() + data
            text = data.decode(errors='replace')
            return '\n'.join(text.splitlines()[-n:])
    except Exception as e:
        return f"(error reading log: {e})"


st.set_page_config(page_title="Traffic Stream Controller", layout="wide")
st.markdown("""
<style>
body {background-color: #0b1020}
.title {color: #fff; font-size:32px; font-weight:700}
.subtitle {color: #9aa3b2; margin-top: -10px}
.chip {display:inline-block; padding:6px 10px; border-radius:12px; color:#fff; font-weight:600}
.chip.green{background:#1f9d55}
.chip.red{background:#e5534b}
.chip.gray{background:#586175}
.kpi {text-align:center}
.logbox {background:#0f1724; color:#cbd5e1; border-radius:6px; padding:8px; font-family:monospace}
.small {font-size:12px; color:#9aa3b2}
</style>
<div class="title">Traffic Stream Controller</div>
<div class="subtitle">Real-time demo: control producer & consumer, view KPI strip and AI decisions</div>
""", unsafe_allow_html=True)

# GEMINI key status badge
gemini_key = None
try:
    from dotenv import dotenv_values
    cfg = dotenv_values(APP_DIR / '.env')
    gemini_key = cfg.get('GEMINI_API_KEY') if cfg else None
except Exception:
    gemini_key = os.getenv('GEMINI_API_KEY')

if gemini_key:
    st.markdown(f"<div style='margin-top:8px'><span class='chip green'>Gemini key loaded</span></div>", unsafe_allow_html=True)
else:
    st.markdown(f"<div style='margin-top:8px'><span class='chip red'>Gemini key missing</span> <span class='small'>set GEMINI_API_KEY in .env</span></div>", unsafe_allow_html=True)

# Auto-refresh mechanism - use Streamlit's built-in auto-refresh if available
try:
    # Try to use st.rerun with a timer (Streamlit 1.28+)
    if 'auto_refresh_count' not in st.session_state:
        st.session_state.auto_refresh_count = 0
    st.session_state.auto_refresh_count += 1
    
    # Auto-refresh every 3 seconds (rerun after 3 page loads)
    if st.session_state.auto_refresh_count % 3 == 0:
        st.rerun()
except:
    pass

# Manual refresh button (always visible)
col1, col2 = st.columns([1, 4])
with col1:
    if st.button("🔄 Refresh Now", use_container_width=True):
        st.session_state.auto_refresh_count = 0
        st.rerun()
with col2:
    st.caption("Auto-refreshing every ~3 seconds...")


def ensure_db_schema(conn: sqlite3.Connection):
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS traffic_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kafka_partition INTEGER,
                kafka_offset INTEGER,
                vehicle_id TEXT,
                speed_kmh REAL,
                lat REAL,
                lon REAL,
                event_timestamp_utc TEXT,
                consumed_at_utc TEXT,
                classification TEXT,
                gemini_raw_response TEXT,
                gemini_status TEXT,
                gemini_error TEXT
            );
            """
        )
        conn.commit()
    except Exception:
        pass


def get_db_connection():
    db_path = APP_DIR / "traffic_events.db"
    # Always open the file-backed DB (it will be created if missing)
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    ensure_db_schema(conn)
    return conn


def compute_kpis(conn: sqlite3.Connection):
    cur = conn.cursor()
    total = 0
    high = 0
    medium = 0
    low = 0
    ai_calls = 0
    events_per_sec = 0.0

    try:
        cur.execute("SELECT COUNT(*) FROM traffic_events")
        total = cur.fetchone()[0] or 0

        # Count classifications; map known labels to LOW/MEDIUM/HIGH
        cur.execute("SELECT classification, COUNT(*) FROM traffic_events GROUP BY classification")
        rows = cur.fetchall()
        counts = {r[0].upper() if r[0] else None: r[1] for r in rows}

        # mapping heuristics
        # treat 'CONGESTED' and 'ACCIDENT' as HIGH
        high = counts.get('CONGESTED', 0) + counts.get('ACCIDENT', 0)
        # treat 'ANOMALY' as MEDIUM
        medium = counts.get('ANOMALY', 0)
        # treat 'NORMAL' as LOW
        low = counts.get('NORMAL', 0)

        # AI calls: count rows where gemini_status == 'success' or gemini_raw_response is not null
        cur.execute("SELECT COUNT(*) FROM traffic_events WHERE gemini_status='success' OR gemini_raw_response IS NOT NULL")
        ai_calls = cur.fetchone()[0] or 0

        # Events/sec: estimate using last 100 rows
        df = pd.read_sql_query("SELECT consumed_at_utc FROM traffic_events ORDER BY id DESC LIMIT 100", conn)
        if not df.empty and df['consumed_at_utc'].notnull().any():
            df['consumed_at_utc'] = pd.to_datetime(df['consumed_at_utc'], errors='coerce')
            df = df.dropna()
            if len(df) >= 2:
                span = (df['consumed_at_utc'].max() - df['consumed_at_utc'].min()).total_seconds()
                if span > 0:
                    events_per_sec = len(df) / span
    except Exception:
        # DB might be empty or not created yet
        pass

    return {
        'total': int(total),
        'high': int(high),
        'medium': int(medium),
        'low': int(low),
        'ai_calls': int(ai_calls),
        'events_per_sec': float(events_per_sec),
    }


def get_recent_timeseries(conn: sqlite3.Connection, limit: int = 200):
    try:
        df = pd.read_sql_query(
            "SELECT event_timestamp_utc AS timestamp, classification FROM traffic_events ORDER BY id DESC LIMIT {}".format(limit),
            conn,
            parse_dates=['timestamp'],
        )
        if df.empty:
            return pd.DataFrame()
        # Map classifications to levels
        def map_level(c):
            if not c:
                return None
            c = c.upper()
            if c in ('CONGESTED', 'ACCIDENT'):
                return 2
            if c == 'ANOMALY':
                return 1
            if c == 'NORMAL':
                return 0
            # fallback: try matching strings
            if 'congest' in c.lower():
                return 2
            if 'accid' in c.lower():
                return 2
            if 'anom' in c.lower():
                return 1
            if 'norm' in c.lower() or 'normal' in c.lower():
                return 0
            return None

        df['level'] = df['classification'].map(map_level)
        df = df.dropna(subset=['timestamp'])
        df = df.sort_values('timestamp')
        return df
    except Exception:
        return pd.DataFrame()


# KPI strip and visualization will be rendered below

cols = st.columns([1,1])
with cols[0]:
    st.markdown("""
    <div style='padding:8px; border-radius:8px; background:#071028'>
      <h3 style='color:#fff; margin:0;'>Producer</h3>
    </div>
    """, unsafe_allow_html=True)
    current_pid = read_pid('producer')
    running = bool(current_pid and is_running(current_pid))
    status_chip = "<span class='chip green'>Running</span>" if running else "<span class='chip gray'>Stopped</span>"
    st.markdown(f"PID: {current_pid or '—'} &nbsp; {status_chip}", unsafe_allow_html=True)

    eps = st.number_input("Events per second", min_value=1, max_value=1000, value=int(os.getenv('EVENTS_PER_SECOND', '10')))

    if st.button("Start Producer"):
        write_env({'EVENTS_PER_SECOND': str(eps)})
        ok, msg = start_process('producer', extra_env={'EVENTS_PER_SECOND': str(eps)})
        if ok:
            st.success(msg)
        else:
            st.error(msg)

    if st.button("Stop Producer"):
        ok, msg = stop_process('producer')
        if ok:
            st.success(msg)
        else:
            st.error(msg)

    st.subheader("📊 Producer Status & Logs")
    producer_log = tail_log('producer', 50)
    if producer_log and producer_log != "(no log yet)":
        st.markdown(f"<div class='logbox' style='max-height:300px; overflow-y:auto;'>{producer_log.replace('\n','<br>')}</div>", unsafe_allow_html=True)
    else:
        st.info("No producer logs yet. Start the producer to see output.")

with cols[1]:
    st.markdown("""
    <div style='padding:8px; border-radius:8px; background:#071028'>
      <h3 style='color:#fff; margin:0;'>Consumer</h3>
    </div>
    """, unsafe_allow_html=True)
    current_pid = read_pid('consumer')
    running = bool(current_pid and is_running(current_pid))
    status_chip = "<span class='chip green'>Running</span>" if running else "<span class='chip gray'>Stopped</span>"
    st.markdown(f"PID: {current_pid or '—'} &nbsp; {status_chip}", unsafe_allow_html=True)

    dry_run = st.checkbox("Dry run (skip Gemini API)", value=(os.getenv('DRY_RUN', '0') == '1'))
    use_real_gemini = st.checkbox("Use real Gemini API (⚠️ costs/quota)", value=(os.getenv('USE_REAL_GEMINI', '0') == '1'))
    csv_enabled = st.checkbox("CSV logging enabled", value=(os.getenv('CSV_LOG_ENABLED', '1') != '0'))
    
    if not use_real_gemini:
        st.info("ℹ️ Using mock/hardcoded responses (no API costs). Enable checkbox above to use real Gemini API.")

    if st.button("Start Consumer"):
        write_env({
            'DRY_RUN': '1' if dry_run else '0',
            'USE_REAL_GEMINI': '1' if use_real_gemini else '0',
            'CSV_LOG_ENABLED': '1' if csv_enabled else '0'
        })
        ok, msg = start_process('consumer', extra_env={
            'DRY_RUN': '1' if dry_run else '0',
            'USE_REAL_GEMINI': '1' if use_real_gemini else '0',
            'CSV_LOG_ENABLED': '1' if csv_enabled else '0'
        })
        if ok:
            st.success(msg)
        else:
            st.error(msg)

    if st.button("Stop Consumer"):
        ok, msg = stop_process('consumer')
        if ok:
            st.success(msg)
        else:
            st.error(msg)

    st.subheader("📊 Consumer Status & Logs")
    consumer_log = tail_log('consumer', 50)
    if consumer_log and consumer_log != "(no log yet)":
        # color important messages
        logtxt = consumer_log
        logtxt = logtxt.replace('[INFO]', "<span style='color:#4ade80;'>[INFO]</span>")
        logtxt = logtxt.replace('[ERROR]', "<span style='color:#f87171; font-weight:700'>[ERROR]</span>")
        logtxt = logtxt.replace('[FATAL]', "<span style='color:#ef4444; font-weight:700'>[FATAL]</span>")
        logtxt = logtxt.replace('[CLASSIFIED]', "<span style='color:#60a5fa;'>[CLASSIFIED]</span>")
        logtxt = logtxt.replace('[CONSUMED]', "<span style='color:#a78bfa;'>[CONSUMED]</span>")
        st.markdown(f"<div class='logbox' style='max-height:300px; overflow-y:auto;'>{logtxt.replace('\n','<br>')}</div>", unsafe_allow_html=True)
    else:
        st.info("No consumer logs yet. Start the consumer to see output.")

st.markdown("---")

# Real-time status summary
st.subheader("🔄 System Status")
status_cols = st.columns(4)

with status_cols[0]:
    producer_pid = read_pid('producer')
    producer_running = bool(producer_pid and is_running(producer_pid))
    st.metric("Producer", "🟢 Running" if producer_running else "🔴 Stopped", 
              f"PID: {producer_pid}" if producer_pid else "No PID")

with status_cols[1]:
    consumer_pid = read_pid('consumer')
    consumer_running = bool(consumer_pid and is_running(consumer_pid))
    st.metric("Consumer", "🟢 Running" if consumer_running else "🔴 Stopped",
              f"PID: {consumer_pid}" if consumer_pid else "No PID")

with status_cols[2]:
    # Check Kafka connection
    try:
        from kafka import KafkaProducer
        test_prod = KafkaProducer(bootstrap_servers='localhost:9092', request_timeout_ms=2000)
        test_prod.close()
        kafka_status = "🟢 Connected"
    except:
        kafka_status = "🔴 Disconnected"
    st.metric("Kafka", kafka_status, "localhost:9092")

with status_cols[3]:
    # Check database
    try:
        test_conn = get_db_connection()
        cur = test_conn.cursor()
        cur.execute("SELECT COUNT(*) FROM traffic_events")
        event_count = cur.fetchone()[0]
        test_conn.close()
        db_status = f"🟢 {event_count} events"
    except:
        db_status = "🔴 Error"
    st.metric("Database", db_status, "traffic_events.db")

# --------------------
# Layer 1: KPI strip
# --------------------
conn = get_db_connection()
kpis = compute_kpis(conn)

kpi_cols = st.columns([1,1,1,1,1])
with kpi_cols[0]:
    st.metric(label="Events/sec", value=f"{kpis['events_per_sec']:.2f}")
with kpi_cols[1]:
    st.metric(label="🔥 High Congestion", value=kpis['high'])
with kpi_cols[2]:
    st.metric(label="🟡 Medium", value=kpis['medium'])
with kpi_cols[3]:
    st.metric(label="🟢 Low", value=kpis['low'])
with kpi_cols[4]:
    st.metric(label="🧠 AI Calls", value=kpis['ai_calls'])

# --------------------
# Layer 2: Live Classification Timeline
# --------------------
st.subheader("Live classification timeline")
ts_df = get_recent_timeseries(conn, limit=200)
if not ts_df.empty:
    chart_df = ts_df.set_index('timestamp')['level']
    st.line_chart(chart_df)
else:
    st.info("No recent events to chart.")

# --------------------
# Layer 3: Explain the AI panel
# --------------------
st.subheader("Latest AI Decision")
try:
    latest = pd.read_sql_query("SELECT * FROM traffic_events ORDER BY id DESC LIMIT 1", conn)
    if latest.empty:
        st.write("No events yet.")
    else:
        # compute simple aggregates over last 30 events
        recent = pd.read_sql_query("SELECT speed_kmh, vehicle_id, classification, gemini_raw_response, event_timestamp_utc FROM traffic_events ORDER BY id DESC LIMIT 30", conn)
        vehicle_count = recent['vehicle_id'].nunique()
        average_speed = recent['speed_kmh'].mean()
        classification = latest.at[0, 'classification']
        gemini_resp = latest.at[0, 'gemini_raw_response'] if 'gemini_raw_response' in latest.columns else None

        st.json({
            'vehicle_count': int(vehicle_count),
            'average_speed': float(round(average_speed or 0.0, 2)),
            'classification': classification,
            'gemini_raw_response': (gemini_resp[:200] + '...') if gemini_resp and len(gemini_resp) > 200 else gemini_resp,
        })
except Exception as e:
    st.write("Could not load latest decision:", e)

conn.close()
