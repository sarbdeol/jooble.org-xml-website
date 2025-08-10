import os
import datetime
import json
import sqlite3
import xml.etree.ElementTree as ET
from typing import Optional, Dict, Any

import requests
from flask import (
    Flask, request, render_template, redirect, url_for, send_file, flash
)
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.base import JobLookupError

# --- Auth ---
from flask_login import (
    LoginManager, UserMixin, login_user, login_required,
    logout_user, current_user
)
from werkzeug.security import generate_password_hash, check_password_hash

# =============================================================================
# App + Config
# =============================================================================
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "supersecretkey")

DB_FILE = "logs.db"
FILTERED_FILE = "static/filtered_feed.xml"
MAX_INTERVAL_HOURS = 8760  # 1 year
ALLOW_SIGNUP = os.getenv("ALLOW_SIGNUP", "1") == "1"  # set ALLOW_SIGNUP=0 to disable

CONFIG: Dict[str, Any] = {
    "feed_url": None,
    "cpc_threshold": 0.0,
    "interval": 1,
    "last_run": None,
    "next_run": None,       # human-readable
    "last_total": 0,        # filtered count from last run
    "stats": {}             # CPC distribution from last run
}

# =============================================================================
# Helpers (parsing + settings persistence)
# =============================================================================
def to_float(val, default=0.0):
    try:
        if val is None:
            return default
        if isinstance(val, (int, float)):
            return float(val)
        s = str(val).strip()
        return float(s) if s else default
    except Exception:
        return default

def to_int(val, default=1):
    try:
        if val is None:
            return default
        s = str(val).strip()
        return int(s) if s else default
    except Exception:
        return default

def init_db():
    os.makedirs("static", exist_ok=True)
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_time TEXT,
                jobs_total INTEGER,
                jobs_filtered INTEGER,
                cpc_stats TEXT
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                created_at TEXT
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                id INTEGER PRIMARY KEY CHECK (id=1),
                feed_url TEXT,
                cpc_threshold REAL,
                interval INTEGER
            )
            """
        )
        conn.commit()

def seed_admin():
    """Create an initial admin user if not present."""
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        try:
            c.execute(
                "INSERT INTO users (email, password_hash, created_at) VALUES (?, ?, ?)",
                (
                    "admin@example.com",
                    generate_password_hash("ChangeMe123!"),
                    datetime.datetime.now().isoformat(),
                ),
            )
            conn.commit()
            print("✅ Seeded admin: admin@example.com / ChangeMe123!")
        except sqlite3.IntegrityError:
            print("ℹ️ Admin already exists")

def load_settings_into_config():
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("SELECT feed_url, cpc_threshold, interval FROM settings WHERE id=1")
        row = c.fetchone()
    if row:
        CONFIG["feed_url"] = row[0]
        CONFIG["cpc_threshold"] = to_float(row[1], 0.0)
        CONFIG["interval"] = to_int(row[2], 1)

def save_settings_from_config():
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO settings (id, feed_url, cpc_threshold, interval)
            VALUES (1, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
              feed_url=excluded.feed_url,
              cpc_threshold=excluded.cpc_threshold,
              interval=excluded.interval
            """,
            (
                CONFIG.get("feed_url"),
                float(CONFIG.get("cpc_threshold") or 0.0),
                int(CONFIG.get("interval") or 1),
            ),
        )
        conn.commit()

# =============================================================================
# Scheduler
# =============================================================================
scheduler = BackgroundScheduler(
    daemon=True,
    job_defaults={"coalesce": True, "misfire_grace_time": 300}
)
scheduler.start()

def _update_next_run_label():
    """Set CONFIG['next_run'] from scheduler's next fire time."""
    job = scheduler.get_job("feed_job")
    if job and job.next_run_time:
        ts = job.next_run_time  # usually timezone-aware UTC
        CONFIG["next_run"] = ts.strftime("%Y-%m-%d %H:%M:%S %Z")
    else:
        CONFIG["next_run"] = None

def update_scheduler(interval_hours: int) -> bool:
    """Replace/define the interval job."""
    try:
        try:
            scheduler.remove_job("feed_job")
        except JobLookupError:
            pass

        scheduler.add_job(
            fetch_and_filter,  # defined below
            "interval",
            hours=interval_hours,
            id="feed_job",
            replace_existing=True,
        )

        _update_next_run_label()
        return True
    except Exception as e:
        print(f"❌ Error updating scheduler: {e}")
        return False

# =============================================================================
# Login manager
# =============================================================================
login_manager = LoginManager(app)
login_manager.login_view = "login"

class User(UserMixin):
    def __init__(self, id: str, email: str, password_hash: str):
        self.id = str(id)
        self.email = email
        self.password_hash = password_hash

@login_manager.user_loader
def load_user(user_id: str) -> Optional[User]:
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("SELECT id, email, password_hash FROM users WHERE id=?", (user_id,))
        row = c.fetchone()
        if row:
            return User(*row)
    return None

# =============================================================================
# Core processing
# =============================================================================
def fetch_and_filter() -> bool:
    """Fetch XML feed and filter by CPC threshold. Writes filtered XML + logs row."""
    if not CONFIG["feed_url"]:
        print("❌ No feed URL configured")
        return False

    try:
        resp = requests.get(CONFIG["feed_url"], timeout=30)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        all_jobs = root.findall(".//job")

        threshold = to_float(CONFIG.get("cpc_threshold"), 0.0)
        filtered = []
        cpc_counts: Dict[str, int] = {}

        for job in all_jobs:
            cpc_raw = job.findtext("cpc", default="0")
            cpc = to_float(cpc_raw, 0.0)

            if cpc >= threshold:
                filtered.append(job)
                key = f"{round(cpc, 2):.2f}"
                cpc_counts[key] = cpc_counts.get(key, 0) + 1

        # Write filtered XML
        new_root = ET.Element("jobs")
        for job in filtered:
            new_root.append(job)
        ET.ElementTree(new_root).write(FILTERED_FILE, encoding="utf-8")

        # Update CONFIG
        now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        CONFIG["last_run"] = now_str
        CONFIG["last_total"] = len(filtered)
        CONFIG["stats"] = cpc_counts

        # Log to DB
        with sqlite3.connect(DB_FILE) as conn:
            c = conn.cursor()
            c.execute(
                "INSERT INTO logs (run_time, jobs_total, jobs_filtered, cpc_stats) VALUES (?, ?, ?, ?)",
                (now_str, len(all_jobs), len(filtered), json.dumps(cpc_counts)),
            )
            conn.commit()

        print(f"✅ Filtered {len(filtered)}/{len(all_jobs)} jobs at {now_str}")
        return True

    except Exception as e:
        print(f"❌ Error fetching or filtering feed: {e}")
        return False

# =============================================================================
# Routes
# =============================================================================
@app.route("/", methods=["GET", "POST"])
@login_required
def index():
    # Always reflect latest saved settings in UI
    load_settings_into_config()

    if request.method == "POST":
        try:
            feed_url = (request.form.get("feed_url") or "").strip()
            if not feed_url:
                flash("Feed URL is required", "danger")
                return redirect(url_for("index"))

            cpc_threshold = to_float(request.form.get("cpc_threshold", ""), 0.0)
            if cpc_threshold < 0:
                raise ValueError("CPC threshold cannot be negative")

            interval = to_int(request.form.get("interval", ""), 1)
            if interval < 1 or interval > MAX_INTERVAL_HOURS:
                raise ValueError(f"Interval must be between 1 and {MAX_INTERVAL_HOURS} hours")

            # Save to CONFIG
            CONFIG["feed_url"] = feed_url
            CONFIG["cpc_threshold"] = float(cpc_threshold)
            CONFIG["interval"] = int(interval)

            # Persist to DB first (source of truth)
            save_settings_from_config()

            # Update scheduler
            if not update_scheduler(interval):
                flash("Failed to update scheduler", "danger")
                return redirect(url_for("index"))

            # Run immediately once
            if fetch_and_filter():
                _update_next_run_label()
                flash("✅ First run completed successfully!", "success")
            else:
                flash("❌ Error during first run. Check your feed URL and threshold.", "danger")

            return redirect(url_for("dashboard"))

        except Exception as e:
            flash(f"Unexpected error: {e}", "danger")
            return redirect(url_for("index"))

    return render_template("setup.html", config=CONFIG, title="Setup Job Feed")

@app.route("/dashboard")
@login_required
def dashboard():
    # Ensure latest settings are loaded for display
    load_settings_into_config()

    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        # Last run
        c.execute(
            "SELECT run_time, jobs_total, jobs_filtered, cpc_stats FROM logs ORDER BY id DESC LIMIT 1"
        )
        last = c.fetchone()
        # Last 10 runs
        c.execute("SELECT * FROM logs ORDER BY id DESC LIMIT 10")
        logs = c.fetchall()

    stats = {}
    last_run = None
    total_filtered = None

    if last:
        last_run = last[0]           # run_time
        total_filtered = last[2]     # jobs_filtered
        try:
            stats = json.loads(last[3]) if last[3] else {}
        except json.JSONDecodeError:
            stats = {}

    return render_template(
        "dashboard.html",
        total=total_filtered,
        filtered_link=url_for("download"),
        last_run=last_run,
        stats=stats,
        next_run=CONFIG.get("next_run"),
        logs=logs,
        current_threshold=CONFIG.get("cpc_threshold", 0),
        current_feed_url=CONFIG.get("feed_url"),
        current_interval=CONFIG.get("interval"),
        title="Dashboard",
    )

@app.route("/download")
@login_required
def download():
    if os.path.exists(FILTERED_FILE):
        return send_file(FILTERED_FILE, as_attachment=True)
    flash("No filtered XML file yet.", "warning")
    return redirect(url_for("dashboard"))

@app.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("dashboard"))

    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        password = request.form.get("password") or ""

        with sqlite3.connect(DB_FILE) as conn:
            c = conn.cursor()
            c.execute("SELECT id, email, password_hash FROM users WHERE email=?", (email,))
            row = c.fetchone()

        if row and check_password_hash(row[2], password):
            user = User(*row)
            login_user(user, remember=True)
            flash("✅ Logged in successfully.", "success")
            next_url = request.args.get("next") or url_for("dashboard")
            return redirect(next_url)
        else:
            flash("❌ Invalid credentials.", "danger")

    return render_template("login.html", title="Login")

@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "GET":
        return render_template("register.html", title="Register", allow_signup=ALLOW_SIGNUP)

    if not ALLOW_SIGNUP:
        flash("Signups are currently disabled.", "warning")
        return redirect(url_for("login"))

    email = (request.form.get("email") or "").strip().lower()
    password = (request.form.get("password") or "")
    confirm  = (request.form.get("confirm") or "")

    if not email or "@" not in email:
        flash("Please enter a valid email.", "danger")
        return redirect(url_for("register"))
    if len(password) < 8:
        flash("Password must be at least 8 characters.", "danger")
        return redirect(url_for("register"))
    if password != confirm:
        flash("Passwords do not match.", "danger")
        return redirect(url_for("register"))

    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("SELECT id FROM users WHERE email=?", (email,))
        if c.fetchone():
            flash("Email already registered. Try logging in.", "warning")
            return redirect(url_for("login"))

        c.execute(
            "INSERT INTO users (email, password_hash, created_at) VALUES (?, ?, ?)",
            (email, generate_password_hash(password), datetime.datetime.now().isoformat()),
        )
        conn.commit()
        user_id = c.lastrowid

    user = User(user_id, email, "")  # password hash not needed for session
    login_user(user, remember=True)
    flash("🎉 Account created and logged in.", "success")
    return redirect(url_for("dashboard"))

@app.route("/logout")
@login_required
def logout():
    logout_user()
    flash("👋 Logged out.", "info")
    return redirect(url_for("login"))

# --- One-time seeding route. Remove after first use.
@app.route("/create-admin")
def create_admin_route():
    seed_admin()
    return "Seed attempted. Check console."

# --- Optional: health check
@app.route("/healthz")
def healthz():
    return {"status": "ok", "next_run": CONFIG.get("next_run")}, 200

# =============================================================================
# Bootstrap
# =============================================================================
init_db()
load_settings_into_config()
# Recreate the scheduler job on boot if a feed is configured
if CONFIG.get("feed_url"):
    update_scheduler(CONFIG.get("interval") or 1)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=True)
