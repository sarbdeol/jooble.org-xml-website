import os
import datetime
import json
import sqlite3
import xml.etree.ElementTree as ET
from typing import Optional, Dict, Any, List, Tuple

import requests
from flask import (
    Flask, request, render_template, redirect, url_for, send_file, flash,
    send_from_directory, abort
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
STATIC_DIR = "static"
FEEDS_DIR = os.path.join(STATIC_DIR, "feeds")
MAX_INTERVAL_HOURS = 8760  # 1 year
ALLOW_SIGNUP = os.getenv("ALLOW_SIGNUP", "1") == "1"  # set ALLOW_SIGNUP=0 to disable

CONFIG: Dict[str, Any] = {
    "feed_url": None,          # single incoming feed
    "interval": 1,
    "base_slug": "stellenonline",  # used in nested URLs: /<base_slug>/<profile_slug>
    "last_run": None,
    "next_run": None,
    "last_total": 0,
    "stats": {}
}

# =============================================================================
# Helpers (parsing + settings persistence)
# =============================================================================
def to_float(val, default=0.0):
    try:
        if val is None: return default
        if isinstance(val, (int, float)): return float(val)
        s = str(val).strip()
        return float(s) if s else default
    except Exception:
        return default

def to_int(val, default=1):
    try:
        if val is None: return default
        s = str(val).strip()
        return int(s) if s else default
    except Exception:
        return default

def normalize_slug(s: str, default: str = "stellenonline") -> str:
    if not s:
        return default
    s = s.strip().lower()
    # keep alnum, dash, underscore only
    return "".join(ch for ch in s if ch.isalnum() or ch in "-_") or default

def init_db():
    os.makedirs(STATIC_DIR, exist_ok=True)
    os.makedirs(FEEDS_DIR, exist_ok=True)
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()

        # logs (add file_path, profile_slug)
        c.execute("""
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_time TEXT,
                jobs_total INTEGER,
                jobs_filtered INTEGER,
                cpc_stats TEXT,
                file_path TEXT,
                profile_slug TEXT
            )
        """)
        for col, sqltype in [("file_path", "TEXT"), ("profile_slug", "TEXT")]:
            try: c.execute(f"ALTER TABLE logs ADD COLUMN {col} {sqltype}")
            except sqlite3.OperationalError: pass

        # users
        c.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                created_at TEXT
            )
        """)

        # settings (feed_url, interval, base_slug)
        c.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                id INTEGER PRIMARY KEY CHECK (id=1),
                feed_url TEXT,
                interval INTEGER,
                base_slug TEXT
            )
        """)
        for col, sqltype in [("base_slug", "TEXT")]:
            try: c.execute(f"ALTER TABLE settings ADD COLUMN {col} {sqltype}")
            except sqlite3.OperationalError: pass

        # profiles: many outgoing XML definitions (slug + min/max)
        c.execute("""
            CREATE TABLE IF NOT EXISTS profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                slug TEXT UNIQUE NOT NULL,
                cpc_min REAL NOT NULL,
                cpc_max REAL,
                is_enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT
            )
        """)

        conn.commit()

def seed_admin():
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        try:
            c.execute(
                "INSERT INTO users (email, password_hash, created_at) VALUES (?, ?, ?)",
                ("admin@example.com", generate_password_hash("ChangeMe123!"), datetime.datetime.now().isoformat())
            )
            conn.commit()
            print("✅ Seeded admin: admin@example.com / ChangeMe123!")
        except sqlite3.IntegrityError:
            print("ℹ️ Admin already exists")

def load_settings_into_config():
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("SELECT feed_url, interval, base_slug FROM settings WHERE id=1")
        row = c.fetchone()
    if row:
        CONFIG["feed_url"] = (row[0] or None)
        CONFIG["interval"] = to_int(row[1], 1)
        CONFIG["base_slug"] = normalize_slug(row[2] or CONFIG["base_slug"])

def save_settings_from_config():
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("""
            INSERT INTO settings (id, feed_url, interval, base_slug)
            VALUES (1, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
              feed_url=excluded.feed_url,
              interval=excluded.interval,
              base_slug=excluded.base_slug
        """, (CONFIG.get("feed_url"), int(CONFIG.get("interval") or 1), CONFIG.get("base_slug")))
        conn.commit()

def get_profiles(enabled_only=True) -> List[Tuple[str, float, Optional[float]]]:
    """Return list of (slug, cpc_min, cpc_max or None)."""
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        if enabled_only:
            c.execute("SELECT slug, cpc_min, cpc_max FROM profiles WHERE is_enabled=1 ORDER BY id")
        else:
            c.execute("SELECT slug, cpc_min, cpc_max FROM profiles ORDER BY id")
        rows = c.fetchall()
    out = []
    for slug, mn, mx in rows:
        out.append((slug, to_float(mn, 0.0), (to_float(mx, None) if mx is not None else None)))
    return out

def ensure_profile_dir(slug: str):
    os.makedirs(os.path.join(FEEDS_DIR, slug), exist_ok=True)

# =============================================================================
# Scheduler
# =============================================================================
scheduler = BackgroundScheduler(
    daemon=True,
    job_defaults={"coalesce": True, "misfire_grace_time": 300}
)
scheduler.start()

def _update_next_run_label():
    job = scheduler.get_job("feed_job")
    if job and job.next_run_time:
        ts = job.next_run_time
        CONFIG["next_run"] = ts.strftime("%Y-%m-%d %H:%M:%S %Z")
    else:
        CONFIG["next_run"] = None

def update_scheduler(interval_hours: int) -> bool:
    try:
        try:
            scheduler.remove_job("feed_job")
        except JobLookupError:
            pass
        scheduler.add_job(fetch_and_filter, "interval", hours=interval_hours, id="feed_job", replace_existing=True)
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
        self.id = str(id); self.email = email; self.password_hash = password_hash

@login_manager.user_loader
def load_user(user_id: str) -> Optional[User]:
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("SELECT id, email, password_hash FROM users WHERE id=?", (user_id,))
        row = c.fetchone()
        if row: return User(*row)
    return None

# =============================================================================
# Core processing
# =============================================================================
def _write_xml(tree: ET.ElementTree, path: str):
    tree.write(path, encoding="utf-8", xml_declaration=True)

def fetch_and_filter() -> bool:
    """Fetch the incoming feed once, then generate latest.xml for each profile (no archives)."""
    if not CONFIG["feed_url"]:
        print("❌ No feed URL configured")
        return False

    try:
        # Fetch once
        resp = requests.get(CONFIG["feed_url"], timeout=30)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        all_jobs = root.findall(".//job")

        profiles = get_profiles(enabled_only=True)
        if not profiles:
            print("ℹ️ No enabled profiles; nothing to generate.")
            return True

        now = datetime.datetime.now()
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")

        CONFIG["last_run"] = now_str
        CONFIG["last_total"] = 0
        CONFIG["stats"] = {}

        for slug, cpc_min, cpc_max in profiles:
            ensure_profile_dir(slug)

            filtered = []
            cpc_counts: Dict[str, int] = {}

            for job in all_jobs:
                cpc_raw = job.findtext("cpc", default="0")
                cpc = to_float(cpc_raw, 0.0)
                if cpc < cpc_min:
                    continue
                if cpc_max is not None and cpc > cpc_max:
                    continue
                filtered.append(job)
                key = f"{round(cpc, 2):.2f}"
                cpc_counts[key] = cpc_counts.get(key, 0) + 1

            new_root = ET.Element("jobs")
            for job in filtered:
                new_root.append(job)
            tree = ET.ElementTree(new_root)

            # Write ONLY the latest file per profile (no archives)
            latest_path = os.path.join(FEEDS_DIR, slug, "latest.xml")
            _write_xml(tree, latest_path)

            # Update summary (last profile’s numbers)
            CONFIG["last_total"] = len(filtered)
            CONFIG["stats"] = cpc_counts

            # Log run (file_path is None since no archive)
            with sqlite3.connect(DB_FILE) as conn:
                c = conn.cursor()
                c.execute(
                    "INSERT INTO logs (run_time, jobs_total, jobs_filtered, cpc_stats, file_path, profile_slug) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (now_str, len(all_jobs), len(filtered), json.dumps(cpc_counts), None, slug)
                )
                conn.commit()

        print(f"✅ Generated latest.xml for {len(profiles)} profile(s) at {now_str}")
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
    load_settings_into_config()

    if request.method == "POST":
        try:
            feed_url = (request.form.get("feed_url") or "").strip()
            if not feed_url:
                flash("Feed URL is required", "danger")
                return redirect(url_for("index"))

            interval = to_int(request.form.get("interval", ""), 1)
            if interval < 1 or interval > MAX_INTERVAL_HOURS:
                raise ValueError(f"Interval must be between 1 and {MAX_INTERVAL_HOURS} hours")

            # Optional: allow updating base_slug if present in form
            base_slug_in = request.form.get("base_slug")
            if base_slug_in is not None:
                CONFIG["base_slug"] = normalize_slug(base_slug_in, CONFIG["base_slug"])

            CONFIG["feed_url"] = feed_url
            CONFIG["interval"] = int(interval)
            save_settings_from_config()

            if not update_scheduler(interval):
                flash("Failed to update scheduler", "danger")
                return redirect(url_for("index"))

            if fetch_and_filter():
                _update_next_run_label()
                flash("✅ First run completed successfully!", "success")
            else:
                flash("❌ Error during first run.", "danger")

            return redirect(url_for("dashboard"))

        except Exception as e:
            flash(f"Unexpected error: {e}", "danger")
            return redirect(url_for("index"))

    # If you want to show base_slug in the setup form, pass it in config
    return render_template("setup.html", config=CONFIG, title="Setup Job Feed", profiles=get_profiles(enabled_only=False))

@app.route("/dashboard")
@login_required
def dashboard():
    load_settings_into_config()
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM logs ORDER BY id DESC LIMIT 200")
        logs = c.fetchall()

    return render_template(
        "dashboard.html",
        logs=logs,
        last_run=CONFIG.get("last_run"),
        next_run=CONFIG.get("next_run"),
        stats=CONFIG.get("stats") or {},
        current_feed_url=CONFIG.get("feed_url"),
        current_interval=CONFIG.get("interval"),
        profiles=get_profiles(enabled_only=False),   # <-- needed
        public_base=request.url_root.rstrip("/"),    # <-- needed
        base_slug=CONFIG.get("base_slug"),           # <-- needed
        title="Dashboard",
    )

# Admin download (latest) for a given profile
@app.route("/download")
@login_required
def download():
    slug = (request.args.get("slug") or "").strip()
    if not slug:
        profs = get_profiles(enabled_only=True)
        if not profs:
            flash("No profiles defined yet.", "warning")
            return redirect(url_for("dashboard"))
        slug = profs[0][0]
    latest_path = os.path.join(FEEDS_DIR, slug, "latest.xml")
    if os.path.exists(latest_path):
        return send_file(latest_path, as_attachment=True, download_name=f"{slug}.xml", mimetype="application/xml")
    flash("No latest XML found for that profile.", "warning")
    return redirect(url_for("dashboard"))

# --- PUBLIC (legacy): latest for a profile
@app.route("/<slug>")
def public_latest(slug):
    latest_path = os.path.join(FEEDS_DIR, slug, "latest.xml")
    if os.path.exists(latest_path):
        return send_file(latest_path, as_attachment=False, mimetype="application/xml", download_name=f"{slug}.xml")
    return ("No filtered XML yet.", 404)

# --- PUBLIC (legacy): archives for a profile
# @app.route("/feeds/<slug>/<path:filename>")
# def public_feed_history(slug, filename):
#     base = os.path.join(FEEDS_DIR, slug)
#     return send_from_directory(base, filename, mimetype="application/xml")

# --- PUBLIC (nested): latest for a profile under a base slug (e.g., /stellenonline/xml1)
@app.route("/<base>/<slug>")
def public_latest_nested(base, slug):
    if normalize_slug(base) != CONFIG.get("base_slug"):
        return ("Unknown path", 404)
    latest_path = os.path.join(FEEDS_DIR, slug, "latest.xml")
    if os.path.exists(latest_path):
        return send_file(latest_path, as_attachment=False, mimetype="application/xml",
                         download_name=f"{slug}.xml")
    return ("No filtered XML yet.", 404)

# --- PUBLIC (nested): archives for a profile (e.g., /feeds/stellenonline/xml1/filtered_*.xml)
# @app.route("/feeds/<base>/<slug>/<path:filename>")
# def public_feed_history_nested(base, slug, filename):
#     if normalize_slug(base) != CONFIG.get("base_slug"):
#         return ("Unknown path", 404)
#     base_dir = os.path.join(FEEDS_DIR, slug)
#     return send_from_directory(base_dir, filename, mimetype="application/xml")

# --- PUBLIC (hyphen style): /stellenonline-xml1
@app.route("/<base>-<slug>")
def public_latest_hyphen(base, slug):
    if normalize_slug(base) != CONFIG.get("base_slug"):
        return ("Unknown path", 404)
    latest_path = os.path.join(FEEDS_DIR, slug, "latest.xml")
    if os.path.exists(latest_path):
        return send_file(latest_path, as_attachment=False, mimetype="application/xml",
                         download_name=f"{slug}.xml")
    return ("No filtered XML yet.", 404)

# --- Simple profile management (list + add + delete)
@app.route("/profiles", methods=["GET", "POST"])
@login_required
def profiles():
    if request.method == "POST":
        slug = normalize_slug(request.form.get("slug") or "")
        cpc_min = to_float(request.form.get("cpc_min", ""), 0.0)
        cpc_max_raw = (request.form.get("cpc_max") or "").strip()
        cpc_max = None if cpc_max_raw == "" else to_float(cpc_max_raw, None)
        enabled = 1 if (request.form.get("is_enabled") == "on") else 0

        if not slug:
            flash("Slug is required.", "danger")
            return redirect(url_for("profiles"))
        if cpc_max is not None and cpc_max < cpc_min:
            flash("Max CPC must be >= Min CPC.", "danger")
            return redirect(url_for("profiles"))

        ensure_profile_dir(slug)
        with sqlite3.connect(DB_FILE) as conn:
            c = conn.cursor()
            try:
                c.execute("""
                    INSERT INTO profiles (slug, cpc_min, cpc_max, is_enabled, created_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (slug, float(cpc_min), cpc_max if cpc_max is None else float(cpc_max), enabled, datetime.datetime.now().isoformat()))
                conn.commit()
                flash("Profile created.", "success")
            except sqlite3.IntegrityError:
                flash("Slug already exists.", "warning")
        return redirect(url_for("profiles"))

    # GET: list
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("SELECT id, slug, cpc_min, cpc_max, is_enabled FROM profiles ORDER BY id")
        profs = c.fetchall()
    return render_template("profiles.html", profiles=profs, title="Profiles")

@app.route("/profiles/<int:pid>/delete", methods=["POST"])
@login_required
def delete_profile(pid):
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("DELETE FROM profiles WHERE id=?", (pid,))
        conn.commit()
    flash("Profile deleted.", "info")
    return redirect(url_for("profiles"))

# --- Auth ---
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
            login_user(User(*row), remember=True)
            flash("✅ Logged in successfully.", "success")
            return redirect(url_for("dashboard"))
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
        flash("Please enter a valid email.", "danger"); return redirect(url_for("register"))
    if len(password) < 8:
        flash("Password must be at least 8 characters.", "danger"); return redirect(url_for("register"))
    if password != confirm:
        flash("Passwords do not match.", "danger"); return redirect(url_for("register"))
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("SELECT id FROM users WHERE email=?", (email,))
        if c.fetchone():
            flash("Email already registered. Try logging in.", "warning"); return redirect(url_for("login"))
        c.execute("INSERT INTO users (email, password_hash, created_at) VALUES (?, ?, ?)",
                  (email, generate_password_hash(password), datetime.datetime.now().isoformat()))
        conn.commit(); user_id = c.lastrowid
    login_user(User(user_id, email, ""), remember=True)
    flash("🎉 Account created and logged in.", "success")
    return redirect(url_for("dashboard"))

@app.route("/logout")
@login_required
def logout():
    logout_user()
    flash("👋 Logged out.", "info")
    return redirect(url_for("login"))

@app.route("/create-admin")
def create_admin_route():
    seed_admin(); return "Seed attempted. Check console."

@app.route("/healthz")
def healthz():
    return {"status": "ok", "next_run": CONFIG.get("next_run")}, 200

# =============================================================================
# Bootstrap
# =============================================================================
init_db()
load_settings_into_config()
# Ensure at least one default profile exists
with sqlite3.connect(DB_FILE) as conn:
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM profiles")
    if c.fetchone()[0] == 0:
        c.execute("""INSERT INTO profiles (slug, cpc_min, cpc_max, is_enabled, created_at)
                     VALUES (?, ?, ?, 1, ?)""",
                  ("xml1", 0.0, None, datetime.datetime.now().isoformat()))
        conn.commit()

# Recreate the scheduler job on boot if a feed is configured
if CONFIG.get("feed_url"):
    update_scheduler(CONFIG.get("interval") or 1)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=True)
