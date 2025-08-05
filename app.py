import os
import datetime
import requests
import xml.etree.ElementTree as ET
import sqlite3
from flask import Flask, request, render_template, redirect, url_for, send_file, flash
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)
app.secret_key = "supersecretkey"  # needed for flash messages

DB_FILE = "logs.db"
FILTERED_FILE = "static/filtered_feed.xml"

CONFIG = {
    "feed_url": None,
    "cpc_threshold": 0.0,
    "last_run": None,
    "next_run": None,
    "interval": None
}

# APScheduler
scheduler = BackgroundScheduler()
scheduler.start()


# === DB Setup ===
def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        run_time TEXT,
                        jobs_total INTEGER,
                        jobs_filtered INTEGER,
                        cpc_stats TEXT
                    )""")
        conn.commit()


init_db()


def fetch_and_filter():
    try:
        url = CONFIG["feed_url"]
        threshold = CONFIG["cpc_threshold"]
        if not url:
            return

        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)

        filtered_jobs = []
        cpc_counts = {}

        for job in root.findall(".//job"):
            try:
                cpc = float(job.findtext("cpc", default="0") or 0)
            except ValueError:
                cpc = 0

            if cpc >= threshold:
                # Extract full job info
                job_info = {
                    "guid": job.findtext("guid"),
                    "reference": job.findtext("referencenumber"),
                    "url": job.findtext("url"),
                    "title": job.findtext("title"),
                    "company": job.findtext("company"),
                    "country": job.find("region/country").text if job.find("region/country") is not None else None,
                    "state": job.find("region/state").text if job.find("region/state") is not None else None,
                    "city": job.find("region/city").text if job.find("region/city") is not None else None,
                    "date_updated": job.findtext("date_updated"),
                    "date_expired": job.findtext("date_expired"),
                    "jobtype": job.findtext("jobtype"),
                    "cpc": cpc,
                    "currency": job.findtext("currency"),
                    "salary_min": job.find("salary/min").text if job.find("salary/min") is not None else None,
                    "salary_max": job.find("salary/max").text if job.find("salary/max") is not None else None,
                    "salary_currency": job.find("salary/currency").text if job.find("salary/currency") is not None else None,
                    "salary_rate": job.find("salary/rate").text if job.find("salary/rate") is not None else None,
                    "description": job.findtext("description")
                }
                filtered_jobs.append(job)

                # Update CPC stats
                cpc_rounded = round(cpc, 2)
                cpc_counts[cpc_rounded] = cpc_counts.get(cpc_rounded, 0) + 1

        # Build filtered XML
        new_root = ET.Element("jobs")
        for job in filtered_jobs:
            new_root.append(job)

        os.makedirs("static", exist_ok=True)
        tree = ET.ElementTree(new_root)
        tree.write(FILTERED_FILE, encoding="utf-8")

        CONFIG["last_run"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        with sqlite3.connect(DB_FILE) as conn:
            c = conn.cursor()
            c.execute("INSERT INTO logs (run_time, jobs_total, jobs_filtered, cpc_stats) VALUES (?, ?, ?, ?)",
                      (CONFIG["last_run"], len(root.findall(".//job")), len(filtered_jobs), str(cpc_counts)))
            conn.commit()

        print(f"✅ Run completed: {len(filtered_jobs)} jobs filtered.")
        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        return False


@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        CONFIG["feed_url"] = request.form["feed_url"]
        CONFIG["cpc_threshold"] = float(request.form["cpc_threshold"])
        interval = int(request.form["interval"])
        CONFIG["interval"] = interval

        # Schedule cron job
        scheduler.add_job(fetch_and_filter, 'interval', hours=interval,
                          id="feed_job", replace_existing=True)
        CONFIG["next_run"] = f"Every {interval} hour(s)"

        # 🚀 Run immediately
        if fetch_and_filter():
            flash("✅ First run completed successfully!", "success")
        else:
            flash("❌ Error during first run. Check feed URL and try again.", "danger")

        return redirect(url_for("dashboard"))

    return render_template("index.html", title="Setup Job Feed")


@app.route("/dashboard")
def dashboard():
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM logs ORDER BY id DESC LIMIT 10")
        logs = c.fetchall()

    return render_template("dashboard.html", config=CONFIG, logs=logs, title="Dashboard")


@app.route("/download")
def download():
    if os.path.exists(FILTERED_FILE):
        return send_file(FILTERED_FILE, as_attachment=True)
    return "No filtered file yet."


if __name__ == "__main__":
    app.run(debug=True)
