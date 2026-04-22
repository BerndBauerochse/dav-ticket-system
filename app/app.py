import json
import os
import re
import secrets
import sqlite3
import threading
import time
import urllib.parse
from datetime import datetime, timedelta
from functools import wraps

import requests
from flask import Flask, flash, jsonify, redirect, render_template, request, session, url_for
from markupsafe import Markup, escape
from werkzeug.security import check_password_hash

import metadaten_update

app = Flask(__name__)

DATA_DIR = os.environ.get("DATA_DIR", "/data")
CONFIG_PATH = os.path.join(DATA_DIR, "config.json")
METADATA_REFRESH_INTERVAL_SECONDS = 3600
METADATA_AUTO_UPDATE_STATE = {
    "last_run_at": None,
    "last_success": None,
    "last_message": "Noch kein automatisches Update ausgeführt.",
}


def load_config():
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception as e:
        print(f"Fehler beim Laden der Config: {e}")
        return {}


def save_config(config):
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=4, ensure_ascii=False)
        return True
    except Exception as e:
        print(f"Fehler beim Speichern der Config: {e}")
        return False


def get_secret_key():
    env_secret = os.environ.get("SECRET_KEY")
    if env_secret:
        return env_secret

    config = load_config()
    config_secret = config.get("secret_key")
    if config_secret:
        return config_secret

    generated_secret = secrets.token_urlsafe(48)
    config["secret_key"] = generated_secret
    save_config(config)
    return generated_secret


app.secret_key = get_secret_key()


def get_db_path():
    config = load_config()
    cfg_path = config.get("database_path", os.path.join(DATA_DIR, "tickets.db"))
    if os.path.isabs(cfg_path):
        return cfg_path
    return os.path.join(DATA_DIR, cfg_path)


def get_metadata_path():
    config = load_config()
    folder = config.get("metadata_folder", os.path.join(DATA_DIR, "Titeldaten"))
    if not os.path.isabs(folder):
        folder = os.path.join(DATA_DIR, folder)
    return os.path.join(folder, "metadata.db")


def get_db_connection():
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def get_metadata_connection():
    meta_path = get_metadata_path()
    if os.path.exists(meta_path):
        conn = sqlite3.connect(meta_path)
        conn.row_factory = sqlite3.Row
        return conn
    return None


def check_auth(password):
    config = load_config()
    pwd_hash = config.get("admin_password_hash", "")
    if not pwd_hash:
        return False
    return check_password_hash(pwd_hash, password)


def is_safe_redirect_target(target):
    if not target:
        return False
    parts = urllib.parse.urlsplit(target)
    return not parts.scheme and not parts.netloc and target.startswith("/")


def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            next_url = request.full_path if request.query_string else request.path
            return redirect(url_for("login", next=next_url))
        return f(*args, **kwargs)

    return decorated


def requires_admin_mode(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            next_url = request.full_path if request.query_string else request.path
            return redirect(url_for("login", next=next_url))
        if not session.get("admin_mode"):
            flash("Zugriff verweigert. Bitte zuerst den Admin-Modus öffnen.", "error")
            return redirect(url_for("dashboard"))
        return f(*args, **kwargs)

    return decorated


def fetch_metadata_by_isbn(isbn):
    conn = None
    try:
        conn = get_metadata_connection()
        if not conn:
            return None

        cursor = conn.cursor()
        cursor.execute("SELECT * FROM metadata LIMIT 1")
        if cursor.description is None:
            return None

        columns = [description[0] for description in cursor.description]
        isbn_col = next((c for c in columns if "ean" in c.lower() or "isbn" in c.lower()), None)
        title_col = next((c for c in columns if "titel" in c.lower() or "title" in c.lower()), None)
        author_col = next(
            (c for c in columns if "autor" in c.lower() or "author" in c.lower() or "artist" in c.lower()),
            None,
        )
        cover_col = next((c for c in columns if "cover" in c.lower() or "bild" in c.lower() or "image" in c.lower()), None)

        if not isbn_col:
            isbn_col = "EAN digital"

        cursor.execute(f'SELECT * FROM metadata WHERE "{isbn_col}" = ?', (str(isbn),))
        row = cursor.fetchone()
        if row:
            data = dict(row)
            return {
                "Titel": data.get(title_col, "Unbekannt") if title_col else "Unbekannt",
                "Autor": data.get(author_col, "") if author_col else "",
                "Cover URL": data.get(cover_col, "") if cover_col else "",
                **data,
            }
    except Exception as e:
        print(f"Metadata Error: {e}")
    finally:
        if conn:
            conn.close()

    return None


def check_and_update_schema():
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(tickets)")
        columns = [info[1] for info in cursor.fetchall()]

        if "created_by" not in columns:
            cursor.execute("ALTER TABLE tickets ADD COLUMN created_by TEXT")
        if "affected_portals" not in columns:
            cursor.execute("ALTER TABLE tickets ADD COLUMN affected_portals TEXT")
        if "author" not in columns:
            cursor.execute("ALTER TABLE tickets ADD COLUMN author TEXT")
            cursor.execute("SELECT id, isbn FROM tickets")
            for row in cursor.fetchall():
                meta = fetch_metadata_by_isbn(row[1])
                if meta and "Autor" in meta:
                    cursor.execute("UPDATE tickets SET author = ? WHERE id = ?", (meta["Autor"], row[0]))
        if "updated_at" not in columns:
            cursor.execute("ALTER TABLE tickets ADD COLUMN updated_at TEXT")
        if "last_comment_at" not in columns:
            cursor.execute("ALTER TABLE tickets ADD COLUMN last_comment_at TEXT")

        conn.commit()
    except Exception as e:
        print(f"Fehler bei Schema-Update: {e}")
    finally:
        if conn:
            conn.close()


check_and_update_schema()


@app.context_processor
def inject_layout_defaults():
    return {"today": datetime.now().strftime("%Y")}


@app.template_filter("format_comments")
def format_comments(text):
    if not text:
        return ""

    escaped = str(escape(text)).replace("\n", "<br>")
    pattern = r"(\[Kommentar Admin - \d{2}\.\d{2}\.\d{4}(?: \d{2}:\d{2})?\]:)"

    def replace(match):
        return (
            '<br><span class="text-audible-accent font-bold block mt-2 mb-1 '
            'border-l-2 border-audible-accent pl-2">'
            f"{match.group(1)}</span>"
        )

    return Markup(re.sub(pattern, replace, escaped))


def send_teams_notification(ticket_data):
    config = load_config()
    webhook_url = config.get("teams_webhook")
    if not webhook_url:
        return

    def _send():
        try:
            payload = {
                "@type": "MessageCard",
                "@context": "http://schema.org/extensions",
                "themeColor": "d21e40",
                "summary": "Neues Ticket angelegt",
                "sections": [
                    {
                        "activityTitle": f"Neues Ticket: {ticket_data['title']}",
                        "activitySubtitle": f"Erstellt von {ticket_data['created_by']}",
                        "facts": [
                            {"name": "ISBN:", "value": ticket_data["isbn"]},
                            {"name": "Autor:", "value": ticket_data["author"]},
                            {"name": "Problem:", "value": ticket_data["problem_type"]},
                            {"name": "Portale:", "value": ticket_data["affected_portals"]},
                            {"name": "Deadline:", "value": ticket_data["deadline"].strftime("%d.%m.%Y")},
                        ],
                        "markdown": True,
                        "text": f"**Beschreibung:**\n{ticket_data['description']}",
                    }
                ],
                "potentialAction": [
                    {
                        "@type": "OpenUri",
                        "name": "Zum Ticket-Dashboard",
                        "targets": [{"os": "default", "uri": f"{config.get('base_url', 'http://localhost:5000')}/dashboard"}],
                    }
                ],
            }
            requests.post(webhook_url, json=payload, timeout=5)
        except Exception as e:
            print(f"Fehler beim Senden an Teams: {e}")

    threading.Thread(target=_send, daemon=True).start()


def get_comment_block(comment_text):
    timestamp = datetime.now().strftime("%d.%m.%Y %H:%M")
    cleaned_comment = (comment_text or "").strip()
    if not cleaned_comment:
        return ""
    return f"[Kommentar Admin - {timestamp}]:\n{cleaned_comment}"


def extend_deadline_from_today(days=5):
    return (datetime.now().date() + timedelta(days=days)).isoformat()


def update_ticket_metadata_refresh(force=False):
    config = load_config()
    auto_enabled = config.get("metadata_auto_update_enabled", True)
    if not auto_enabled and not force:
        return False, "Automatisches Metadaten-Update ist deaktiviert."

    last_run = config.get("metadata_auto_last_run")
    today = datetime.now().date().isoformat()
    if not force and last_run == today:
        return True, "Automatisches Metadaten-Update wurde heute bereits ausgeführt."

    success, message = metadaten_update.fetch_and_update()
    config["metadata_auto_last_run"] = today
    config["metadata_auto_last_success"] = success
    config["metadata_auto_last_message"] = message
    save_config(config)

    METADATA_AUTO_UPDATE_STATE["last_run_at"] = datetime.now().strftime("%d.%m.%Y %H:%M")
    METADATA_AUTO_UPDATE_STATE["last_success"] = success
    METADATA_AUTO_UPDATE_STATE["last_message"] = message
    return success, message


def metadata_auto_update_loop():
    while True:
        try:
            update_ticket_metadata_refresh(force=False)
        except Exception as e:
            print(f"Fehler im automatischen Metadaten-Update: {e}")
        time.sleep(METADATA_REFRESH_INTERVAL_SECONDS)


def start_metadata_auto_update_scheduler():
    worker = threading.Thread(target=metadata_auto_update_loop, name="metadata-auto-update", daemon=True)
    worker.start()


start_metadata_auto_update_scheduler()


@app.route("/")
def index():
    return render_template("landing.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        password = request.form.get("password", "")
        requested_next = request.form.get("next") or request.args.get("next") or url_for("dashboard")
        next_url = requested_next if is_safe_redirect_target(requested_next) else url_for("dashboard")

        if check_auth(password):
            session.clear()
            session["logged_in"] = True
            flash("Erfolgreich eingeloggt.", "success")
            return redirect(next_url)

        flash("Ungültiges Passwort.", "error")

    next_url = request.args.get("next", "")
    if not is_safe_redirect_target(next_url):
        next_url = ""
    return render_template("login.html", next_url=next_url)


@app.route("/logout", methods=["POST"])
@requires_auth
def logout():
    session.clear()
    flash("Erfolgreich ausgeloggt.", "info")
    return redirect(url_for("index"))


@app.route("/new-ticket", methods=["GET", "POST"])
@requires_auth
def new_ticket():
    if request.method == "POST":
        isbn = (request.form.get("isbn") or "").strip()
        problem_type = request.form.get("problem_type")
        description = (request.form.get("description") or "").strip()
        contact_date = request.form.get("initial_contact_date")
        created_by = request.form.get("created_by")
        portals_list = request.form.getlist("portals")
        affected_portals = ", ".join(portals_list) if portals_list else ""

        if not isbn:
            flash("Bitte ISBN angeben.", "error")
            return redirect(url_for("new_ticket"))

        creation_date = datetime.now().date()
        try:
            contact_date_obj = datetime.strptime(contact_date, "%Y-%m-%d").date() if contact_date else creation_date
        except ValueError:
            contact_date_obj = creation_date

        title = "Unbekannter Titel"
        author = ""
        meta = fetch_metadata_by_isbn(isbn)
        if meta:
            title = meta.get("Titel", title)
            author = meta.get("Autor", author)

        deadline = creation_date + timedelta(days=5 if problem_type == "Titel nicht online" else 14)

        conn = None
        try:
            conn = get_db_connection()
            existing = conn.execute(
                "SELECT affected_portals FROM tickets WHERE isbn = ? AND problem_type = ? AND status = 'offen'",
                (isbn, problem_type),
            ).fetchall()

            if existing:
                new_portals_set = set(portals_list)
                for row in existing:
                    existing_set = {p.strip() for p in (row[0] or "").split(",") if p.strip()}
                    intersection = new_portals_set.intersection(existing_set)
                    if intersection:
                        flash(
                            f"STOP: Ein offenes Ticket für diese ISBN und das Problem '{problem_type}' existiert bereits für: {', '.join(sorted(intersection))}.",
                            "error",
                        )
                        return redirect(url_for("new_ticket"))

            now_iso = datetime.now().isoformat(timespec="seconds")
            conn.execute(
                """
                INSERT INTO tickets
                (creation_date, problem_type, description, isbn, deadline, initial_contact_date, title, status,
                 created_by, affected_portals, author, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    creation_date.isoformat(),
                    problem_type,
                    description,
                    isbn,
                    deadline.isoformat(),
                    contact_date_obj.isoformat(),
                    title,
                    "offen",
                    created_by,
                    affected_portals,
                    author,
                    now_iso,
                ),
            )
            conn.commit()

            send_teams_notification(
                {
                    "title": title,
                    "author": author,
                    "created_by": created_by,
                    "isbn": isbn,
                    "problem_type": problem_type,
                    "affected_portals": affected_portals,
                    "deadline": deadline,
                    "description": description,
                }
            )
            flash(f"Ticket für '{title}' erfolgreich angelegt.", "success")
            return redirect(url_for("new_ticket"))
        except Exception as e:
            flash(f"Fehler beim Speichern: {str(e)}", "error")
        finally:
            if conn:
                conn.close()

    return render_template("creator.html", form_today=datetime.now().strftime("%Y-%m-%d"))


@app.route("/api/metadata/<isbn>")
@requires_auth
def api_metadata(isbn):
    meta = fetch_metadata_by_isbn(isbn)
    if meta:
        return jsonify(
            {
                "found": True,
                "title": meta.get("Titel", "Unbekannt"),
                "author": meta.get("Autor", ""),
                "cover_url": meta.get("Cover URL", ""),
            }
        )
    return jsonify({"found": False})


@app.route("/dashboard")
@requires_auth
def dashboard():
    if session.get("admin_mode"):
        return redirect(url_for("admin_dashboard"))
    return render_dashboard(admin_mode=False)


@app.route("/admin")
@requires_auth
def admin_dashboard():
    session["admin_mode"] = True
    return render_dashboard(admin_mode=True)


@app.route("/exit_admin", methods=["POST"])
@requires_auth
def exit_admin():
    session.pop("admin_mode", None)
    return redirect(url_for("dashboard"))


def render_dashboard(admin_mode):
    sort_order = request.args.get("sort", "asc")
    order_clause = "ORDER BY deadline DESC" if sort_order == "desc" else "ORDER BY deadline ASC"
    conn = get_db_connection()
    try:
        tickets = conn.execute(f'SELECT * FROM tickets WHERE status = "offen" {order_clause}').fetchall()
    except sqlite3.OperationalError as e:
        flash(f"Datenbankfehler: {e}", "error")
        tickets = []
    finally:
        conn.close()

    return render_template(
        "dashboard.html",
        tickets=tickets,
        view_mode="open",
        now=datetime.now().strftime("%Y-%m-%d"),
        sort_order=sort_order,
        admin_mode=admin_mode,
    )


@app.route("/archive")
@requires_auth
def archive():
    conn = get_db_connection()
    try:
        cutoff_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
        tickets = conn.execute(
            'SELECT * FROM tickets WHERE status = "erledigt" AND completion_date >= ? ORDER BY completion_date DESC, id DESC',
            (cutoff_date,),
        ).fetchall()
    except sqlite3.OperationalError as e:
        flash(f"Datenbankfehler: {e}", "error")
        tickets = []
    finally:
        conn.close()

    return render_template("dashboard.html", tickets=tickets, view_mode="archive", now=datetime.now().strftime("%Y-%m-%d"))


@app.route("/ticket/<int:id>/complete", methods=["POST"])
@requires_auth
def complete_ticket(id):
    conn = get_db_connection()
    conn.execute(
        "UPDATE tickets SET status = ?, completion_date = ?, updated_at = ? WHERE id = ?",
        ("erledigt", datetime.now().date().isoformat(), datetime.now().isoformat(timespec="seconds"), id),
    )
    conn.commit()
    conn.close()
    flash("Ticket als erledigt markiert.", "success")
    return redirect(url_for("dashboard"))


@app.route("/ticket/<int:id>/reopen", methods=["POST"])
@requires_auth
def reopen_ticket(id):
    conn = get_db_connection()
    conn.execute(
        "UPDATE tickets SET status = ?, completion_date = NULL, deadline = ?, updated_at = ? WHERE id = ?",
        ("offen", extend_deadline_from_today(), datetime.now().isoformat(timespec="seconds"), id),
    )
    conn.commit()
    conn.close()
    flash("Ticket wiedereröffnet und Deadline um 5 Tage verlängert.", "success")
    return redirect(url_for("archive"))


@app.route("/ticket/<int:id>/delete", methods=["POST"])
@requires_admin_mode
def delete_ticket(id):
    conn = get_db_connection()
    conn.execute("DELETE FROM tickets WHERE id = ?", (id,))
    conn.commit()
    conn.close()
    flash("Ticket gelöscht.", "info")
    return redirect(url_for("dashboard"))


def generate_mail_content(ticket):
    title = ticket["title"] or "Unknown Title"
    isbn = ticket["isbn"] or "Unknown ISBN"
    ptype = ticket["problem_type"]
    init_date = ticket["initial_contact_date"]
    date_str = init_date if isinstance(init_date, str) else (init_date.strftime("%d.%m.%Y") if init_date else "N/A")

    if ptype == "Preisänderung":
        subject = f"[change request] price change - {title} - {isbn}"
        body = (
            "Dear Audible Team,\n\n"
            f"On {date_str}, I submitted a price change request for...\n\n"
            f"ISBN: {isbn}\nTitle: {title}\n\n"
            "Please process immediately.\n\nBest regards,\nDoro"
        )
    elif ptype == "Titel nicht online":
        subject = f"[title not online] - {title} - {isbn}"
        body = (
            "Dear Audible Team,\n\n"
            f"The title {title} ({isbn}) is still not online.\n"
            f"Release Date was supposed to be around {date_str}.\n\n"
            "Please check.\n\nBest regards,\nDoro"
        )
    else:
        subject = f"Audible Issue: {ptype} - {title}"
        body = (
            "Hello Audible Team,\n\n"
            "We have an issue regarding:\n"
            f"Title: {title}\nISBN: {isbn}\n\n"
            f"Problem: {ptype}\nReference Date: {date_str}\n\n"
            "Please check.\n\nBest regards,\nDAV Digital Team"
        )

    return urllib.parse.quote(subject), urllib.parse.quote(body)


@app.route("/ticket/<int:id>/edit", methods=["GET", "POST"])
@requires_admin_mode
def edit_ticket(id):
    conn = get_db_connection()

    if request.method == "POST":
        portals_list = request.form.getlist("portals")
        comment_text = request.form.get("admin_comment", "")
        existing_ticket = conn.execute("SELECT * FROM tickets WHERE id = ?", (id,)).fetchone()
        if not existing_ticket:
            conn.close()
            flash("Ticket nicht gefunden.", "error")
            return redirect(url_for("dashboard"))

        new_status = request.form.get("status")
        updated_description = (request.form.get("description") or "").strip()
        comment_block = get_comment_block(comment_text)
        last_comment_at = existing_ticket["last_comment_at"]
        if comment_block:
            updated_description = f"{updated_description}\n\n{comment_block}".strip()
            last_comment_at = datetime.now().isoformat(timespec="seconds")

        new_deadline = request.form.get("deadline")
        if new_status == "offen":
            new_deadline = extend_deadline_from_today()

        completion_date = existing_ticket["completion_date"]
        if new_status == "erledigt":
            completion_date = datetime.now().date().isoformat()
        elif existing_ticket["status"] == "erledigt" and new_status == "offen":
            completion_date = None

        try:
            conn.execute(
                """
                UPDATE tickets
                SET title = ?, author = ?, isbn = ?, status = ?, problem_type = ?, description = ?,
                    deadline = ?, initial_contact_date = ?, affected_portals = ?, completion_date = ?,
                    updated_at = ?, last_comment_at = ?
                WHERE id = ?
                """,
                (
                    request.form.get("title"),
                    request.form.get("author"),
                    request.form.get("isbn"),
                    new_status,
                    request.form.get("problem_type"),
                    updated_description,
                    new_deadline,
                    request.form.get("initial_contact_date"),
                    ", ".join(portals_list) if portals_list else "",
                    completion_date,
                    datetime.now().isoformat(timespec="seconds"),
                    last_comment_at,
                    id,
                ),
            )
            conn.commit()
            if comment_block and new_status == "offen":
                flash("Ticket aktualisiert, Kommentar ergänzt und Deadline um 5 Tage verlängert.", "success")
            elif new_status == "offen":
                flash("Ticket aktualisiert und Deadline um 5 Tage verlängert.", "success")
            else:
                flash("Ticket erfolgreich aktualisiert.", "success")
        except Exception as e:
            flash(f"Fehler beim Speichern: {e}", "error")
        finally:
            conn.close()

        return redirect(url_for("admin_dashboard") if session.get("admin_mode") else url_for("dashboard"))

    ticket = conn.execute("SELECT * FROM tickets WHERE id = ?", (id,)).fetchone()
    conn.close()
    if not ticket:
        flash("Ticket nicht gefunden.", "error")
        return redirect(url_for("dashboard"))

    t_dict = dict(ticket)
    subj, body = generate_mail_content(t_dict)
    config = load_config()
    auto_update_enabled = config.get("metadata_auto_update_enabled", True)
    auto_update_message = config.get("metadata_auto_last_message") or METADATA_AUTO_UPDATE_STATE["last_message"]
    auto_update_last_run = config.get("metadata_auto_last_run")

    return render_template(
        "edit_ticket.html",
        ticket=t_dict,
        mail_subject=subj,
        mail_body=body,
        auto_update_enabled=auto_update_enabled,
        auto_update_message=auto_update_message,
        auto_update_last_run=auto_update_last_run,
    )


@app.route("/settings", methods=["GET", "POST"])
@requires_admin_mode
def settings():
    config = load_config()
    if request.method == "POST":
        for key in ["database_path", "metadata_folder", "teams_webhook", "metadata_webhook_url"]:
            val = request.form.get(key)
            if val is not None:
                config[key] = val.strip()

        config["metadata_auto_update_enabled"] = request.form.get("metadata_auto_update_enabled") == "on"

        if save_config(config):
            flash("Einstellungen erfolgreich gespeichert.", "success")
        else:
            flash("Fehler beim Speichern der Einstellungen.", "error")
        return redirect(url_for("settings"))

    return render_template(
        "settings.html",
        config=config,
        auto_update_state=METADATA_AUTO_UPDATE_STATE,
    )


@app.route("/settings/update_metadata", methods=["POST"])
@requires_admin_mode
def update_metadata():
    success, message = update_ticket_metadata_refresh(force=True)
    flash(message, "success" if success else "error")
    return redirect(url_for("settings"))


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=5000)
