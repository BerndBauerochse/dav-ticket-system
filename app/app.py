import json
import os
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
BACKGROUND_INTERVAL_SECONDS = 60
DEFAULT_METADATA_UPDATE_TIME = "06:00"
TEAM_MEMBERS = ["Doro", "Bernd"]
BATCH_OPTIONS = ["", "Stuck"]
PROBLEM_TYPE_ALIASES = {
    "Preisänderung": "Preisaenderung",
    "PreisÃ¤nderung": "Preisaenderung",
    "Preisaenderung": "Preisaenderung",
    "Rezension löschen": "Rezension loeschen",
    "Rezension lÃ¶schen": "Rezension loeschen",
    "Rezension loeschen": "Rezension loeschen",
    "Metadaten ändern": "Metadaten aendern",
    "Metadaten Ã¤ndern": "Metadaten aendern",
    "Metadaten aendern": "Metadaten aendern",
}
METADATA_AUTO_UPDATE_STATE = {
    "last_run_at": None,
    "last_success": None,
    "last_message": "Noch kein automatisches Update ausgefuehrt.",
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

    import secrets

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
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def get_metadata_connection():
    meta_path = get_metadata_path()
    if not os.path.exists(meta_path):
        return None
    conn = sqlite3.connect(meta_path)
    conn.row_factory = sqlite3.Row
    return conn


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
            flash("Zugriff verweigert. Bitte zuerst den Admin-Modus oeffnen.", "error")
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
        if not row:
            return None

        data = dict(row)
        return {
            "Titel": data.get(title_col, "Unbekannt") if title_col else "Unbekannt",
            "Autor": data.get(author_col, "") if author_col else "",
            "Cover URL": data.get(cover_col, "") if cover_col else "",
            **data,
        }
    except Exception as e:
        print(f"Metadata Error: {e}")
        return None
    finally:
        if conn:
            conn.close()


def normalize_metadata_update_time(raw_value):
    value = (raw_value or "").strip()
    if not value:
        return DEFAULT_METADATA_UPDATE_TIME
    try:
        parsed = datetime.strptime(value, "%H:%M")
        return parsed.strftime("%H:%M")
    except ValueError:
        return DEFAULT_METADATA_UPDATE_TIME


def should_run_metadata_update_now(config, now=None):
    now = now or datetime.now()
    scheduled_time = normalize_metadata_update_time(config.get("metadata_auto_update_time"))
    scheduled_hour, scheduled_minute = map(int, scheduled_time.split(":"))
    last_run = config.get("metadata_auto_last_run")

    if last_run == now.date().isoformat():
        return False

    current_minutes = now.hour * 60 + now.minute
    scheduled_minutes = scheduled_hour * 60 + scheduled_minute
    return current_minutes >= scheduled_minutes


def normalize_batch_label(batch_label):
    value = (batch_label or "").strip()
    if value in BATCH_OPTIONS:
        return value
    return ""


def normalize_problem_type(problem_type):
    value = (problem_type or "").strip()
    return PROBLEM_TYPE_ALIASES.get(value, value)


def extract_audible_link(metadata):
    if not metadata:
        return ""

    prioritized_values = []
    fallback_values = []

    for key, value in metadata.items():
        if value is None:
            continue

        text = str(value).strip()
        if not text or not text.lower().startswith(("http://", "https://")):
            continue

        lowered_key = str(key).lower()
        lowered_value = text.lower()
        if "audible" not in lowered_value:
            continue

        if "audible" in lowered_key and any(token in lowered_key for token in ("link", "url", "href")):
            prioritized_values.append(text)
        else:
            fallback_values.append(text)

    return prioritized_values[0] if prioritized_values else (fallback_values[0] if fallback_values else "")


def refresh_ticket_audible_links(conn, only_missing=False):
    query = "SELECT id, isbn, audible_url FROM tickets"
    rows = conn.execute(query).fetchall()
    for row in rows:
        if only_missing and (row["audible_url"] or "").strip():
            continue

        meta = fetch_metadata_by_isbn(row["isbn"])
        audible_url = extract_audible_link(meta)
        conn.execute("UPDATE tickets SET audible_url = ? WHERE id = ?", (audible_url, row["id"]))


def check_and_update_schema():
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(tickets)")
        columns = [info[1] for info in cursor.fetchall()]

        ticket_columns = {
            "created_by": "ALTER TABLE tickets ADD COLUMN created_by TEXT",
            "affected_portals": "ALTER TABLE tickets ADD COLUMN affected_portals TEXT",
            "author": "ALTER TABLE tickets ADD COLUMN author TEXT",
            "updated_at": "ALTER TABLE tickets ADD COLUMN updated_at TEXT",
            "last_comment_at": "ALTER TABLE tickets ADD COLUMN last_comment_at TEXT",
            "assigned_to": "ALTER TABLE tickets ADD COLUMN assigned_to TEXT",
            "batch_label": "ALTER TABLE tickets ADD COLUMN batch_label TEXT",
            "last_reminder_sent_at": "ALTER TABLE tickets ADD COLUMN last_reminder_sent_at TEXT",
            "last_reminder_status": "ALTER TABLE tickets ADD COLUMN last_reminder_status TEXT",
            "audible_url": "ALTER TABLE tickets ADD COLUMN audible_url TEXT",
        }

        for column_name, statement in ticket_columns.items():
            if column_name not in columns:
                cursor.execute(statement)

        if "author" not in columns:
            cursor.execute("SELECT id, isbn FROM tickets")
            for row in cursor.fetchall():
                meta = fetch_metadata_by_isbn(row[1])
                if meta and meta.get("Autor"):
                    cursor.execute("UPDATE tickets SET author = ? WHERE id = ?", (meta["Autor"], row[0]))

        if "audible_url" not in columns:
            refresh_ticket_audible_links(conn, only_missing=False)
        else:
            refresh_ticket_audible_links(conn, only_missing=True)

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS ticket_comments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticket_id INTEGER NOT NULL,
                author TEXT NOT NULL,
                comment TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(ticket_id) REFERENCES tickets(id) ON DELETE CASCADE
            )
            """
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_ticket_comments_ticket_id ON ticket_comments (ticket_id)")
        conn.commit()
    except Exception as e:
        print(f"Fehler bei Schema-Update: {e}")
    finally:
        if conn:
            conn.close()


check_and_update_schema()


@app.context_processor
def inject_layout_defaults():
    return {
        "today": datetime.now().strftime("%Y"),
        "team_members": TEAM_MEMBERS,
        "batch_options": BATCH_OPTIONS,
    }


@app.template_filter("format_comments")
def format_comments(text):
    if not text:
        return ""
    return Markup(str(escape(text)).replace("\n", "<br>"))


def compute_ticket_batch(ticket):
    manual_batch = normalize_batch_label(ticket.get("batch_label"))
    if manual_batch:
        return manual_batch

    status = ticket.get("status")
    deadline = ticket.get("deadline")
    if status != "offen" or not deadline:
        return ""

    try:
        deadline_date = datetime.strptime(deadline, "%Y-%m-%d").date()
    except ValueError:
        return ""

    if deadline_date < datetime.now().date():
        return "Aktion erforderlich"
    return ""


def enrich_ticket(ticket, comment_count=None):
    data = dict(ticket)
    data["problem_type"] = normalize_problem_type(data.get("problem_type"))
    data["effective_batch"] = compute_ticket_batch(data)
    data["comment_count"] = comment_count if comment_count is not None else 0
    data["audible_url"] = (data.get("audible_url") or "").strip()
    return data


def add_ticket_comment(conn, ticket_id, author, comment):
    cleaned_comment = (comment or "").strip()
    if not cleaned_comment:
        return False

    created_at = datetime.now().isoformat(timespec="seconds")
    conn.execute(
        """
        INSERT INTO ticket_comments (ticket_id, author, comment, created_at)
        VALUES (?, ?, ?, ?)
        """,
        (ticket_id, author, cleaned_comment, created_at),
    )
    conn.execute(
        "UPDATE tickets SET last_comment_at = ?, updated_at = ? WHERE id = ?",
        (created_at, created_at, ticket_id),
    )
    return True


def get_ticket_comments(ticket_id):
    conn = get_db_connection()
    try:
        rows = conn.execute(
            """
            SELECT id, ticket_id, author, comment, created_at
            FROM ticket_comments
            WHERE ticket_id = ?
            ORDER BY created_at DESC, id DESC
            """,
            (ticket_id,),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def get_comment_counts(conn):
    rows = conn.execute(
        """
        SELECT ticket_id, COUNT(*) AS comment_count
        FROM ticket_comments
        GROUP BY ticket_id
        """
    ).fetchall()
    return {row["ticket_id"]: row["comment_count"] for row in rows}


def send_teams_card(title, facts, text, color="d21e40"):
    config = load_config()
    webhook_url = config.get("teams_webhook")
    if not webhook_url:
        return False

    def _send():
        try:
            payload = {
                "@type": "MessageCard",
                "@context": "http://schema.org/extensions",
                "themeColor": color,
                "summary": title,
                "sections": [
                    {
                        "activityTitle": title,
                        "facts": facts,
                        "markdown": True,
                        "text": text,
                    }
                ],
            }
            requests.post(webhook_url, json=payload, timeout=5)
        except Exception as e:
            print(f"Fehler beim Senden an Teams: {e}")

    threading.Thread(target=_send, daemon=True).start()
    return True


def send_teams_notification(ticket_data):
    send_teams_card(
        title=f"Neues Ticket: {ticket_data['title']}",
        facts=[
            {"name": "ISBN:", "value": ticket_data["isbn"]},
            {"name": "Autor:", "value": ticket_data["author"] or "-"},
            {"name": "Problem:", "value": ticket_data["problem_type"]},
            {"name": "Portale:", "value": ticket_data["affected_portals"] or "-"},
            {"name": "Verantwortlich:", "value": ticket_data["assigned_to"] or "-"},
            {"name": "Deadline:", "value": ticket_data["deadline"].strftime("%d.%m.%Y")},
        ],
        text=f"**Beschreibung:**\n{ticket_data['description'] or '-'}",
    )


def send_due_ticket_reminders():
    conn = get_db_connection()
    try:
        today = datetime.now().date()
        today_iso = today.isoformat()
        tickets = conn.execute(
            """
            SELECT *
            FROM tickets
            WHERE status = 'offen' AND deadline IS NOT NULL
            """
        ).fetchall()

        for row in tickets:
            ticket = dict(row)
            try:
                deadline_date = datetime.strptime(ticket["deadline"], "%Y-%m-%d").date()
            except (TypeError, ValueError):
                continue

            reminder_status = None
            if deadline_date < today:
                reminder_status = "overdue"
            elif deadline_date <= today + timedelta(days=1):
                reminder_status = "due_soon"

            if not reminder_status:
                continue

            if ticket.get("last_reminder_sent_at") == today_iso and ticket.get("last_reminder_status") == reminder_status:
                continue

            title = (
                f"Ticket ueberfaellig: {ticket.get('title') or ticket.get('isbn')}"
                if reminder_status == "overdue"
                else f"Ticket kurz vor Deadline: {ticket.get('title') or ticket.get('isbn')}"
            )
            facts = [
                {"name": "Ticket:", "value": f"#{ticket['id']}"},
                {"name": "ISBN:", "value": ticket.get("isbn") or "-"},
                {"name": "Problem:", "value": ticket.get("problem_type") or "-"},
                {"name": "Verantwortlich:", "value": ticket.get("assigned_to") or "-"},
                {"name": "Deadline:", "value": ticket.get("deadline") or "-"},
                {"name": "Batch:", "value": compute_ticket_batch(ticket) or "-"},
            ]
            body = ticket.get("description") or "Keine Beschreibung hinterlegt."
            sent = send_teams_card(title, facts, body, color="ff9f1c" if reminder_status == "due_soon" else "d21e40")
            if sent:
                conn.execute(
                    """
                    UPDATE tickets
                    SET last_reminder_sent_at = ?, last_reminder_status = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (today_iso, reminder_status, datetime.now().isoformat(timespec="seconds"), ticket["id"]),
                )

        conn.commit()
    finally:
        conn.close()


def update_ticket_metadata_refresh(force=False):
    config = load_config()
    auto_enabled = config.get("metadata_auto_update_enabled", True)
    if not auto_enabled and not force:
        return False, "Automatisches Metadaten-Update ist deaktiviert."

    config["metadata_auto_update_time"] = normalize_metadata_update_time(config.get("metadata_auto_update_time"))
    now = datetime.now()
    today = now.date().isoformat()
    if not force and not should_run_metadata_update_now(config, now):
        return False, "Automatisches Metadaten-Update ist fuer heute noch nicht faellig."

    success, message = metadaten_update.fetch_and_update()
    if success:
        conn = get_db_connection()
        try:
            refresh_ticket_audible_links(conn, only_missing=False)
            conn.commit()
        finally:
            conn.close()

    config["metadata_auto_last_run"] = today
    config["metadata_auto_last_run_at"] = now.strftime("%d.%m.%Y %H:%M")
    config["metadata_auto_last_success"] = success
    config["metadata_auto_last_message"] = message
    save_config(config)

    METADATA_AUTO_UPDATE_STATE["last_run_at"] = config["metadata_auto_last_run_at"]
    METADATA_AUTO_UPDATE_STATE["last_success"] = success
    METADATA_AUTO_UPDATE_STATE["last_message"] = message
    return success, message


def background_loop():
    while True:
        try:
            update_ticket_metadata_refresh(force=False)
            send_due_ticket_reminders()
        except Exception as e:
            print(f"Fehler im Hintergrund-Loop: {e}")
        time.sleep(BACKGROUND_INTERVAL_SECONDS)


def start_background_worker():
    worker = threading.Thread(target=background_loop, name="ticket-background-worker", daemon=True)
    worker.start()


start_background_worker()


def generate_mail_content(ticket):
    title = ticket["title"] or "Unknown Title"
    isbn = ticket["isbn"] or "Unknown ISBN"
    ptype = ticket["problem_type"]
    init_date = ticket["initial_contact_date"]
    date_str = init_date if isinstance(init_date, str) else (init_date.strftime("%d.%m.%Y") if init_date else "N/A")

    ptype = normalize_problem_type(ptype)

    if ptype == "Preisaenderung":
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

        flash("Ungueltiges Passwort.", "error")

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
        problem_type = normalize_problem_type(request.form.get("problem_type"))
        description = (request.form.get("description") or "").strip()
        contact_date = request.form.get("initial_contact_date")
        created_by = request.form.get("created_by")
        assigned_to = request.form.get("assigned_to")
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
        audible_url = ""
        meta = fetch_metadata_by_isbn(isbn)
        if meta:
            title = meta.get("Titel", title)
            author = meta.get("Autor", author)
            audible_url = extract_audible_link(meta)

        deadline = creation_date + timedelta(days=5 if problem_type == "Titel nicht online" else 14)
        now_iso = datetime.now().isoformat(timespec="seconds")

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
                            f"STOP: Ein offenes Ticket fuer diese ISBN und das Problem '{problem_type}' existiert bereits fuer: {', '.join(sorted(intersection))}.",
                            "error",
                        )
                        return redirect(url_for("new_ticket"))

            conn.execute(
                """
                INSERT INTO tickets
                (creation_date, problem_type, description, isbn, deadline, initial_contact_date, title, status,
                 created_by, affected_portals, author, updated_at, assigned_to, batch_label, audible_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    assigned_to,
                    "",
                    audible_url,
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
                    "assigned_to": assigned_to,
                    "deadline": deadline,
                    "description": description,
                }
            )
            flash(f"Ticket fuer '{title}' erfolgreich angelegt.", "success")
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


def render_dashboard(admin_mode):
    sort_order = request.args.get("sort", "asc")
    order_clause = "ORDER BY deadline DESC, id DESC" if sort_order == "desc" else "ORDER BY deadline ASC, id ASC"
    conn = get_db_connection()
    try:
        comment_counts = get_comment_counts(conn)
        if request.path == "/archive":
            cutoff_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
            rows = conn.execute(
                f"""
                SELECT *
                FROM tickets
                WHERE status = 'erledigt' AND completion_date >= ?
                ORDER BY completion_date DESC, id DESC
                """,
                (cutoff_date,),
            ).fetchall()
            view_mode = "archive"
        else:
            rows = conn.execute(f'SELECT * FROM tickets WHERE status = "offen" {order_clause}').fetchall()
            view_mode = "open"

        tickets = [enrich_ticket(row, comment_counts.get(row["id"], 0)) for row in rows]
    except sqlite3.OperationalError as e:
        flash(f"Datenbankfehler: {e}", "error")
        tickets = []
        view_mode = "open"
    finally:
        conn.close()

    return render_template(
        "dashboard.html",
        tickets=tickets,
        view_mode=view_mode,
        now=datetime.now().strftime("%Y-%m-%d"),
        sort_order=sort_order,
        admin_mode=admin_mode,
    )


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


@app.route("/archive")
@requires_auth
def archive():
    return render_dashboard(admin_mode=bool(session.get("admin_mode")))


@app.route("/exit_admin", methods=["POST"])
@requires_auth
def exit_admin():
    session.pop("admin_mode", None)
    return redirect(url_for("dashboard"))


@app.route("/ticket/<int:id>/complete", methods=["POST"])
@requires_auth
def complete_ticket(id):
    conn = get_db_connection()
    try:
        conn.execute(
            """
            UPDATE tickets
            SET status = ?, completion_date = ?, updated_at = ?
            WHERE id = ?
            """,
            ("erledigt", datetime.now().date().isoformat(), datetime.now().isoformat(timespec="seconds"), id),
        )
        conn.commit()
    finally:
        conn.close()
    flash("Ticket als erledigt markiert.", "success")
    return redirect(url_for("dashboard"))


@app.route("/ticket/<int:id>/reopen", methods=["POST"])
@requires_auth
def reopen_ticket(id):
    conn = get_db_connection()
    try:
        conn.execute(
            """
            UPDATE tickets
            SET status = ?, completion_date = NULL, deadline = ?, updated_at = ?, batch_label = ''
            WHERE id = ?
            """,
            ("offen", (datetime.now().date() + timedelta(days=5)).isoformat(), datetime.now().isoformat(timespec="seconds"), id),
        )
        conn.commit()
    finally:
        conn.close()
    flash("Ticket wiedereroeffnet und Deadline um 5 Tage verlaengert.", "success")
    return redirect(url_for("archive"))


@app.route("/ticket/<int:id>/delete", methods=["POST"])
@requires_admin_mode
def delete_ticket(id):
    conn = get_db_connection()
    try:
        conn.execute("DELETE FROM tickets WHERE id = ?", (id,))
        conn.execute("DELETE FROM ticket_comments WHERE ticket_id = ?", (id,))
        conn.commit()
    finally:
        conn.close()
    flash("Ticket geloescht.", "info")
    return redirect(url_for("dashboard"))


@app.route("/tickets/batch/stuck", methods=["POST"])
@requires_admin_mode
def bulk_mark_stuck():
    conn = get_db_connection()
    try:
        conn.execute(
            """
            UPDATE tickets
            SET batch_label = 'Stuck', updated_at = ?
            WHERE status = 'offen'
            """,
            (datetime.now().isoformat(timespec="seconds"),),
        )
        conn.commit()
    finally:
        conn.close()

    flash("Alle aktuell sichtbaren offenen Tickets wurden auf 'Stuck' gesetzt.", "success")
    if session.get("admin_mode"):
        return redirect(url_for("admin_dashboard"))
    return redirect(url_for("dashboard"))


@app.route("/ticket/<int:id>/edit", methods=["GET", "POST"])
@requires_admin_mode
def edit_ticket(id):
    conn = get_db_connection()

    if request.method == "POST":
        existing_ticket = conn.execute("SELECT * FROM tickets WHERE id = ?", (id,)).fetchone()
        if not existing_ticket:
            conn.close()
            flash("Ticket nicht gefunden.", "error")
            return redirect(url_for("dashboard"))

        portals_list = request.form.getlist("portals")
        new_status = request.form.get("status")
        new_deadline = request.form.get("deadline")
        assigned_to = request.form.get("assigned_to")
        batch_label = normalize_batch_label(request.form.get("batch_label"))
        problem_type = normalize_problem_type(request.form.get("problem_type"))
        comment_author = request.form.get("comment_author") or assigned_to or session.get("user_name") or "Admin"
        comment_added = False
        isbn = (request.form.get("isbn") or "").strip()
        meta = fetch_metadata_by_isbn(isbn) if isbn else None
        audible_url = extract_audible_link(meta)

        skip_deadline_extension = bool((assigned_to or "").strip()) and batch_label == "Stuck"
        deadline_auto_extended = new_status == "offen" and not skip_deadline_extension
        if deadline_auto_extended:
            new_deadline = (datetime.now().date() + timedelta(days=5)).isoformat()

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
                    updated_at = ?, assigned_to = ?, batch_label = ?, audible_url = ?
                WHERE id = ?
                """,
                (
                    request.form.get("title"),
                    request.form.get("author"),
                    isbn,
                    new_status,
                    problem_type,
                    (request.form.get("description") or "").strip(),
                    new_deadline,
                    request.form.get("initial_contact_date"),
                    ", ".join(portals_list) if portals_list else "",
                    completion_date,
                    datetime.now().isoformat(timespec="seconds"),
                    assigned_to,
                    batch_label,
                    audible_url,
                    id,
                ),
            )

            if request.form.get("admin_comment", "").strip():
                comment_added = add_ticket_comment(conn, id, comment_author, request.form.get("admin_comment"))

            conn.commit()
            if comment_added and deadline_auto_extended:
                flash("Ticket aktualisiert, Kommentar gespeichert und Deadline um 5 Tage verlaengert.", "success")
            elif comment_added:
                flash("Ticket aktualisiert und Kommentar gespeichert.", "success")
            elif deadline_auto_extended:
                flash("Ticket aktualisiert und Deadline um 5 Tage verlaengert.", "success")
            else:
                flash("Ticket erfolgreich aktualisiert.", "success")
        except Exception as e:
            flash(f"Fehler beim Speichern: {e}", "error")
        finally:
            conn.close()

        return redirect(url_for("admin_dashboard"))

    ticket = conn.execute("SELECT * FROM tickets WHERE id = ?", (id,)).fetchone()
    conn.close()
    if not ticket:
        flash("Ticket nicht gefunden.", "error")
        return redirect(url_for("dashboard"))

    ticket_data = enrich_ticket(ticket, len(get_ticket_comments(id)))
    comments = get_ticket_comments(id)
    mail_subject, mail_body = generate_mail_content(ticket_data)
    config = load_config()

    return render_template(
        "edit_ticket.html",
        ticket=ticket_data,
        comments=comments,
        mail_subject=mail_subject,
        mail_body=mail_body,
        auto_update_enabled=config.get("metadata_auto_update_enabled", True),
        auto_update_message=config.get("metadata_auto_last_message") or METADATA_AUTO_UPDATE_STATE["last_message"],
        auto_update_last_run=config.get("metadata_auto_last_run_at") or METADATA_AUTO_UPDATE_STATE["last_run_at"],
        auto_update_time=normalize_metadata_update_time(config.get("metadata_auto_update_time")),
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
        config["metadata_auto_update_time"] = normalize_metadata_update_time(request.form.get("metadata_auto_update_time"))

        if save_config(config):
            flash("Einstellungen erfolgreich gespeichert.", "success")
        else:
            flash("Fehler beim Speichern der Einstellungen.", "error")
        return redirect(url_for("settings"))

    config["metadata_auto_update_time"] = normalize_metadata_update_time(config.get("metadata_auto_update_time"))
    return render_template("settings.html", config=config, auto_update_state=METADATA_AUTO_UPDATE_STATE)


@app.route("/settings/update_metadata", methods=["POST"])
@requires_admin_mode
def update_metadata():
    success, message = update_ticket_metadata_refresh(force=True)
    flash(message, "success" if success else "error")
    return redirect(url_for("settings"))


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=5000)
