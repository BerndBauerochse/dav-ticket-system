import os
import sqlite3
import json
from datetime import datetime, timedelta
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, Response, session
from functools import wraps

import requests
import threading
import re
from markupsafe import Markup, escape
from werkzeug.security import check_password_hash, generate_password_hash
import metadaten_update

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'change-this-in-production')

DATA_DIR = os.environ.get('DATA_DIR', '/data')
CONFIG_PATH = os.path.join(DATA_DIR, 'config.json')

# --- AUTHENTICATION ---
def check_auth(password):
    config = load_config()
    pwd_hash = config.get('admin_password_hash', '')
    if not pwd_hash:
        return False
    return check_password_hash(pwd_hash, password)

@app.before_request
def require_login():
    if request.endpoint in ['login', 'static']:
        return
    if not session.get('logged_in'):
        return redirect(url_for('login', next=request.url))

def requires_auth(f):
    return f


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        password = request.form.get('password')
        next_url = request.args.get('next') or url_for('dashboard')
        if check_auth(password):
            session['logged_in'] = True
            flash('Erfolgreich eingeloggt.', 'success')
            return redirect(next_url)
        else:
            flash('Ungültiges Passwort.', 'error')
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.pop('logged_in', None)
    flash('Erfolgreich ausgeloggt.', 'info')
    return redirect(url_for('index'))

def load_config():
    try:
        with open(CONFIG_PATH, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Fehler beim Laden der Config: {e}")
        return {}

def save_config(config):
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(CONFIG_PATH, 'w') as f:
            json.dump(config, f, indent=4)
        return True
    except Exception as e:
        print(f"Fehler beim Speichern der Config: {e}")
        return False

def get_db_path():
    config = load_config()
    cfg_path = config.get('database_path', os.path.join(DATA_DIR, 'tickets.db'))
    if os.path.isabs(cfg_path):
        return cfg_path
    return os.path.join(DATA_DIR, cfg_path)

def get_metadata_path():
    config = load_config()
    folder = config.get('metadata_folder', os.path.join(DATA_DIR, 'Titeldaten'))
    if not os.path.isabs(folder):
        folder = os.path.join(DATA_DIR, folder)
    return os.path.join(folder, 'metadata.db')

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

def fetch_metadata_by_isbn(isbn):
    try:
        conn = get_metadata_connection()
        if not conn:
            return None
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM metadata LIMIT 1")
        columns = [description[0] for description in cursor.description]
        isbn_col = next((c for c in columns if 'ean' in c.lower() or 'isbn' in c.lower()), None)
        title_col = next((c for c in columns if 'titel' in c.lower() or 'title' in c.lower()), None)
        author_col = next((c for c in columns if 'autor' in c.lower() or 'author' in c.lower() or 'artist' in c.lower()), None)
        cover_col = next((c for c in columns if 'cover' in c.lower() or 'bild' in c.lower() or 'image' in c.lower()), None)
        if not isbn_col:
            isbn_col = "EAN digital"
        cursor.execute(f'SELECT * FROM metadata WHERE "{isbn_col}" = ?', (str(isbn),))
        row = cursor.fetchone()
        conn.close()
        if row:
            data = dict(row)
            return {
                'Titel': data.get(title_col, 'Unbekannt') if title_col else 'Unbekannt',
                'Autor': data.get(author_col, '') if author_col else '',
                'Cover URL': data.get(cover_col, '') if cover_col else '',
                **data
            }
    except Exception as e:
        print(f"Metadata Error: {e}")
    return None

def check_and_update_schema():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(tickets)")
        columns = [info[1] for info in cursor.fetchall()]
        if 'created_by' not in columns:
            cursor.execute('ALTER TABLE tickets ADD COLUMN created_by TEXT')
        if 'affected_portals' not in columns:
            cursor.execute('ALTER TABLE tickets ADD COLUMN affected_portals TEXT')
        if 'author' not in columns:
            cursor.execute('ALTER TABLE tickets ADD COLUMN author TEXT')
            cursor.execute("SELECT id, isbn FROM tickets")
            for row in cursor.fetchall():
                meta = fetch_metadata_by_isbn(row[1])
                if meta and 'Autor' in meta:
                    cursor.execute("UPDATE tickets SET author = ? WHERE id = ?", (meta['Autor'], row[0]))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Fehler bei Schema-Update: {e}")

check_and_update_schema()

@app.template_filter('format_comments')
def format_comments(text):
    if not text:
        return ""
    escaped = str(escape(text))
    pattern = r"(\[Kommentar Admin - \d{2}\.\d{2}\.\d{4}\]:)"
    def replace(match):
        return f'<br><span class="text-audible-accent font-bold block mt-1 mb-0.5 border-l-2 border-audible-accent pl-2">{match.group(1)}</span>'
    return Markup(re.sub(pattern, replace, escaped))

def send_teams_notification(ticket_data):
    config = load_config()
    webhook_url = config.get('teams_webhook')
    if not webhook_url:
        return
    def _send():
        try:
            payload = {
                "@type": "MessageCard",
                "@context": "http://schema.org/extensions",
                "themeColor": "d21e40",
                "summary": "Neues Ticket angelegt",
                "sections": [{
                    "activityTitle": f"🎫 Neues Ticket: {ticket_data['title']}",
                    "activitySubtitle": f"Erstellt von {ticket_data['created_by']}",
                    "facts": [
                        {"name": "ISBN:", "value": ticket_data['isbn']},
                        {"name": "Autor:", "value": ticket_data['author']},
                        {"name": "Problem:", "value": ticket_data['problem_type']},
                        {"name": "Portale:", "value": ticket_data['affected_portals']},
                        {"name": "Deadline:", "value": ticket_data['deadline'].strftime('%d.%m.%Y')}
                    ],
                    "markdown": True,
                    "text": f"**Beschreibung:**\n{ticket_data['description']}"
                }],
                "potentialAction": [{
                    "@type": "OpenUri",
                    "name": "Zum Ticket-Dashboard",
                    "targets": [{"os": "default", "uri": f"{config.get('base_url', 'http://localhost:5000')}/dashboard"}]
                }]
            }
            requests.post(webhook_url, json=payload, timeout=5)
        except Exception as e:
            print(f"Fehler beim Senden an Teams: {e}")
    threading.Thread(target=_send).start()

@app.route('/')
def index():
    return render_template('landing.html')

@app.route('/new-ticket', methods=['GET', 'POST'])
def new_ticket():
    if request.method == 'POST':
        isbn = request.form.get('isbn')
        problem_type = request.form.get('problem_type')
        description = request.form.get('description')
        contact_date = request.form.get('initial_contact_date')
        created_by = request.form.get('created_by')
        portals_list = request.form.getlist('portals')
        affected_portals = ", ".join(portals_list) if portals_list else ""
        if not isbn:
            flash("Bitte ISBN angeben", "error")
            return redirect(url_for('new_ticket'))
        creation_date = datetime.now().date()
        try:
            contact_date_obj = datetime.strptime(contact_date, '%Y-%m-%d').date() if contact_date else creation_date
        except ValueError:
            contact_date_obj = creation_date
        title = "Unbekannter Titel"
        author = ""
        meta = fetch_metadata_by_isbn(isbn)
        if meta:
            title = meta.get('Titel', title)
            author = meta.get('Autor', author)
        deadline = creation_date + timedelta(days=5 if problem_type == "Titel nicht online" else 14)
        try:
            conn = get_db_connection()
            existing = conn.execute(
                "SELECT affected_portals FROM tickets WHERE isbn = ? AND problem_type = ? AND status = 'offen'",
                (isbn, problem_type)
            ).fetchall()
            if existing:
                new_portals_set = set(portals_list)
                for row in existing:
                    existing_set = set(p.strip() for p in (row[0] or "").split(',') if p.strip())
                    intersection = new_portals_set.intersection(existing_set)
                    if intersection:
                        conn.close()
                        flash(f"STOP: Ein offenes Ticket für diese ISBN und das Problem '{problem_type}' existiert bereits für: {', '.join(intersection)}. Bitte kein doppeltes Ticket anlegen!", "error")
                        return redirect(url_for('new_ticket'))
            conn.execute('''
                INSERT INTO tickets
                (creation_date, problem_type, description, isbn, deadline, initial_contact_date, title, status, created_by, affected_portals, author)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (creation_date, problem_type, description, isbn, deadline, contact_date_obj, title, "offen", created_by, affected_portals, author))
            conn.commit()
            conn.close()
            send_teams_notification({
                'title': title, 'author': author, 'created_by': created_by,
                'isbn': isbn, 'problem_type': problem_type,
                'affected_portals': affected_portals, 'deadline': deadline, 'description': description
            })
            flash(f"Ticket für '{title}' erfolgreich angelegt!", "success")
            return redirect(url_for('new_ticket'))
        except Exception as e:
            flash(f"Fehler beim Speichern: {str(e)}", "error")
    return render_template('creator.html', today=datetime.now().strftime('%Y-%m-%d'))

@app.route('/api/metadata/<isbn>')
def api_metadata(isbn):
    meta = fetch_metadata_by_isbn(isbn)
    if meta:
        return jsonify({'found': True, 'title': meta.get('Titel', 'Unbekannt'), 'author': meta.get('Autor', ''), 'cover_url': meta.get('Cover URL', '')})
    return jsonify({'found': False})

@app.route('/dashboard')
@requires_auth
def dashboard():
    if session.get('admin_mode'):
        return redirect(url_for('admin_dashboard'))
    return render_dashboard(admin_mode=False)

@app.route('/admin')
@requires_auth
def admin_dashboard():
    session['admin_mode'] = True
    return render_dashboard(admin_mode=True)

@app.route('/exit_admin')
def exit_admin():
    session.pop('admin_mode', None)
    return redirect(url_for('dashboard'))

def render_dashboard(admin_mode):
    sort_order = request.args.get('sort', 'asc')
    order_clause = 'ORDER BY deadline DESC' if sort_order == 'desc' else 'ORDER BY deadline ASC'
    conn = get_db_connection()
    try:
        tickets = conn.execute(f'SELECT * FROM tickets WHERE status = "offen" {order_clause}').fetchall()
    except sqlite3.OperationalError as e:
        flash(f"Datenbankfehler: {e}", "error")
        tickets = []
    finally:
        conn.close()
    return render_template('dashboard.html', tickets=tickets, view_mode='open',
                           now=datetime.now().strftime('%Y-%m-%d'), sort_order=sort_order, admin_mode=admin_mode)

@app.route('/archive')
@requires_auth
def archive():
    conn = get_db_connection()
    try:
        cutoff_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
        tickets = conn.execute(
            'SELECT * FROM tickets WHERE status = "erledigt" AND completion_date >= ? ORDER BY completion_date DESC, id DESC',
            (cutoff_date,)
        ).fetchall()
    except sqlite3.OperationalError as e:
        flash(f"Datenbankfehler: {e}", "error")
        tickets = []
    finally:
        conn.close()
    return render_template('dashboard.html', tickets=tickets, view_mode='archive', now=datetime.now().strftime('%Y-%m-%d'))

@app.route('/ticket/<int:id>/complete', methods=['POST'])
def complete_ticket(id):
    conn = get_db_connection()
    conn.execute('UPDATE tickets SET status = ?, completion_date = ? WHERE id = ?', ('erledigt', datetime.now().date(), id))
    conn.commit()
    conn.close()
    flash("Ticket als erledigt markiert.", "success")
    return redirect(url_for('dashboard'))

@app.route('/ticket/<int:id>/reopen', methods=['POST'])
def reopen_ticket(id):
    conn = get_db_connection()
    conn.execute('UPDATE tickets SET status = ?, completion_date = NULL WHERE id = ?', ('offen', id))
    conn.commit()
    conn.close()
    flash("Ticket wiedereröffnet.", "success")
    return redirect(url_for('archive'))

@app.route('/ticket/<int:id>/delete', methods=['POST'])
@requires_auth
def delete_ticket(id):
    conn = get_db_connection()
    conn.execute('DELETE FROM tickets WHERE id = ?', (id,))
    conn.commit()
    conn.close()
    flash("Ticket gelöscht.", "info")
    return redirect(url_for('dashboard'))

def generate_mail_content(ticket):
    import urllib.parse
    title = ticket['title'] or "Unknown Title"
    isbn = ticket['isbn'] or "Unknown ISBN"
    ptype = ticket['problem_type']
    init_date = ticket['initial_contact_date']
    date_str = init_date if isinstance(init_date, str) else (init_date.strftime('%d.%m.%Y') if init_date else "N/A")
    if ptype == "Preisänderung":
        subject = f"[change request] price change - {title} - {isbn}"
        body = f"Dear Audible Team,\n\nOn {date_str}, I submitted a price change request for...\n\nISBN: {isbn}\nTitle: {title}\n\nPlease process immediately.\n\nBest regards,\nDoro"
    elif ptype == "Titel nicht online":
        subject = f"[title not online] - {title} - {isbn}"
        body = f"Dear Audible Team,\n\nThe title {title} ({isbn}) is still not online.\nRelease Date was supposed to be around {date_str}.\n\nPlease check.\n\nBest regards,\nDoro"
    else:
        subject = f"Audible Issue: {ptype} - {title}"
        body = f"Hello Audible Team,\n\nWe have an issue regarding:\nTitle: {title}\nISBN: {isbn}\n\nProblem: {ptype}\nReference Date: {date_str}\n\nPlease check.\n\nBest regards,\nDAV Digital Team"
    return urllib.parse.quote(subject), urllib.parse.quote(body)

@app.route('/ticket/<int:id>/edit', methods=['GET', 'POST'])
@requires_auth
def edit_ticket(id):
    conn = get_db_connection()
    if request.method == 'POST':
        portals_list = request.form.getlist('portals')
        try:
            conn.execute('''
                UPDATE tickets
                SET title=?, author=?, isbn=?, status=?, problem_type=?, description=?,
                    deadline=?, initial_contact_date=?, affected_portals=?
                WHERE id=?
            ''', (
                request.form.get('title'), request.form.get('author'), request.form.get('isbn'),
                request.form.get('status'), request.form.get('problem_type'), request.form.get('description'),
                request.form.get('deadline'), request.form.get('initial_contact_date'),
                ", ".join(portals_list) if portals_list else "", id
            ))
            conn.commit()
            flash("Ticket erfolgreich aktualisiert.", "success")
        except Exception as e:
            flash(f"Fehler beim Speichern: {e}", "error")
        finally:
            conn.close()
        return redirect(url_for('admin_dashboard') if session.get('admin_mode') else url_for('dashboard'))
    ticket = conn.execute('SELECT * FROM tickets WHERE id = ?', (id,)).fetchone()
    conn.close()
    if not ticket:
        flash("Ticket nicht gefunden.", "error")
        return redirect(url_for('dashboard'))
    t_dict = dict(ticket)
    subj, body = generate_mail_content(t_dict)
    config = load_config()
    nocodb_active = bool(config.get('nocodb_url') and config.get('nocodb_token'))
    return render_template('edit_ticket.html', ticket=t_dict, mail_subject=subj, mail_body=body, nocodb_active=nocodb_active)

@app.route('/settings', methods=['GET', 'POST'])
@requires_auth
def settings():
    if not session.get('admin_mode'):
        flash("Zugriff verweigert. Nur für Administratoren.", "error")
        return redirect(url_for('dashboard'))
    config = load_config()
    if request.method == 'POST':
        for key in ['database_path', 'metadata_folder', 'teams_webhook', 'metadata_webhook_url']:
            val = request.form.get(key)
            if val is not None:
                config[key] = val.strip()
        if save_config(config):
            flash("Einstellungen erfolgreich gespeichert.", "success")
        else:
            flash("Fehler beim Speichern der Einstellungen.", "error")
        return redirect(url_for('settings'))
    return render_template('settings.html', config=config)

@app.route('/settings/update_metadata', methods=['POST'])
@requires_auth
def update_metadata():
    if not session.get('admin_mode'):
        flash("Zugriff verweigert.", "error")
        return redirect(url_for('dashboard'))
    success, message = metadaten_update.fetch_and_update()
    flash(message, "success" if success else "error")
    return redirect(url_for('settings'))

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5000)
