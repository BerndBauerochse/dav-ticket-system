import requests
import sqlite3
import json
import os
from datetime import datetime

DATA_DIR = os.environ.get('DATA_DIR', '/data')
CONFIG_PATH = os.path.join(DATA_DIR, 'config.json')

def load_config():
    try:
        with open(CONFIG_PATH, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Fehler beim Laden der Config: {e}")
        return {}

def fetch_and_update():
    """Importiert Metadaten vom Webhook. Gibt (True, 'Nachricht') oder (False, 'Fehler') zurück."""
    config = load_config()
    webhook_url = config.get('metadata_webhook_url', '')
    target_folder = config.get('metadata_folder', os.path.join(DATA_DIR, 'Titeldaten'))

    if not os.path.isabs(target_folder):
        target_folder = os.path.join(DATA_DIR, target_folder)

    db_path = os.path.join(target_folder, "metadata.db")

    if not webhook_url:
        return False, "Keine gültige Webhook URL konfiguriert."

    try:
        print(f"Rufe Webhook ab: {webhook_url}")
        response = requests.get(webhook_url, timeout=60)
        response.raise_for_status()

        try:
            json_response = response.json()
        except json.JSONDecodeError:
            return False, f"Ungültiges JSON erhalten (Status {response.status_code})"

        if isinstance(json_response, list):
            items = json_response
        elif "data" in json_response:
            items = json_response["data"]
        else:
            return False, "JSON hat kein 'data' Feld und ist keine Liste."

        if not items:
            return True, "Webhook lieferte leere Liste (keine Updates)."

        print(f"{len(items)} Datensätze empfangen.")

        os.makedirs(target_folder, exist_ok=True)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        columns = list(items[0].keys())
        safe_columns = [f'"{c}"' for c in columns]

        cursor.execute("DROP TABLE IF EXISTS metadata")
        cursor.execute(f"CREATE TABLE metadata ({', '.join([c + ' TEXT' for c in safe_columns])})")

        placeholders = ",".join(["?"] * len(columns))
        insert_query = f"INSERT INTO metadata ({', '.join(safe_columns)}) VALUES ({placeholders})"

        for item in items:
            cursor.execute(insert_query, [str(item.get(col, "")) for col in columns])

        conn.commit()

        for col in columns:
            if "ean" in col.lower() or "isbn" in col.lower():
                try:
                    cursor.execute(f'CREATE INDEX IF NOT EXISTS idx_{col} ON metadata ("{col}")')
                except Exception:
                    pass

        conn.close()
        return True, f"{len(items)} Titel erfolgreich importiert."

    except requests.exceptions.RequestException as e:
        return False, f"Netzwerkfehler: {str(e)}"
    except Exception as e:
        return False, f"Fehler: {str(e)}"

if __name__ == "__main__":
    success, msg = fetch_and_update()
    print(msg)
