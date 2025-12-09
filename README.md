# FIT to BigQuery ETL Pipeline

Ein Python-ETL-Pipeline zum Verarbeiten von FIT-Dateien (von Wahoo, Garmin, Zwift) und Hochladen zu Google BigQuery.

## Features

- **Hash-basierte Duplikaterkennung**: SHA-256 Hashing verhindert doppelte Verarbeitung
- **Batch-Upload**: Effizientes Hochladen in konfigurierbaren Batches (Standard: 1000 Zeilen)
- **Transaktionale Verarbeitung**: Details zuerst, dann Session-Daten mit Rollback bei Fehlern
- **Automatisches Archivieren**: Erfolgreich verarbeitete Dateien → `processed/`
- **Fehlerbehandlung**: Fehlerhafte Dateien → `failed/` mit detaillierten Logs
- **Umfassendes Logging**: Datei + Console Output mit konfigurierbarem Log-Level
- **BigQuery Optimierung**: Partitionierung nach Timestamp, Clustering nach Manufacturer/Sport
- **GPS-Konvertierung**: Automatische Umrechnung von Semicircles zu Dezimalgrad
- **Datetime-Serialisierung**: Sichere JSON-Konvertierung für BigQuery Upload
- **Windows-kompatibel**: UTF-8 Encoding, keine Unicode-Symbole in Logs

## Voraussetzungen

- Python 3.8+
- Google Cloud Service Account mit BigQuery-Berechtigungen
- FIT-Dateien von Fitness-Geräten

## Installation

1. **Repository klonen oder herunterladen**

2. **Virtuelle Umgebung erstellen (empfohlen)**

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

3. **Dependencies installieren**

```powershell
pip install --user -r requirements.txt
```

4. **Konfiguration einrichten**

Erstellen Sie eine `.env` Datei im Projektverzeichnis (Vorlage: `.env.example`):

```env
GOOGLE_APPLICATION_CREDENTIALS=path/to/your-service-account-key.json
BIGQUERY_PROJECT_ID=your-project-id
BIGQUERY_DATASET=fitness_data
INPUT_DIR=files
PROCESSED_DIR=processed
FAILED_DIR=failed
LOG_DIR=logs
BATCH_SIZE=1000
LOG_LEVEL=INFO
```

## BigQuery Schema

### Sessions Tabelle

Speichert Zusammenfassungen der Trainingseinheiten:

- **Metadaten**: `file_hash` (Primary Key), `filename`, `session_id` (UUID)
- **Zeitinformationen**: `start_time`, `timestamp`, `total_timer_time`, `total_elapsed_time`
- **Geräteinfo**: `manufacturer`, `product`, `serial_number`
- **Metriken**: `total_distance`, `avg_speed`, `max_speed`, `avg_cadence`, `max_cadence`
- **Herzfrequenz**: `min_heart_rate`, `avg_heart_rate`, `max_heart_rate`
- **Leistung**: `avg_power`, `max_power`, `normalized_power`, `threshold_power`, `total_work`
- **Höhendaten**: `min_altitude`, `avg_altitude`, `max_altitude`, `total_ascent`, `total_descent`
- **Steigung**: `avg_grade`, `max_pos_grade`, `max_neg_grade`
- **Kalorien**: `total_calories`
- **Training**: `training_stress_score`, `intensity_factor`, `sport`, `sub_sport`, `num_laps`

**Partitionierung**: Täglich nach `start_time`  
**Clustering**: Nach `manufacturer`, `sport`

### Details Tabelle

Speichert Zeitreihen-Datenpunkte (Sekunde für Sekunde):

- **Zuordnung**: `session_id` (Foreign Key), `file_hash`, `record_id` (eindeutig)
- **Zeit**: `timestamp`
- **GPS**: `position_lat`, `position_long` (Dezimalgrad), `gps_accuracy`
- **Höhe**: `altitude`, `enhanced_altitude`, `grade`
- **Bewegung**: `distance` (kumulativ), `speed`, `enhanced_speed`
- **Vitalwerte**: `heart_rate`, `cadence`, `power`
- **Umgebung**: `temperature`, `calories`, `battery_soc`

**Partitionierung**: Täglich nach `timestamp`  
**Clustering**: Nach `session_id`, `file_hash`

## Verwendung

### Setup ausführen (Optional)

Interaktive Konfiguration mit Setup-Wizard:

```powershell
python setup.py
```

### BigQuery-Verbindung testen

```powershell
python test_bigquery.py
```

Prüft:

- ✅ Konfiguration gültig
- ✅ Service Account Authentifizierung
- ✅ Dataset erreichbar
- ✅ Query-Fähigkeit

### FIT-Parser testen (ohne BigQuery)

```powershell
python test_parser.py "files\2024-05-09-095135-ELEMNT BOLT 3FFA-146-0.fit"
```

Zeigt:

- Session-Informationen (Sport, Dauer, Metriken)
- Anzahl Records
- GPS-Koordinaten
- Herzfrequenz, Leistung, etc.

### ETL Pipeline ausführen

```powershell
python run_etl.py
```

### Ablauf

1. **SETUP**: Initialisiert BigQuery

   - Erstellt Dataset (falls nicht vorhanden)
   - Erstellt Tables mit Schema (falls nicht vorhanden)
   - Verifiziert Partitionierung und Clustering

2. **EXTRACT**: Findet unverarbeitete Dateien

   - Scannt `files/` nach `*.fit` und `*.FIT` (ohne Duplikate)
   - Generiert SHA-256 Hash pro Datei
   - Prüft gegen BigQuery `sessions.file_hash`
   - Gibt Liste unverarbeiteter Dateien zurück

3. **TRANSFORM**: Parsed FIT-Dateien

   - Extrahiert Session-Daten mit `garmin-fit-sdk`
   - Extrahiert Record-Daten (Zeitreihen)
   - Konvertiert GPS: Semicircles → Dezimalgrad
   - Konvertiert Datetime → ISO-Format Strings
   - Generiert eindeutige `session_id` (UUID)

4. **LOAD**: Lädt zu BigQuery hoch

   - **Details zuerst**: Batch-Upload (1000 Zeilen)
   - **Sessions danach**: Single-Row Upload
   - Bei Fehler: Transaction Rollback

5. **ARCHIVE**: Verschiebt Dateien

   - Erfolg → `processed/`
   - Fehler → `failed/`
   - Hash bleibt unverändert (wichtig!)

6. **SUMMARY**: Statistiken
   - Anzahl erfolgreich verarbeitet
   - Anzahl fehlgeschlagen
   - Gesamt-Anzahl

## Projektstruktur

```
python-fit/
├── src/
│   ├── __init__.py
│   ├── config.py              # Konfigurationsmanagement + BigQuery Schemas
│   ├── hash_manager.py        # SHA-256 Duplikaterkennung
│   ├── fit_parser.py          # FIT-Datei Parser mit GPS-Konvertierung
│   ├── bigquery_client.py     # BigQuery Operations mit DateTime-Serialisierung
│   └── etl_pipeline.py        # Haupt-Pipeline Orchestrierung
├── files/                     # Input: FIT-Dateien
├── processed/                 # Erfolgreich verarbeitete Dateien
├── failed/                    # Fehlgeschlagene Dateien
├── logs/                      # Log-Dateien (UTF-8 encoded)
├── run_etl.py                 # Pipeline Runner
├── setup.py                   # Setup-Wizard
├── test_parser.py             # FIT Parser Test (ohne BigQuery)
├── test_bigquery.py           # BigQuery Connection Test
├── requirements.txt           # Python Dependencies
├── .env.example              # Konfigurationsvorlage
├── .gitignore                # Git Ignore Patterns
├── README.md                 # Diese Datei
└── QUICKSTART.md             # Schnellstart-Anleitung
```

## Logging

Logs werden gespeichert in:

- **Datei**: `logs/etl_YYYYMMDD_HHMMSS.log` (UTF-8 encoded)
- **Console**: Gleichzeitige Ausgabe auf Bildschirm

**Log-Format**:

```
%(asctime)s - %(name)s - %(levelname)s - %(message)s
```

**Log-Level** konfigurierbar über `.env`:

- `DEBUG`: Sehr detailliert (Hash-Checks, einzelne Records)
- `INFO`: Standard (Processing-Status, Uploads)
- `WARNING`: Warnungen (fehlende Dateien, leere Records)
- `ERROR`: Fehler (Parsing-Fehler, BigQuery-Fehler)

## Fehlerbehandlung

### Parsing-Fehler

- **Ursache**: Korrupte FIT-Datei, nicht unterstütztes Format
- **Aktion**: Datei → `failed/`
- **Log**: Stack Trace mit Fehlerdetails
- **Lösung**: Datei manuell prüfen, ggf. reparieren

### BigQuery-Fehler

- **Ursache**: Netzwerkfehler, Authentication, Quota überschritten
- **Aktion**: Transaction Rollback, Datei bleibt in `files/`
- **Log**: BigQuery API Error Details
- **Lösung**: Beim nächsten Lauf wird erneut versucht

### Datetime-Serialisierung

- **Ursache**: `datetime` Objekte nicht JSON-serialisierbar
- **Lösung**: Automatische Konvertierung zu ISO-Format
- **Funktion**: `serialize_datetime()` in `bigquery_client.py`

### Duplikat-Dateien

- **Ursache**: Windows case-insensitive Filesystem
- **Lösung**: Set-basierte Deduplizierung in `find_unprocessed_files()`
- **Verhalten**: Jede Datei wird nur einmal verarbeitet

### Fehlende Dateien

- **Ursache**: Datei wurde bereits verschoben
- **Lösung**: Existence-Check vor Verarbeitung
- **Log**: Warning statt Error

## GPS-Koordinaten Konvertierung

FIT-Dateien speichern GPS-Koordinaten in **Semicircles** (Integer-Format).

Die Pipeline konvertiert automatisch zu **Dezimalgrad**:

```python
def semicircles_to_degrees(semicircles: int) -> float:
    """Konvertiert Semicircles zu Dezimalgrad"""
    return semicircles * (180.0 / 2**31)

# Beispiel:
# Semicircles: 591621683
# Dezimalgrad: 49.326624507
```

**Warum Semicircles?**

- 2³¹ Semicircles = 180° (halber Kreis)
- Integer-Speicherung = präzise & kompakt
- Standard in FIT-Format

## Beispiel-Output

```
================================================================================
FIT to BigQuery ETL Pipeline Started
================================================================================
INFO - Validating configuration...
================================================================================
SETUP: Initializing BigQuery...
================================================================================
INFO - Initializing BigQuery client...
INFO - Initialized BigQuery client for fit-analyze-480219.fitness_data
INFO - Verifying dataset exists...
INFO - Dataset fitness_data exists
INFO - Setting up sessions table...
INFO - Table sessions exists
INFO - Setting up details table...
INFO - Table details exists
INFO - [OK] BigQuery setup complete - dataset and tables ready
================================================================================
EXTRACT: Finding unprocessed files...
================================================================================
INFO - Scanning for FIT files in files
INFO - Found 67 FIT files
INFO - Found 262 previously processed files in BigQuery
INFO - New file: 2024-08-24-100540-ELEMNT_BOLT_3FFA-159-0.fit
INFO - New file: zwift-activity-1729985326102888448.fit
INFO - Found 2 unprocessed files
================================================================================
TRANSFORM & LOAD: Processing files...
================================================================================
--------------------------------------------------------------------------------
Processing: 2024-08-24-100540-ELEMNT_BOLT_3FFA-159-0.fit
Hash: a3b2c1d4e5f6...
INFO - Parsing FIT file...
INFO - Extracted session and 8932 records from file
INFO - Uploading to BigQuery...
INFO - Uploading 8932 records to details
INFO - Successfully inserted 8932 rows into details
INFO - Uploading session to sessions
INFO - Successfully inserted 1 rows into sessions
INFO - [OK] Successfully processed 2024-08-24-100540-ELEMNT_BOLT_3FFA-159-0.fit
INFO - Moved file to processed/
--------------------------------------------------------------------------------
Processing: zwift-activity-1729985326102888448.fit
Hash: b4c3d2e1f0a9...
INFO - Parsing FIT file...
INFO - Extracted session and 3621 records from file
INFO - Uploading to BigQuery...
INFO - Uploading 3621 records to details
INFO - Successfully inserted 3621 rows into details
INFO - Uploading session to sessions
INFO - Successfully inserted 1 rows into sessions
INFO - [OK] Successfully processed zwift-activity-1729985326102888448.fit
INFO - Moved file to processed/
================================================================================
ETL Pipeline Complete
================================================================================
Successfully processed: 2
Failed: 0
Total: 2
```

## Performance-Optimierung

- **Batch-Upload**: Standard 1000 Zeilen pro Batch (konfigurierbar via `BATCH_SIZE`)
- **Partitionierung**: Nach Timestamp für schnelle Zeitbereichs-Abfragen
  - Sessions: Täglich nach `start_time`
  - Details: Täglich nach `timestamp`
- **Clustering**: Nach Manufacturer und Sport für optimierte Filterung
  - Sessions: `manufacturer`, `sport`
  - Details: `session_id`, `file_hash`
- **Streaming-Insert**: Für sofortige Datenverfügbarkeit in BigQuery
- **DateTime-Serialisierung**: Automatische ISO-Format Konvertierung
- **Set-basierte Deduplizierung**: Keine doppelten Dateien auf Windows
- **Existence-Checks**: Vermeidet FileNotFoundError bei verschobenen Dateien

## Beispiel-Abfragen (BigQuery)

### Alle Sessions anzeigen

```sql
SELECT
  session_id,
  filename,
  start_time,
  sport,
  total_distance / 1000 AS distance_km,
  total_timer_time / 3600 AS duration_hours,
  avg_heart_rate,
  avg_power,
  total_calories
FROM `your-project.fitness_data.sessions`
ORDER BY start_time DESC
LIMIT 20
```

### Session mit Details kombinieren

```sql
SELECT
  s.filename,
  s.start_time,
  s.sport,
  s.manufacturer,
  COUNT(d.record_id) AS total_records,
  AVG(d.heart_rate) AS avg_hr,
  MAX(d.power) AS max_power,
  MAX(d.distance) AS final_distance
FROM `your-project.fitness_data.sessions` s
LEFT JOIN `your-project.fitness_data.details` d
  ON s.session_id = d.session_id
WHERE s.start_time >= '2024-01-01'
GROUP BY s.filename, s.start_time, s.sport, s.manufacturer
ORDER BY s.start_time DESC
```

### GPS-Track einer Session

```sql
SELECT
  timestamp,
  position_lat,
  position_long,
  enhanced_altitude AS altitude,
  heart_rate,
  power,
  enhanced_speed AS speed,
  distance
FROM `your-project.fitness_data.details`
WHERE session_id = 'YOUR-SESSION-ID'
  AND position_lat IS NOT NULL
  AND position_long IS NOT NULL
ORDER BY timestamp
```

### Leistungsanalyse nach Hersteller

```sql
SELECT
  manufacturer,
  COUNT(*) AS total_activities,
  SUM(total_distance) / 1000 AS total_km,
  AVG(avg_power) AS avg_power,
  AVG(avg_heart_rate) AS avg_hr,
  SUM(total_calories) AS total_calories
FROM `your-project.fitness_data.sessions`
WHERE sport = 'cycling'
GROUP BY manufacturer
ORDER BY total_activities DESC
```

## Troubleshooting

### "Import could not be resolved"

Dependencies müssen installiert werden:

```powershell
pip install --user -r requirements.txt
```

### "GOOGLE_APPLICATION_CREDENTIALS not set"

Prüfen Sie Ihre `.env` Datei:

- Pfad zum Service Account Key korrekt?
- Datei existiert an diesem Ort?
- Umgebungsvariable gesetzt?

Testen Sie mit:

```powershell
python test_bigquery.py
```

### "Not found: Table"

Tabellen werden automatisch beim ersten Lauf erstellt.

- Service Account benötigt BigQuery Admin oder Editor Rolle
- Dataset-Name in `.env` korrekt?
- Project-ID korrekt?

### "Object of type datetime is not JSON serializable"

**Behoben!** Die Pipeline konvertiert automatisch `datetime` zu ISO-Format Strings.

Falls der Fehler trotzdem auftritt:

- Prüfen Sie `bigquery_client.py` → `serialize_datetime()` Funktion
- Neueste Version des Codes?

### "charmap codec can't encode character"

**Behoben!** Logs verwenden jetzt UTF-8 encoding.

Falls auf Windows Probleme auftreten:

- Log-Datei mit UTF-8 Editor öffnen (VS Code, Notepad++)
- Keine Unicode-Symbole mehr in Logs (`[OK]` statt `✓`)

### Duplikate werden verarbeitet

**Behoben!** Set-basierte Deduplizierung verhindert doppelte Verarbeitung.

Falls immer noch Probleme:

- Prüfen Sie `hash_manager.py` → `find_unprocessed_files()`
- Beide glob-Patterns in einem Set vereint?

### "File not found" für bereits verschobene Dateien

**Behoben!** Existence-Checks vor Verarbeitung und beim Verschieben.

Falls der Fehler auftritt:

- Datei manuell aus `failed/` oder `processed/` nach `files/` zurückverschieben
- Pipeline erneut ausführen

### Keine unverarbeiteten Dateien gefunden

Mögliche Ursachen:

- Alle Dateien bereits in BigQuery (Hash-Check)
- Dateien bereits in `processed/` verschoben
- Keine FIT-Dateien in `files/` Verzeichnis

Prüfen:

```powershell
# Dateien in files/ zählen
Get-ChildItem files\*.fit | Measure-Object

# BigQuery abfragen
python test_bigquery.py
```

### BigQuery Quota überschritten

Bei vielen Dateien kann das Streaming-Insert Quota erreicht werden.

Lösung:

- `BATCH_SIZE` in `.env` erhöhen (z.B. 5000)
- Oder: Warten (Quota regeneriert sich)
- Oder: Batch-Insert statt Streaming-Insert verwenden

## Technische Details

### Implementierte Anforderungen

✅ **Extract (Hash-basierte Duplikaterkennung)**

- SHA-256 Hash-Generierung für jede Datei
- BigQuery-Abfrage gegen `sessions.file_hash`
- Set-basierte Deduplizierung (Windows-kompatibel)
- Existence-Checks vor Verarbeitung

✅ **Transform (FIT-Parsing & Strukturierung)**

- `garmin-fit-sdk` Library für FIT-Datei Parsing (offizielles Garmin SDK)
- Session-Daten Extraktion (Zusammenfassungs-Metriken)
- Record-Daten Extraktion (Sekunden-genaue Zeitreihen)
- GPS-Konvertierung: Semicircles → Dezimalgrad
- DateTime-Serialisierung: `datetime` → ISO-Format
- UUID-Generierung für `session_id`
- Null-Value Handling

✅ **Load (BigQuery Batch-Upload)**

- Transaktionale Logik: Details zuerst, dann Sessions
- Batch-Upload (konfigurierbar, Standard 1000)
- Streaming-Insert für sofortige Verfügbarkeit
- Error-Handling mit Rollback
- Partitionierung und Clustering

✅ **Fehlerbehandlung**

- Try-Catch auf allen Ebenen
- Detailliertes Logging (UTF-8 encoded)
- Failed-Files → `failed/`
- Success-Files → `processed/`
- Hash bleibt unverändert

✅ **Aufräumarbeiten**

- Automatisches Verschieben nach Verarbeitung
- Hash-Preservation (wichtig für Deduplizierung)
- Name-Collision Handling (Timestamp-Suffix)
- Existence-Checks vor dem Verschieben

### Code-Architektur

**Modular & SOLID-Prinzipien:**

- `config.py`: Single Responsibility - Konfiguration
- `hash_manager.py`: Single Responsibility - Duplikaterkennung
- `fit_parser.py`: Single Responsibility - FIT-Parsing
- `bigquery_client.py`: Single Responsibility - BigQuery Ops
- `etl_pipeline.py`: Orchestrierung aller Module

**Error-Resilient:**

- Existence-Checks auf 3 Ebenen
- Set-basierte Deduplizierung
- DateTime-Serialisierung
- UTF-8 Encoding
- Rollback bei BigQuery-Fehlern

**Production-Ready:**

- Umfassendes Logging
- Konfigurierbar via `.env`
- Helper-Scripts (setup, test_parser, test_bigquery)
- Dokumentation (README, QUICKSTART, Docstrings)

## Lizenz

Privates Projekt

## Autor

Pierre (plaub)
