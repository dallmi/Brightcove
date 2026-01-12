# UnifiedPipeline - Brightcove Analytics Pipeline

Robuste Pipeline zur Erfassung von Brightcove Video-Analytics mit allen Metadaten.

## Voraussetzungen

- Python 3.9+
- `pip install requests tqdm pandas openpyxl`
- `secrets.json` im Brightcove-Hauptverzeichnis mit:
  ```json
  {
    "client_id": "...",
    "client_secret": "...",
    "proxies": {"http": "...", "https": "..."}
  }
  ```

## Test-Modus (empfohlen vor erstem Run!)

Testet die Pipeline mit nur 1 Account (MyWay) und nur 2026 (keine History):

```bash
cd /Users/micha/Documents/Arbeit/Brightcove

# Test-Modus aktivieren
export PIPELINE_TEST=1

# Alle Scripts durchlaufen (~5-10 min statt 5-9 Stunden)
python UnifiedPipeline/scripts/1_cms_metadata.py
python UnifiedPipeline/scripts/2_dt_last_viewed.py
python UnifiedPipeline/scripts/3_daily_analytics.py
python UnifiedPipeline/scripts/4_combine_output.py

# Test-Modus deaktivieren
unset PIPELINE_TEST
```

**Test-Config (`config/*_TEST.json`):**
- `accounts_TEST.json`: Nur MyWay (kleinster Account)
- `settings_TEST.json`: Nur 2026, keine historischen Jahre

**Nach erfolgreichem Test:** Checkpoints löschen vor Produktions-Run:
```bash
rm -rf UnifiedPipeline/checkpoints/*
rm -rf UnifiedPipeline/output/*
```

## Workflow

### Erster Run (~5-9 Stunden)

```bash
cd /Users/micha/Documents/Arbeit/Brightcove

python UnifiedPipeline/scripts/1_cms_metadata.py      # ~10 min
python UnifiedPipeline/scripts/2_dt_last_viewed.py    # ~60-90 min
python UnifiedPipeline/scripts/3_daily_analytics.py   # ~4-8 h (2024+2025+2026)
python UnifiedPipeline/scripts/4_combine_output.py    # ~3 min
```

### Folgende Runs (~2 Stunden)

```bash
cd /Users/micha/Documents/Arbeit/Brightcove

python UnifiedPipeline/scripts/1_cms_metadata.py      # ~10 min  (WICHTIG: neue Videos!)
python UnifiedPipeline/scripts/2_dt_last_viewed.py    # ~60-90 min (WICHTIG: dt_last_viewed!)
python UnifiedPipeline/scripts/3_daily_analytics.py   # ~30-60 min (nur 2026)
python UnifiedPipeline/scripts/4_combine_output.py    # ~3 min
```

## Warum jeden Schritt bei jedem Run?

| Skript | Warum bei jedem Run? |
|--------|---------------------|
| `1_cms_metadata` | Neue Videos seit letztem Run erfassen |
| `2_dt_last_viewed` | dt_last_viewed aktualisieren (90-Tage-Filter braucht aktuelle Daten) |
| `3_daily_analytics` | Historisch wird automatisch übersprungen |
| `4_combine_output` | CSVs neu generieren |

## Daten-Strategie

```
┌─────────────────────────────────────────────────────┐
│  HISTORISCH (2024 + 2025)                           │
│  • Alle Videos (kein Filter)                        │
│  • Einmalig beim ersten Run                         │
│  • Checkpoint: daily_historical.jsonl               │
└─────────────────────────────────────────────────────┘
                      +
┌─────────────────────────────────────────────────────┐
│  AKTUELL (2026)                                     │
│  • Nur Videos mit Views in letzten 90 Tagen         │
│  • Inkrementell bei jedem Run                       │
│  • Checkpoint: daily_current.jsonl                  │
└─────────────────────────────────────────────────────┘
                      =
┌─────────────────────────────────────────────────────┐
│  OUTPUT                                             │
│  • daily_analytics_2024_*.csv                       │
│  • daily_analytics_2025_*.csv                       │
│  • daily_analytics_2026_*.csv                       │
│  • daily_analytics_2024_2025_2026_all.csv           │
└─────────────────────────────────────────────────────┘
```

## Ordnerstruktur

```
UnifiedPipeline/
├── config/
│   ├── accounts.json       # 11 Accounts + Kategorien
│   └── settings.json       # Jahre, Retry, etc.
├── checkpoints/
│   ├── daily_historical.jsonl    # 2024+2025 Daten
│   ├── daily_current.jsonl       # 2026 Daten
│   └── historical_status.json    # Tracking welche Jahre fertig
├── output/
│   ├── cms/                # CMS Metadaten (JSON/CSV)
│   ├── analytics/          # dt_last_viewed + enriched JSON
│   ├── daily/              # Finale Analytics CSVs
│   └── life_cycle_mgmt/    # Excel-Dateien für Lifecycle Management ⭐
│       ├── Internet_cms.xlsx
│       ├── Intranet_cms.xlsx
│       ├── neo_cms.xlsx
│       └── ... (alle 11 Accounts)
├── logs/                   # Log-Dateien
└── scripts/
    ├── shared.py           # Gemeinsame Utilities
    ├── 1_cms_metadata.py
    ├── 2_dt_last_viewed.py
    ├── 3_daily_analytics.py
    └── 4_combine_output.py
```

## Lifecycle Management Output (Excel)

Nach jedem Run von `2_dt_last_viewed.py` werden automatisch Excel-Dateien generiert:

```
output/life_cycle_mgmt/
├── Internet_cms.xlsx
├── Intranet_cms.xlsx
├── neo_cms.xlsx
├── research_cms.xlsx
├── research_internal_cms.xlsx
├── impact_cms.xlsx
├── circleone_cms.xlsx
├── digital_networks_events_cms.xlsx
├── fa_web_cms.xlsx
├── SuMiTrust_cms.xlsx
└── MyWay_cms.xlsx
```

Diese Dateien entsprechen dem Harper-Format (`channel_cms.xlsx`) und enthalten:
- Alle CMS-Metadaten
- `dt_last_viewed` (letztes View-Datum)
- Alle `cf_*` Custom Fields

## Accounts (11)

| Account | Kategorie |
|---------|-----------|
| Internet, Intranet | internet_intranet |
| neo, research, research_internal | research |
| impact, circleone, fa_web, SuMiTrust, MyWay | gwm |
| digital_networks_events | events |

## Output-Spalten (44)

Reporting-Felder (32) + Harper-Felder (12):
- `dt_last_viewed` - Letztes View-Datum
- `cf_*` - Alle Custom Fields (Owner, Compliance, etc.)

## Fehlerbehandlung

- **5 Retries** mit exponential Backoff + Jitter
- **Window-Splitting** bei API-Fehlern (bis auf Tagesebene)
- **Checkpointing** nach jedem Video/Window
- Bei Abbruch einfach neu starten - setzt automatisch fort

## Konfiguration anpassen

`config/settings.json`:
```json
{
  "daily_analytics": {
    "historical_years": [2024, 2025],
    "current_year": 2026,
    "days_back_filter": 90
  }
}
```

Für 2027: `current_year` auf 2027 ändern, 2026 zu `historical_years` hinzufügen.
