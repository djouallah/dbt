import os
import duckrun

FILES_PATH = os.environ.get("FILES_PATH", "/tmp/landing").rstrip("/")
DOWNLOAD_LIMIT = int(os.environ.get("download_limit", "2"))
DAILY_DOWNLOAD_LIMIT = int(os.environ.get("daily_download_limit", str(DOWNLOAD_LIMIT)))

dr = duckrun.connect(FILES_PATH, read_only=False)
con = dr.con
con.sql("INSTALL httpfs; LOAD httpfs; INSTALL json; LOAD json;")
try:
    con.sql(f"SET GLOBAL azure_transport_option_type='{os.environ.get('AZURE_TRANSPORT_OPTION_TYPE', 'default')}'")
except Exception:
    pass

def push_new(local_folder, rel):
    dr.copy(local_folder, rel, overwrite=False)

def push_replace(local_folder, rel):
    import obstore
    from dbt.adapters.duckrun import objectstore, secret
    base = f"{FILES_PATH}/{rel}" if rel else FILES_PATH
    store = objectstore.build_store(base, secret.refreshed(dr.storage_options))
    for n in os.listdir(local_folder):
        try:
            obstore.delete(store, n)
        except Exception:
            pass
    dr.copy(local_folder, rel, overwrite=True)

print(f"Landing to: {FILES_PATH}")


# The downloader. Idempotent: the archive-log watermark means re-runs only fetch new files.
import io, zipfile, tempfile, urllib.request, urllib.error, time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

def download_aemo(session, files_path, download_limit, daily_download_limit):
    csv_log_path = files_path + "/csv_raw_archive_log.parquet"
    batch_size, max_workers = 7, 8

    # --- Load existing log (or start an empty one) ---
    log_exists = session.sql(f"SELECT count(*) FROM glob('{csv_log_path}')").fetchone()[0]
    if log_exists > 0:
        session.sql(f"""
            CREATE OR REPLACE TEMP TABLE _csv_archive_log AS
            SELECT source_type, source_filename, archive_path, archived_at,
                   row_count, source_url, etag, csv_filename
            FROM read_parquet('{csv_log_path}') WHERE csv_filename IS NOT NULL
        """)
    else:
        session.sql("""
            CREATE OR REPLACE TEMP TABLE _csv_archive_log (
                source_type VARCHAR, source_filename VARCHAR, archive_path VARCHAR,
                archived_at TIMESTAMPTZ, row_count BIGINT, source_url VARCHAR,
                etag VARCHAR, csv_filename VARCHAR)
        """)

    def download_and_extract(url, temp_dir):
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (dbt-aemo)"})
        for attempt in range(3):
            try:
                zip_bytes = urllib.request.urlopen(req, timeout=60).read(); break
            except urllib.error.HTTPError:
                if attempt < 2:
                    time.sleep(2 ** attempt); continue
                raise
        z = zipfile.ZipFile(io.BytesIO(zip_bytes))
        out = []
        for name in z.namelist():
            if name.upper().endswith(".CSV"):
                safe = name.replace("/", "_")
                p = os.path.join(temp_dir, safe)
                with open(p, "wb") as f:      # land UNCOMPRESSED
                    f.write(z.read(name))
                out.append((name, safe, p))
        return out

    def save_log():
        with tempfile.TemporaryDirectory() as ltmp:
            lp = os.path.join(ltmp, "csv_raw_archive_log.parquet").replace("\\", "/")
            session.sql(f"COPY _csv_archive_log TO '{lp}' (FORMAT PARQUET)")
            push_replace(ltmp, "")

    def process(rows, source_type, subfolder):
        files = [(r[0], r[1]) for r in rows]
        for i in range(0, len(files), batch_size):
            batch = files[i:i + batch_size]
            with tempfile.TemporaryDirectory() as tmp:
                extracted = []
                with ThreadPoolExecutor(max_workers=max_workers) as ex:
                    futs = {ex.submit(download_and_extract, u, tmp): (u, fn) for u, fn in batch}
                    for fut in as_completed(futs):
                        u, fn = futs[fut]
                        try:
                            for csv_name, safe, path in fut.result():
                                extracted.append((fn, safe, path, u))
                        except Exception as e:
                            print(f"  WARN skip {fn}: {e}")
                if extracted:
                    push_new(tmp, f"csv_raw/{subfolder}")
                now = datetime.now(timezone.utc).isoformat()
                for fn, csv_name, path, u in extracted:
                    base = csv_name.removesuffix(".CSV").removesuffix(".csv")
                    session.sql(f"""INSERT INTO _csv_archive_log VALUES (
                        '{source_type}', '{fn}', '/{subfolder}/{csv_name}',
                        '{now}'::TIMESTAMPTZ, NULL, '{u}', NULL, '{base}')""")
            save_log()

    def sql_retry(q, attempts=4, base=5):
        for a in range(attempts):
            try:
                return session.sql(q)
            except Exception as e:
                if a < attempts - 1:
                    w = base * (2 ** a); print(f"  WARN net {type(e).__name__}; retry in {w}s"); time.sleep(w)
                else:
                    raise

    def new_files(table, source_type):
        return session.sql(f"""SELECT full_url, filename FROM {table}
            WHERE '{source_type}::' || filename NOT IN (
                SELECT source_type || '::' || source_filename FROM _csv_archive_log)
            LIMIT {download_limit if source_type != 'daily' else daily_download_limit}""").fetchall()

    # --- DAILY (price + scada records live in the same file) ---
    sql_retry("""CREATE OR REPLACE TEMP TABLE daily_files_web AS
        WITH h AS (SELECT content AS html FROM read_text('https://nemweb.com.au/Reports/Current/Daily_Reports/')),
             l AS (SELECT unnest(string_split(html, '<br>')) AS line FROM h)
        SELECT 'https://nemweb.com.au' || regexp_extract(line, 'HREF="([^"]+)"', 1) AS full_url,
               split_part(regexp_extract(line, 'HREF="[^"]+/([^"]+\\.zip)"', 1), '.', 1) AS filename
        FROM l WHERE line LIKE '%PUBLIC_DAILY%.zip%'""")
    aemo_new = session.sql("""SELECT count(*) FROM daily_files_web
        WHERE 'daily::' || filename NOT IN (SELECT source_type || '::' || source_filename FROM _csv_archive_log)""").fetchone()[0]
    if aemo_new < daily_download_limit:
        # backfill history from a GitHub mirror
        sql_retry("""INSERT INTO daily_files_web
            WITH api AS (
                SELECT content AS j FROM read_text('https://api.github.com/repos/djouallah/aemo_data/contents/data/archive/2018')
                UNION ALL SELECT content FROM read_text('https://api.github.com/repos/djouallah/aemo_data/contents/data/archive/2019')
                UNION ALL SELECT content FROM read_text('https://api.github.com/repos/djouallah/aemo_data/contents/data/archive/2020')
                UNION ALL SELECT content FROM read_text('https://api.github.com/repos/djouallah/aemo_data/contents/data/archive/2021')
                UNION ALL SELECT content FROM read_text('https://api.github.com/repos/djouallah/aemo_data/contents/data/archive/2022')
                UNION ALL SELECT content FROM read_text('https://api.github.com/repos/djouallah/aemo_data/contents/data/archive/2023')
                UNION ALL SELECT content FROM read_text('https://api.github.com/repos/djouallah/aemo_data/contents/data/archive/2024')
                UNION ALL SELECT content FROM read_text('https://api.github.com/repos/djouallah/aemo_data/contents/data/archive/2025')
                UNION ALL SELECT content FROM read_text('https://api.github.com/repos/djouallah/aemo_data/contents/data/archive/2026')),
                 p AS (SELECT unnest(from_json(j, '["json"]')) AS fi FROM api)
            SELECT json_extract_string(fi, '$.download_url') AS full_url,
                   split_part(json_extract_string(fi, '$.name'), '.', 1) AS filename
            FROM p WHERE json_extract_string(fi, '$.name') LIKE 'PUBLIC_DAILY%.zip'
              AND split_part(json_extract_string(fi, '$.name'), '.', 1) NOT IN (SELECT filename FROM daily_files_web)""")
    daily = new_files('daily_files_web', 'daily')
    if daily:
        process(daily, 'daily', 'daily')

    # --- INTRADAY SCADA ---
    sql_retry("""CREATE OR REPLACE TEMP TABLE intraday_scada_web AS
        WITH h AS (SELECT content AS html FROM read_text('http://nemweb.com.au/Reports/Current/Dispatch_SCADA/')),
             l AS (SELECT unnest(string_split(html, '<br>')) AS line FROM h)
        SELECT 'http://nemweb.com.au' || regexp_extract(line, 'HREF="([^"]+)"', 1) AS full_url,
               split_part(regexp_extract(line, 'HREF="[^"]+/([^"]+\\.zip)"', 1), '.', 1) AS filename
        FROM l WHERE line LIKE '%PUBLIC_DISPATCHSCADA%' ORDER BY full_url DESC LIMIT 500""")
    scada = new_files('intraday_scada_web', 'scada_today')
    if scada:
        process(scada, 'scada_today', 'scada_today')

    # --- INTRADAY PRICE ---
    sql_retry("""CREATE OR REPLACE TEMP TABLE intraday_price_web AS
        WITH h AS (SELECT content AS html FROM read_text('http://nemweb.com.au/Reports/Current/DispatchIS_Reports/')),
             l AS (SELECT unnest(string_split(html, '<br>')) AS line FROM h)
        SELECT 'http://nemweb.com.au' || regexp_extract(line, 'HREF="([^"]+)"', 1) AS full_url,
               split_part(regexp_extract(line, 'HREF="[^"]+/([^"]+\\.zip)"', 1), '.', 1) AS filename
        FROM l WHERE line LIKE '%PUBLIC_DISPATCHIS_%.zip%' ORDER BY full_url DESC LIMIT 500""")
    price = new_files('intraday_price_web', 'price_today')
    if price:
        process(price, 'price_today', 'price_today')

    # --- DUID REFERENCE (refresh at most daily) ---
    duid_sources = [
        ("duid_data", "duid_data", "https://raw.githubusercontent.com/djouallah/aemo_data/refs/heads/main/duid_data.csv", "duid_data.csv"),
        ("duid_facilities", "facilities", "https://data.wa.aemo.com.au/datafiles/post-facilities/facilities.csv", "facilities.csv"),
        ("duid_wa_energy", "WA_ENERGY", "https://raw.githubusercontent.com/djouallah/aemo_data/refs/heads/main/WA_ENERGY.csv", "WA_ENERGY.csv"),
        ("duid_geo_data", "geo_data", "https://raw.githubusercontent.com/djouallah/aemo_data/refs/heads/main/geo_data.csv", "geo_data.csv"),
    ]
    last = session.sql("SELECT max(archived_at) FROM _csv_archive_log WHERE source_type LIKE 'duid_%'").fetchone()[0]
    fresh = last is not None and (datetime.now(last.tzinfo) - last).total_seconds() < 86400
    if fresh:
        print(f"  DUID data fresh ({last}), skipping")
    else:
        with tempfile.TemporaryDirectory() as dtmp:
            for st, sf, url, fn in duid_sources:
                hdr = ", header=true" if sf == "WA_ENERGY" else ""
                lp = os.path.join(dtmp, fn).replace("\\", "/")
                session.sql(f"""COPY (SELECT * FROM read_csv_auto('{url}', null_padding=true, ignore_errors=true{hdr}))
                                TO '{lp}' (FORMAT CSV, HEADER)""")
            push_replace(dtmp, "csv_raw/duid")
        session.sql("DELETE FROM _csv_archive_log WHERE source_type LIKE 'duid_%'")
        now = datetime.now(timezone.utc).isoformat()
        for st, sf, url, fn in duid_sources:
            base = fn.rsplit(".", 1)[0]
            session.sql(f"""INSERT INTO _csv_archive_log VALUES (
                '{st}', '{sf}', '/duid/{fn}', '{now}'::TIMESTAMPTZ, NULL, '{url}', NULL, '{base}')""")

    save_log()
    return session.sql("SELECT source_type, count(*) AS files FROM _csv_archive_log GROUP BY source_type ORDER BY source_type")

download_aemo(con, FILES_PATH, DOWNLOAD_LIMIT, DAILY_DOWNLOAD_LIMIT).show()
print("Done. Now run:  dbt run --target duckrun   (or iceberg / dwh / spark)")
