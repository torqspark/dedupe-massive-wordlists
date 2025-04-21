import argparse
import os
import sys
import time
import traceback
import sqlite3
from tqdm import tqdm
from datetime import datetime
from multiprocessing import Pool, cpu_count

# Defaults
DEFAULT_REPORT = "duplicates_report.txt"
DEFAULT_CLEANED = "cleaned_noduplicates.txt"
DEFAULT_LOG = "duplicate_log.txt"
DB_FILE = "dedupe_cache.db"
CHUNK_SIZE = 50000       # Output chunk size
BATCH_SIZE = 100000      # Insert buffer size (optimized for 16GB RAM)

def setup_log(log_path):
    try:
        with open(log_path, 'w', encoding='utf-8') as log:
            log.write(f"[{datetime.now()}] Script started\n")
        return log_path
    except Exception as e:
        print(f"❌ Failed to initialize log file: {e}")
        sys.exit(1)

def log_message(message, log_path):
    with open(log_path, 'a', encoding='utf-8') as log:
        log.write(f"[{datetime.now()}] {message}\n")

def format_eta(seconds):
    if seconds < 0:
        return "Estimating..."
    mins, secs = divmod(int(seconds), 60)
    hrs, mins = divmod(mins, 60)
    return f"{hrs:02d}:{mins:02d}:{secs:02d}"

def setup_database(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA synchronous=OFF;")
    cursor.execute("PRAGMA temp_store=MEMORY;")
    cursor.execute("PRAGMA cache_size=-16777216")  # Use up to 16GB RAM
    cursor.execute("DROP TABLE IF EXISTS dedupe")
    cursor.execute("CREATE TABLE dedupe (line TEXT PRIMARY KEY, count INTEGER)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_count ON dedupe(count DESC)")
    conn.commit()
    return conn, cursor

def write_chunk_to_file(args):
    chunk, path = args
    with open(path, 'w', encoding='utf-8') as f:
        for row in chunk:
            f.write(row[0] + "\n")
    return len(chunk)

def count_duplicates_sqlite(input_file, report_path, cleaned_path, log_path):
    script_start = time.time()
    try:
        total_lines = sum(1 for _ in open(input_file, 'r', encoding='utf-8', errors='ignore'))
        log_message(f"Total lines in file: {total_lines}", log_path)

        conn, cursor = setup_database(DB_FILE)

        ingestion_start = time.time()

        with open(input_file, 'r', encoding='utf-8', errors='ignore') as f, \
             tqdm(total=total_lines, desc="Counting duplicates", unit="line", dynamic_ncols=True) as pbar:

            buffer = []
            for line in f:
                line = line.strip()
                buffer.append((line,))
                if len(buffer) >= BATCH_SIZE:
                    cursor.execute("BEGIN TRANSACTION")
                    cursor.executemany("""
                        INSERT INTO dedupe (line, count)
                        VALUES (?, 1)
                        ON CONFLICT(line) DO UPDATE SET count = count + 1
                    """, buffer)
                    cursor.execute("COMMIT")
                    pbar.update(len(buffer))
                    buffer = []

            if buffer:
                cursor.execute("BEGIN TRANSACTION")
                cursor.executemany("""
                    INSERT INTO dedupe (line, count)
                    VALUES (?, 1)
                    ON CONFLICT(line) DO UPDATE SET count = count + 1
                """, buffer)
                cursor.execute("COMMIT")
                pbar.update(len(buffer))

        conn.commit()
        ingestion_end = time.time()

        cursor.execute("SELECT line, count FROM dedupe WHERE count > 1 ORDER BY count DESC")
        duplicates = cursor.fetchall()
        total_duplicates = sum(count - 1 for _, count in duplicates)

        with open(report_path, 'w', encoding='utf-8') as out:
            out.write(f"Total duplicate entries: {total_duplicates}\n\n")
            out.write("Duplicated lines (ranked by count):\n\n")
            for line, count in duplicates:
                out.write(f"{repr(line)}: {count} times\n")

        log_message(f"✅ Report written to: {report_path}", log_path)

        output_start = time.time()

        cursor.execute("SELECT line FROM dedupe ORDER BY rowid")
        all_lines = cursor.fetchall()

        chunked = [all_lines[i:i + CHUNK_SIZE] for i in range(0, len(all_lines), CHUNK_SIZE)]
        temp_paths = [f"{cleaned_path}.part{i}" for i in range(len(chunked))]

        with Pool(min(cpu_count(), len(chunked))) as pool:
            pool.map(write_chunk_to_file, zip(chunked, temp_paths))

        with open(cleaned_path, 'w', encoding='utf-8') as out_final:
            for path in temp_paths:
                with open(path, 'r', encoding='utf-8') as part:
                    out_final.write(part.read())
                os.remove(path)

        output_end = time.time()

        log_message(f"✅ Cleaned file written to: {cleaned_path}", log_path)
        conn.close()

        print(f"\n📁 Report saved to: {report_path}")
        print(f"📄 Cleaned non-duplicate file saved to: {cleaned_path}")

        total_time = round(time.time() - script_start, 2)
        ingestion_time = round(ingestion_end - ingestion_start, 2)
        output_time = round(output_end - output_start, 2)

        log_message(f"⏱️ Total duration: {total_time} seconds", log_path)
        log_message(f"📥 Ingestion duration: {ingestion_time} seconds", log_path)
        log_message(f"📤 Output write duration: {output_time} seconds", log_path)
        log_message("✅ Script completed successfully", log_path)

        if os.path.exists(DB_FILE):
            os.remove(DB_FILE)
            log_message("🧹 Temporary SQLite database deleted.", log_path)

    except KeyboardInterrupt:
        log_message("❌ Script interrupted by user (Ctrl+C)", log_path)
        print("\n❌ Process interrupted by user. Exiting gracefully...")
    except Exception as e:
        log_message(f"❌ Error: {e}", log_path)
        log_message(traceback.format_exc(), log_path)
        print("❌ An error occurred. See log file for details.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Optimized 16GB-RAM SQLite duplicate line analyzer with benchmarking.")
    parser.add_argument("input_file", help="Path to input file")
    parser.add_argument("-o", "--output", help="Duplicate report file path (default: ./duplicates_report.txt)")
    parser.add_argument("-c", "--cleaned", help="Cleaned output file path (default: ./cleaned_noduplicates.txt)")
    parser.add_argument("-l", "--log", help="Log file path (default: ./duplicate_log.txt)")
    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.realpath(__file__))
    report_path = args.output or os.path.join(script_dir, DEFAULT_REPORT)
    cleaned_path = args.cleaned or os.path.join(script_dir, DEFAULT_CLEANED)
    log_path = args.log or os.path.join(script_dir, DEFAULT_LOG)

    setup_log(log_path)
    log_message(f"Input file: {args.input_file}", log_path)
    log_message(f"Output report: {report_path}", log_path)
    log_message(f"Cleaned file: {cleaned_path}", log_path)
    log_message(f"Log file active at: {log_path}", log_path)

    count_duplicates_sqlite(args.input_file, report_path, cleaned_path, log_path)
