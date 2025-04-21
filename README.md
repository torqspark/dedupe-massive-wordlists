SQLite Duplicate Line Analyzer (16GB Optimized with Benchmarking)
==================================================================

This Python application is built for efficiently analyzing and deduplicating very large text files — such as password lists, log archives, or large datasets. It uses an SQLite database (Write-Ahead Logging mode) and is tuned to take advantage of up to 16 GB of RAM for caching, large transaction batching, and parallel output file writing.

-------------------------------------------------------------------------------
WHAT THIS APPLICATION IS UNIQUELY DESIGNED FOR
-------------------------------------------------------------------------------

✅ Deduplicating massive text files containing millions to billions of lines  
✅ Identifying and ranking duplicate lines by frequency  
✅ Generating a clean, deduplicated output file  
✅ Fast I/O using large SQLite memory cache and `executemany()` batching  
✅ Efficient output writing using all CPU cores via multiprocessing  
✅ Logging total runtime, ingestion time, and output time for benchmarking

-------------------------------------------------------------------------------
HARDWARE REQUIREMENTS
-------------------------------------------------------------------------------

🧠 MEMORY:
- Minimum: 4 GB
- Recommended: 16 GB (this version is optimized for it)

💽 DISK SPACE:
- At least 2× the input file size (due to SQLite temp DB and output files)
- SSD highly recommended for speed

🖥️ CPU:
- Quad-core or higher recommended (uses multiprocessing for writing)

-------------------------------------------------------------------------------
SOFTWARE REQUIREMENTS
-------------------------------------------------------------------------------

- Python 3.6 or newer
- Required Python packages:
    tqdm

Install using pip:

    pip install tqdm

-------------------------------------------------------------------------------
USAGE INSTRUCTIONS
-------------------------------------------------------------------------------

Looks like it is frozen or stuck?
Depending on the size of the input file, it will take several minutes to initially show the progress bar.  
For example, a 163GB input file (~14M lines) will take approx 8 minutes to show the progress bar.

🔧 Default usage:

    python3 sqlite_duplicate_line_analyzer_16gb_benchmarked.py inputfile.txt

This creates:
- `duplicates_report.txt` — report of all duplicate lines and their counts
- `cleaned_noduplicates.txt` — deduplicated version of input
- `duplicate_log.txt` — execution log with timing info

⚙️ Advanced usage with custom paths:

    python3 sqlite_duplicate_line_analyzer_16gb_benchmarked.py inputfile.txt \\
        -o logs/my_report.txt \\
        -c results/cleaned_output.txt \\
        -l logs/session_log.txt

OPTIONS:
- input_file: Required — the path to the input file
- -o, --output: Path to the duplicate report (default: duplicates_report.txt)
- -c, --cleaned: Path for deduplicated output (default: cleaned_noduplicates.txt)
- -l, --log: Path to log file (default: duplicate_log.txt)

-------------------------------------------------------------------------------
OUTPUT FILES
-------------------------------------------------------------------------------

| File                     | Description                                           |
|--------------------------|-------------------------------------------------------|
| duplicates_report.txt    | List of duplicates ranked by frequency               |
| cleaned_noduplicates.txt | Final deduplicated output                            |
| duplicate_log.txt        | Log of script progress, errors, and timing           |
| dedupe_cache.db          | Temporary SQLite DB (auto-deleted on success)        |
| *.partN                  | Temporary chunks created for parallel output writing |

-------------------------------------------------------------------------------
PERFORMANCE OPTIMIZATIONS
-------------------------------------------------------------------------------

- `PRAGMA cache_size=-16777216`: SQLite uses up to 16 GB RAM
- `BATCH_SIZE = 100000`: Reduces insert transaction overhead
- Parallel file output: Final file is written using all available CPU cores
- Auto-benchmarks: Logs time spent in ingestion, output, and total runtime

-------------------------------------------------------------------------------
BENCHMARK LOGGING
-------------------------------------------------------------------------------

The log file will contain:
- Total execution time
- Time taken to ingest and process input file
- Time taken to write cleaned output using multiprocessing

-------------------------------------------------------------------------------
LIMITATIONS & CHALLENGES
-------------------------------------------------------------------------------

- Input file must be UTF-8 encoded text (not binary)
- Script doesn't resume from a partial run (no checkpointing)
- SQLite is not optimized for high-concurrency writes, so ingestion is single-process
- Large temporary files are created (~size of input)

-------------------------------------------------------------------------------
FAILURE BEHAVIOR
-------------------------------------------------------------------------------

If the script is interrupted (e.g., via Ctrl+C):
✅ The log file will show graceful termination
🧹 The temporary SQLite DB may remain but can be safely deleted

If the system crashes:
⚠️ Temporary `.partN` files may remain in the output folder
⚠️ The final output file may be incomplete
✅ You can safely rerun the script — it will regenerate everything from scratch

"""
