#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Universal NCBI genomes downloader via rsync (RefSeq/GenBank) using assembly_summary.txt.

Highlights:
- requests/tqdm optional (fallback to stdlib).
- Accurate downloaded bytes using rsync --stats parsing (not "dir total size").
- Async queue + workers (scales to huge lists).
- Options for include/exclude, retries, skip-nonempty, max-genomes.

Examples:
  ./ncbi_rsync_genomes.py -d ./db -b -t 8
  ./ncbi_rsync_genomes.py -d ./db --refseq-bacteria --skip-nonempty
  ./ncbi_rsync_genomes.py -d ./db -v --include "*.fna.gz" --exclude "*"
"""

import os
import re
import sys
import time
import shutil
import argparse
import asyncio
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

# ------------------------- optional deps (requests/tqdm) -------------------------
try:
    import requests  # type: ignore
except Exception:
    requests = None

try:
    from tqdm import tqdm  # type: ignore
except Exception:
    tqdm = None

from urllib.request import urlopen, Request


# ------------------------- small utilities -------------------------
def naturalsize(value, binary=True, fmt="%.1f"):
    if value is None:
        return "0 B"
    suffixes = ["B", "KB", "MB", "GB", "TB", "PB"]
    base = 1024.0 if binary else 1000.0
    v = float(value)
    for suffix in suffixes:
        if v < base or suffix == suffixes[-1]:
            if suffix == "B":
                return f"{int(v)} B"
            return (fmt + " %s") % (v, suffix)
        v /= base
    return f"{int(v)} B"


def eprint(*a, **k):
    print(*a, file=sys.stderr, **k)


def which_or_fail(cmd: str) -> str:
    p = shutil.which(cmd)
    if not p:
        raise RuntimeError(f"Required command not found in PATH: {cmd}")
    return p


# ------------------------- groups / sources -------------------------
GROUPS = {
    "refseq_bacteria": ("refseq", "bacteria", "assembly_summary.txt"),
    "refseq_archaea": ("refseq", "archaea", "assembly_summary.txt"),
    "refseq_viral": ("refseq", "viral", "assembly_summary.txt"),
    "genbank_bacteria": ("genbank", "bacteria", "assembly_summary.txt"),
    "genbank_archaea": ("genbank", "archaea", "assembly_summary.txt"),
    "genbank_viral": ("genbank", "viral", "assembly_summary.txt"),
}


def download_text(url: str, timeout: int = 60) -> str:
    """
    Download text file via requests (if available) else urllib.
    """
    if requests is not None:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        return r.text
    # stdlib fallback
    req = Request(url, headers={"User-Agent": "ncbi-rsync-downloader/1.0"})
    with urlopen(req, timeout=timeout) as fh:
        return fh.read().decode("utf-8", errors="replace")


def download_summary_file(base_url: str, remote_subdir: str, summary_file: str,
                          outdir: Path, force: bool = False,
                          label: Optional[str] = None,
                          timeout: int = 60) -> Path:
    """
    Download assembly_summary.txt and store as outdir/<label>_assembly_summary.txt
    """
    outdir.mkdir(parents=True, exist_ok=True)
    if label is None:
        label = remote_subdir

    url = f"{base_url.rstrip('/')}/{remote_subdir}/{summary_file}"
    outpath = outdir / f"{label}_{summary_file}"

    if outpath.exists() and not force:
        eprint(f"[INFO] Using existing summary file: {outpath}")
        return outpath

    eprint(f"[INFO] Downloading: {url} -> {outpath}")
    text = download_text(url, timeout=timeout)
    outpath.write_text(text, encoding="utf-8")
    return outpath


def parse_assembly_summary(path: Path) -> List[Tuple[str, str]]:
    """
    Returns list of (ftp_path, prefix_path_relative_to_genomes_all).
    """
    genomes: List[Tuple[str, str]] = []

    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            if not line or line.startswith("#"):
                continue
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 20:
                continue

            ftp_path = parts[19].strip()
            if not ftp_path or ftp_path.lower() == "na":
                continue

            # Normalize: accept either ftp://.../genomes/all/... or https://.../genomes/all/...
            if "/genomes/all/" in ftp_path:
                prefix = ftp_path.split("/genomes/all/", 1)[1].strip("/")
                if prefix:
                    genomes.append((ftp_path, prefix))
            else:
                # Some older/odd cases might not contain /genomes/all/
                # We skip to avoid corrupt paths.
                continue

    return genomes


# ------------------------- rsync stats parsing -------------------------
_STATS_RE = re.compile(r"Total transferred file size:\s*([0-9,]+)\s*bytes", re.IGNORECASE)


def parse_rsync_transferred_bytes(stdout: str) -> int:
    """
    Parse rsync --stats output and return 'Total transferred file size' in bytes.
    """
    m = _STATS_RE.search(stdout or "")
    if not m:
        return 0
    raw = m.group(1).replace(",", "")
    try:
        return int(raw)
    except Exception:
        return 0


# ------------------------- download worker -------------------------
async def rsync_download(prefix_path: str,
                        db_root: Path,
                        rsync_base: str,
                        rsync_bin: str,
                        timeout: int,
                        include: Optional[List[str]],
                        exclude: Optional[List[str]],
                        skip_nonempty: bool,
                        retries: int) -> Tuple[int, float, int]:
    """
    Downloads one genome directory via rsync.
    Returns: (bytes_transferred, elapsed_seconds, rsync_exit_code)
    """
    target_dir = db_root / "genomes" / "all" / prefix_path
    target_dir.mkdir(parents=True, exist_ok=True)

    # optional optimization: if directory already has something, skip
    if skip_nonempty:
        try:
            if any(target_dir.iterdir()):
                return (0, 0.0, 0)
        except Exception:
            pass

    # ensure trailing slash to copy directory content
    src = f"{rsync_base.rstrip('/')}/{prefix_path.strip('/')}/"
    dst = str(target_dir) + "/"

    cmd = [
        rsync_bin,
        "-a",
        "--ignore-existing",
        "--no-motd",
        f"--timeout={timeout}",
        "--stats",
    ]

    # include/exclude rules (rsync evaluates in order)
    if include:
        for pat in include:
            cmd += ["--include", pat]
    if exclude:
        for pat in exclude:
            cmd += ["--exclude", pat]

    cmd += [src, dst]

    attempt = 0
    while True:
        attempt += 1
        start = time.time()
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out_b, err_b = await proc.communicate()
        elapsed = time.time() - start

        out = (out_b or b"").decode("utf-8", errors="replace")
        err = (err_b or b"").decode("utf-8", errors="replace")

        transferred = parse_rsync_transferred_bytes(out)
        rc = int(proc.returncode or 0)

        if rc == 0:
            return (transferred, elapsed, 0)

        # Retry for transient failures
        if attempt <= retries:
            backoff = min(10, 2 ** (attempt - 1))
            eprint(f"[WARN] rsync failed (rc={rc}) attempt {attempt}/{retries+1} for {prefix_path}. Retrying in {backoff}s...")
            # If you want debug:
            # eprint(err.strip())
            await asyncio.sleep(backoff)
            continue

        eprint(f"[ERROR] rsync failed (rc={rc}) for {prefix_path}")
        if err.strip():
            eprint(err.strip())
        return (transferred, elapsed, rc)


async def worker(name: str,
                 q: "asyncio.Queue[str]",
                 stats: dict,
                 lock: asyncio.Lock,
                 args,
                 pbar=None):
    while True:
        prefix = await q.get()
        if prefix is None:  # type: ignore
            q.task_done()
            return

        b, sec, rc = await rsync_download(
            prefix_path=prefix,
            db_root=Path(args.database),
            rsync_base=args.rsync_base,
            rsync_bin=args.rsync_bin,
            timeout=args.timeout,
            include=args.include,
            exclude=args.exclude,
            skip_nonempty=args.skip_nonempty,
            retries=args.retries,
        )

        async with lock:
            stats["done"] += 1
            stats["bytes"] += b
            stats["elapsed"] += max(sec, 1e-6)
            if rc != 0:
                stats["failed"] += 1

            speed = stats["bytes"] / stats["elapsed"]
            if pbar is not None:
                pbar.update(1)
                pbar.set_postfix({
                    "Total": naturalsize(stats["bytes"]),
                    "AvgSpeed": f"{speed/1024/1024:.2f} MB/s",
                    "Failed": stats["failed"],
                })
            else:
                # minimal progress output
                if stats["done"] % max(1, args.progress_every) == 0 or stats["done"] == stats["total"]:
                    eprint(f"[PROGRESS] {stats['done']}/{stats['total']}  "
                           f"downloaded={naturalsize(stats['bytes'])}  "
                           f"avg={speed/1024/1024:.2f} MB/s  failed={stats['failed']}")

        q.task_done()


# ------------------------- main -------------------------
def build_selected(args) -> List[str]:
    selected: List[str] = []

    if args.bacteria:
        selected += ["refseq_bacteria", "genbank_bacteria"]
    if args.archaea:
        selected += ["refseq_archaea", "genbank_archaea"]
    if args.viral:
        selected += ["refseq_viral", "genbank_viral"]

    if args.refseq_bacteria:
        selected.append("refseq_bacteria")
    if args.refseq_archaea:
        selected.append("refseq_archaea")
    if args.refseq_viral:
        selected.append("refseq_viral")
    if args.gb_bacteria:
        selected.append("genbank_bacteria")
    if args.gb_archaea:
        selected.append("genbank_archaea")
    if args.gb_viral:
        selected.append("genbank_viral")

    # de-dup preserving order
    seen = set()
    out = []
    for x in selected:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out


async def amain(args) -> int:
    # Validate rsync
    try:
        args.rsync_bin = which_or_fail(args.rsync_bin)
    except Exception as ex:
        eprint(f"[ERROR] {ex}")
        eprint("Install rsync or point --rsync-bin to it.")
        return 2

    selected = build_selected(args)
    if not selected:
        eprint("[ERROR] Select at least one group (e.g. -b / -a / -v / --refseq-bacteria).")
        return 2

    db_root = Path(args.database).resolve()
    summary_dir = db_root / "ASSEMBLY_SUMMARY"
    summary_dir.mkdir(parents=True, exist_ok=True)

    all_prefixes: List[str] = []

    for group in selected:
        source, remote_subdir, summary_name = GROUPS[group]
        base_url = f"{args.ncbi_base.rstrip('/')}/{source}"

        summary_path = download_summary_file(
            base_url=base_url,
            remote_subdir=remote_subdir,
            summary_file=summary_name,
            outdir=summary_dir,
            force=args.update_assembly,
            label=group,
            timeout=args.timeout,
        )

        genomes = parse_assembly_summary(summary_path)
        eprint(f"[INFO] {len(genomes)} genomes found in {group}")
        all_prefixes.extend([prefix for _ftp, prefix in genomes])

    # de-dup prefixes (common across groups occasionally)
    prefixes = sorted(set(all_prefixes))
    if args.max_genomes and args.max_genomes > 0:
        prefixes = prefixes[:args.max_genomes]

    eprint(f"[INFO] Total {len(prefixes)} genomes to process.")
    if len(prefixes) == 0:
        eprint("[WARN] No genomes parsed. Check assembly_summary files or filters.")
        return 0

    # queue + workers
    q: asyncio.Queue = asyncio.Queue(maxsize=max(1000, args.threads * 10))
    stats = {"total": len(prefixes), "done": 0, "failed": 0, "bytes": 0, "elapsed": 1e-6}
    lock = asyncio.Lock()

    # Progress bar
    pbar = None
    if tqdm is not None and not args.no_tqdm:
        pbar = tqdm(total=len(prefixes), desc="Downloading genomes", unit="genome")

    workers = [
        asyncio.create_task(worker(f"w{i+1}", q, stats, lock, args, pbar=pbar))
        for i in range(args.threads)
    ]

    # feed queue
    for prefix in prefixes:
        await q.put(prefix)

    # stop workers
    for _ in workers:
        await q.put(None)  # type: ignore

    await q.join()
    for w in workers:
        await w

    if pbar is not None:
        pbar.close()

    speed = stats["bytes"] / stats["elapsed"]
    eprint(f"[DONE] processed={stats['done']} failed={stats['failed']} "
           f"downloaded={naturalsize(stats['bytes'])} avg={speed/1024/1024:.2f} MB/s")

    return 0 if stats["failed"] == 0 else 1


def parse_args():
    p = argparse.ArgumentParser(
        description="Universal downloader of RefSeq/GenBank genomes from NCBI using rsync + assembly_summary.txt"
    )
    p.add_argument("-d", "--database", default="./db", help="Target download directory")

    # Convenience group flags
    p.add_argument("-b", "--bacteria", action="store_true",
                   help="Include bacterial genomes from both RefSeq and GenBank")
    p.add_argument("-a", "--archaea", action="store_true",
                   help="Include archaeal genomes from both RefSeq and GenBank")
    p.add_argument("-v", "--viral", action="store_true",
                   help="Include viral genomes from both RefSeq and GenBank")

    # Specific groups
    p.add_argument("--refseq-bacteria", dest="refseq_bacteria", action="store_true")
    p.add_argument("--refseq-archaea", dest="refseq_archaea", action="store_true")
    p.add_argument("--refseq-viral", dest="refseq_viral", action="store_true")
    p.add_argument("--gb-bacteria", dest="gb_bacteria", action="store_true")
    p.add_argument("--gb-archaea", dest="gb_archaea", action="store_true")
    p.add_argument("--gb-viral", dest="gb_viral", action="store_true")

    # Network / tools
    p.add_argument("--ncbi-base", default="https://ftp.ncbi.nlm.nih.gov/genomes",
                   help="Base URL for summary files (default: NCBI FTP over HTTPS)")
    p.add_argument("--rsync-base", default="rsync://ftp.ncbi.nlm.nih.gov/genomes/all",
                   help="Base rsync URL for genomes/all (default: NCBI)")
    p.add_argument("--rsync-bin", default="rsync",
                   help="rsync executable name/path (default: rsync)")

    p.add_argument("-t", "--threads", type=int, default=5,
                   help="Number of concurrent rsync downloads")
    p.add_argument("--timeout", type=int, default=60,
                   help="rsync/network timeout seconds")
    p.add_argument("--retries", type=int, default=2,
                   help="Retries per genome for rsync transient errors")

    # Selection/behavior
    p.add_argument("--update-assembly", action="store_true",
                   help="Force redownload of assembly_summary files")
    p.add_argument("--skip-nonempty", action="store_true",
                   help="Skip genomes whose target directory is not empty (fast resume mode)")
    p.add_argument("--max-genomes", type=int, default=0,
                   help="Process only first N genomes (useful for testing)")

    # rsync filters
    p.add_argument("--include", action="append", default=None,
                   help="rsync --include pattern (repeatable). Example: --include '*.fna.gz'")
    p.add_argument("--exclude", action="append", default=None,
                   help="rsync --exclude pattern (repeatable). Example: --exclude '*'")

    # Progress
    p.add_argument("--no-tqdm", action="store_true",
                   help="Disable tqdm even if installed")
    p.add_argument("--progress-every", type=int, default=50,
                   help="If no tqdm, print progress every N genomes")

    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    try:
        rc = asyncio.run(amain(args))
    except KeyboardInterrupt:
        eprint("\n[INFO] Interrupted by user.")
        rc = 130
    sys.exit(rc)
