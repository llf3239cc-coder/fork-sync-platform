#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Process generate_syncability_input output: keyword filter -> DeepSeek API to judge security-related.

Stream by batch (default 1000 per batch); within each batch: multiprocess keyword filter, multithread DeepSeek.
Input: generate_syncability_input result dir (file_count_N/*.json or root *.json).
Flow: 1) Filter by security keywords; 2) For commits passing keywords, call DeepSeek to judge security.
Output: One commit per file in output_dir/commits (or --commits-dir), same naming as input (origin__fork__hash.json),
        with keyword_matched / deepseek_security / deepseek_reason / deepseek_error; plus summary CSV.
"""

from __future__ import annotations

import json
import os
import re
import sys
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

try:
    import httpx
except ImportError:
    httpx = None

DEEPSEEK_BASE_URL = "https://api.deepseek.com/v1/chat/completions"
# DeepSeek-V3.2: deepseek-chat (non-reasoning) / deepseek-reasoner (reasoning)
DEEPSEEK_MODEL = "deepseek-reasoner"
# API key set in code; can override with --api-key or env DEEPSEEK_API_KEY
DEEPSEEK_API_KEY = "sk-ea182b22c22d403f9d093aaf588c6f98"

DEFAULT_SECURITY_KEYWORDS = [
    "CVE", "security", "vulnerability", "vulnerabilities", "exploit", "CVSS",
    "XSS", "CSRF", "RCE", "injection", "sanitize", "buffer overflow",
    "auth", "authentication", "authorization", "password", "secret", "token",
    "privilege", "escalation", "malicious", "unsafe", "mitigation", "hardening","fix", "bug",
]

MAX_DIFF_CHARS = 80_000


def _normalize_files(files_any: Any) -> List[str]:
    if not files_any or not isinstance(files_any, list):
        return []
    out = []
    for f in files_any:
        if isinstance(f, str) and f.strip():
            out.append(f.strip())
        elif isinstance(f, dict):
            s = (f.get("path") or f.get("filename") or f.get("file_path") or "").strip()
            if s:
                out.append(s)
    return out


def _collect_files_from_commit(commit: Dict[str, Any]) -> List[str]:
    seen: Set[str] = set()
    result: List[str] = []

    def add(files_any: Any) -> None:
        for p in _normalize_files(files_any):
            if p and p not in seen:
                seen.add(p)
                result.append(p)

    add(commit.get("files_changed"))
    add(commit.get("files"))
    raw = commit.get("raw_data") or {}
    add(raw.get("files"))
    add(raw.get("files_changed"))
    for key in ("stage0_result", "stage1_result", "stage2_result"):
        st = commit.get(key) or {}
        if isinstance(st, dict):
            add(st.get("files_changed"))
            add(st.get("files"))
    return result


def _keyword_match(text: str, keywords: List[str]) -> bool:
    if not text:
        return False
    t = text.lower()
    for kw in keywords:
        if kw.lower() in t:
            return True
    return False


def _keyword_match_commit(commit: Dict[str, Any], keywords: List[str]) -> bool:
    raw = commit.get("raw_data") or {}
    msg = (
        raw.get("message") or raw.get("commit_message")
        or commit.get("message") or commit.get("commit_message") or ""
    )
    diff = raw.get("diff") or raw.get("patch") or commit.get("diff") or ""
    title = commit.get("title") or ""
    return _keyword_match(f"{title}\n{msg}\n{diff}", keywords)


def _commit_id(commit: Dict[str, Any]) -> str:
    raw = commit.get("raw_data") or {}
    h = (
        commit.get("item_id") or commit.get("commit_hash")
        or commit.get("sha") or commit.get("hash")
        or raw.get("hash") or raw.get("sha") or raw.get("id") or "unknown"
    )
    return str(h)[:64]


def _commit_filename(commit: Dict[str, Any]) -> str:
    origin = (commit.get("origin_repo") or "unknown_origin").replace("/", "__")
    fork = (commit.get("fork_repo") or "unknown_fork").replace("/", "__")
    h = _commit_id(commit)
    return f"{origin}__{fork}__{h}.json"


def _count_security(rows: List[Dict[str, Any]]) -> int:
    """Count rows where deepseek_security is True."""
    n = 0
    for r in rows:
        v = r.get("deepseek_security")
        if v is True:
            n += 1
        elif isinstance(v, str) and v.strip().lower() in ("true", "1"):
            n += 1
    return n


def _count_keyword_matched(rows: List[Dict[str, Any]]) -> int:
    """Count rows where keyword_matched is True."""
    n = 0
    for r in rows:
        v = r.get("keyword_matched")
        if v is True:
            n += 1
        elif isinstance(v, str) and v.strip().lower() in ("true", "1"):
            n += 1
    return n


def _count_from_commits_dir(commits_dir: Path) -> Tuple[int, int]:
    """Scan commits_dir for *__*__*.json, sum keyword_matched and deepseek_security. Return (keyword_matched count, security count)."""
    kw_n, sec_n = 0, 0
    if not commits_dir.exists():
        return 0, 0
    for p in commits_dir.glob("*.json"):
        if "__" not in p.name or p.name.startswith("."):
            continue
        try:
            with open(p, "r", encoding="utf-8") as f:
                d = json.load(f)
        except Exception:
            continue
        v = d.get("keyword_matched")
        if v is True or (isinstance(v, str) and v.strip().lower() in ("true", "1")):
            kw_n += 1
        v = d.get("deepseek_security")
        if v is True or (isinstance(v, str) and v.strip().lower() in ("true", "1")):
            sec_n += 1
    return kw_n, sec_n


def _fname_from_row(row: Dict[str, Any]) -> str:
    """Derive output filename from CSV row (same as _commit_filename)."""
    o = (row.get("origin_repo") or "unknown_origin").replace("/", "__")
    f = (row.get("fork_repo") or "unknown_fork").replace("/", "__")
    h = (row.get("commit_id") or "unknown_hash")[:64]
    return f"{o}__{f}__{h}.json"


def _load_processed_and_existing(
    output_dir: Path,
    commits_dir: Optional[Path] = None,
) -> Tuple[Set[str], List[Dict[str, Any]]]:
    """
    Load processed set (for skip) and existing CSV rows (for merge).
    Return (processed_fnames, existing_rows).
    """
    processed: Set[str] = set()
    existing_rows: List[Dict[str, Any]] = []
    csv_path = output_dir / "keyword_deepseek_security_summary.csv"
    if csv_path.exists():
        try:
            with open(csv_path, "r", encoding="utf-8", newline="") as f:
                r = csv.DictReader(f)
                for row in r:
                    existing_rows.append(row)
                    processed.add(_fname_from_row(row))
        except Exception:
            pass
    cd = commits_dir if commits_dir is not None else output_dir / "commits"
    if cd.exists():
        for p in cd.glob("*.json"):
            if "__" in p.name and not p.name.startswith("."):
                processed.add(p.name)
    return processed, existing_rows


def _filter_chunk(
    arg: Tuple[List[Tuple[str, Dict[str, Any]]], List[str]],
) -> List[Tuple[str, Dict[str, Any], bool]]:
    """Multiprocess worker: keyword filter a batch of (path, commit). Return [(path, commit, matched), ...]."""
    chunk, keywords = arg
    out: List[Tuple[str, Dict[str, Any], bool]] = []
    for path_str, commit in chunk:
        matched = _keyword_match_commit(commit, keywords)
        out.append((path_str, commit, matched))
    return out


def _write_commit_result(commit: Dict[str, Any], keyword_matched: bool, deepseek_security: bool, deepseek_reason: str, deepseek_error: str, commits_dir: Path) -> None:
    """Write single commit result to commits_dir, same format as input (origin__fork__hash.json), with keyword_matched / deepseek_*."""
    out = {**commit, "keyword_matched": keyword_matched, "deepseek_security": deepseek_security, "deepseek_reason": deepseek_reason, "deepseek_error": deepseek_error}
    fname = _commit_filename(commit)
    path = commits_dir / fname
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)


def _deepseek_task(
    commit: Dict[str, Any],
    api_key: str,
    model: str,
    commits_dir: Path,
) -> Dict[str, Any]:
    """Thread task: call DeepSeek, write single commit to commits_dir, return CSV row."""
    cid = _commit_id(commit)
    origin = commit.get("origin_repo") or ""
    fork = commit.get("fork_repo") or ""
    msg = (
        commit.get("message") or commit.get("commit_message")
        or (commit.get("raw_data") or {}).get("message")
        or (commit.get("raw_data") or {}).get("commit_message")
        or ""
    )
    diff = (
        commit.get("diff") or (commit.get("raw_data") or {}).get("diff")
        or (commit.get("raw_data") or {}).get("patch") or ""
    )
    files_changed = _collect_files_from_commit(commit)
    is_sec, reason, err = _call_deepseek_security(
        msg, diff, files_changed, api_key=api_key, model=model
    )
    _write_commit_result(commit, True, is_sec, reason, err or "", commits_dir)
    return {
        "commit_id": cid,
        "origin_repo": origin,
        "fork_repo": fork,
        "keyword_matched": True,
        "deepseek_security": is_sec,
        "deepseek_reason": reason,
        "deepseek_error": err or "",
    }


def _collect_commit_files(input_dir: Path) -> List[Path]:
    ordered: List[Path] = []
    subs = sorted(
        [d for d in input_dir.iterdir() if d.is_dir() and d.name.startswith("file_count_")],
        key=lambda x: int(x.name.split("_")[-1]) if x.name.split("_")[-1].isdigit() else 999999,
    )
    if subs:
        for d in subs:
            ordered.extend(d.glob("*.json"))
    else:
        for f in input_dir.glob("*.json"):
            if "__" in f.name and not f.name.startswith("."):
                ordered.append(f)
    return ordered


def _call_deepseek_security(
    message: str,
    diff_content: str,
    files_changed: Optional[List[str]] = None,
    api_key: Optional[str] = None,
    model: Optional[str] = None,
) -> Tuple[bool, str, Optional[str]]:
    """
    Call DeepSeek to judge whether commit is security-related.
    Return (is_security, reason, error). Non-empty error means API call failed.
    Default model: deepseek-chat (DeepSeek-V3.2 non-reasoning).
    """
    key = (api_key or os.getenv("DEEPSEEK_API_KEY") or DEEPSEEK_API_KEY or "").strip()
    use_model = (model or DEEPSEEK_MODEL).strip() or DEEPSEEK_MODEL
    if not key:
        return False, "", "Missing DEEPSEEK_API_KEY (set in code, --api-key, or env)"

    if not httpx:
        return False, "", "Install httpx: pip install httpx"

    if len(diff_content) > MAX_DIFF_CHARS:
        diff_content = diff_content[:MAX_DIFF_CHARS] + "\n\n... [truncated]"

    files_str = ""
    if files_changed:
        files_str = "\nFiles changed:\n" + "\n".join(f"  - {f}" for f in files_changed[:50])

    prompt = f"""You are a code and security analysis expert. Given the commit message and diff below, determine whether this commit is **security-related** (e.g. vulnerability fix, auth hardening, CVE, XSS/CSRF/injection mitigation, etc.).

Output only a single JSON object, no other text or markdown:
{{"security": true or false, "reason": "brief reason"}}

Commit message:
{message}
{files_str}

Diff:
{diff_content}

Output JSON:"""

    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    payload = {
        "model": use_model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.2,
    }

    try:
        proxy = os.getenv("HTTP_PROXY") or os.getenv("HTTPS_PROXY") or os.getenv("http_proxy") or os.getenv("https_proxy")
        client_kwargs = {"timeout": 120.0}
        if proxy and not (proxy.startswith("socks") or "socks" in proxy.lower()):
            client_kwargs["proxies"] = proxy

        with httpx.Client(**client_kwargs) as client:
            r = client.post(DEEPSEEK_BASE_URL, headers=headers, json=payload)
            r.raise_for_status()
        data = r.json()
        content = (data.get("choices") or [{}])[0].get("message", {}).get("content", "")
    except Exception as e:
        return False, "", str(e)

    content = content.strip()
    for raw in (content, re.sub(r"^```\w*\n?", "", content), re.sub(r"\n?```\s*$", "", content)):
        try:
            obj = json.loads(raw)
            sec = obj.get("security", False)
            if isinstance(sec, str):
                sec = sec.strip().lower() in ("true", "yes", "1")
            reason = (obj.get("reason") or "").strip() or "(none)"
            return bool(sec), reason, None
        except json.JSONDecodeError:
            pass
    def _first_json_obj(s: str) -> Optional[str]:
        i = s.find("{")
        if i < 0:
            return None
        depth = 0
        for j in range(i, len(s)):
            if s[j] == "{":
                depth += 1
            elif s[j] == "}":
                depth -= 1
                if depth == 0:
                    return s[i : j + 1]
        return None

    frag = _first_json_obj(content)
    if frag:
        try:
            obj = json.loads(frag)
            sec = obj.get("security", False)
            if isinstance(sec, str):
                sec = sec.strip().lower() in ("true", "yes", "1")
            reason = (obj.get("reason") or "").strip() or "(none)"
            return bool(sec), reason, None
        except json.JSONDecodeError:
            pass
    low = content.lower()
    if re.search(r'"security"\s*:\s*true', content, re.I):
        reason = "(regex)"
        r = re.search(r'"reason"\s*:\s*"((?:[^"\\]|\\.)*)"', content)
        if r:
            reason = r.group(1).strip() or "(none)"
        return True, reason, None
    if re.search(r'"security"\s*:\s*false', content, re.I):
        reason = "(regex)"
        r = re.search(r'"reason"\s*:\s*"((?:[^"\\]|\\.)*)"', content)
        if r:
            reason = r.group(1).strip() or "(none)"
        return False, reason, None
    if "yes" in low:
        return True, "(parsed fallback)", None
    if "no" in low or "false" in low:
        return False, "(parsed fallback)", None
    return False, "(parse failed)", None


def _process_batch(
    batch: List[Tuple[str, Dict[str, Any]]],
    kw: List[str],
    key: str,
    use_model: str,
    commits_dir: Path,
    processed: Set[str],
    new_rows: List[Dict[str, Any]],
    stats: Dict[str, Any],
    n_filter: int,
    n_deepseek: int,
) -> None:
    """Process batch: multiprocess keyword filter + multithread DeepSeek, write results and update new_rows/stats."""
    if not batch:
        return
    chunk_size = max(1, len(batch) // (n_filter * 4))
    chunks: List[Tuple[List[Tuple[str, Dict[str, Any]]], List[str]]] = []
    for i in range(0, len(batch), chunk_size):
        chunks.append((batch[i : i + chunk_size], kw))
    filter_results: List[Tuple[str, Dict[str, Any], bool]] = []
    with Pool(processes=n_filter) as pool:
        for res in pool.map(_filter_chunk, chunks):
            filter_results.extend(res)

    matched: List[Dict[str, Any]] = []
    for path_str, commit, m in filter_results:
        fname = _commit_filename(commit)
        processed.add(fname)
        cid = _commit_id(commit)
        origin = commit.get("origin_repo") or ""
        fork = commit.get("fork_repo") or ""
        if not m:
            _write_commit_result(commit, False, False, "", "", commits_dir)
            new_rows.append({
                "commit_id": cid,
                "origin_repo": origin,
                "fork_repo": fork,
                "keyword_matched": False,
                "deepseek_security": False,
                "deepseek_reason": "",
                "deepseek_error": "",
            })
        else:
            matched.append(commit)
            stats["keyword_matched"] += 1

    if matched:
        with ThreadPoolExecutor(max_workers=n_deepseek) as ex:
            futures = {ex.submit(_deepseek_task, c, key, use_model, commits_dir): c for c in matched}
            for fut in as_completed(futures):
                try:
                    row = fut.result()
                    new_rows.append(row)
                    stats["deepseek_called"] += 1
                    if row.get("deepseek_security"):
                        stats["deepseek_security"] += 1
                    if row.get("deepseek_error"):
                        stats["deepseek_errors"] += 1
                except Exception as e:
                    stats["deepseek_called"] += 1
                    stats["deepseek_errors"] += 1
                    c = futures[fut]
                    _write_commit_result(c, True, False, "", str(e), commits_dir)
                    new_rows.append({
                        "commit_id": _commit_id(c),
                        "origin_repo": c.get("origin_repo") or "",
                        "fork_repo": c.get("fork_repo") or "",
                        "keyword_matched": True,
                        "deepseek_security": False,
                        "deepseek_reason": "",
                        "deepseek_error": str(e),
                    })


def run(
    input_dir: Path,
    output_dir: Path,
    *,
    keywords: Optional[List[str]] = None,
    api_key: Optional[str] = None,
    model: Optional[str] = None,
    limit: Optional[int] = None,
    batch_size: int = 1000,
    filter_workers: Optional[int] = None,
    deepseek_workers: Optional[int] = None,
    commits_dir: Optional[Path] = None,
) -> Dict[str, Any]:
    """
    Stream by batch (batch_size per batch); multiprocess keyword filter + multithread DeepSeek per batch.
    Results written per commit to output_dir/commits, plus summary CSV.
    """
    input_dir = input_dir.resolve()
    output_dir = output_dir.resolve()
    kw = keywords or DEFAULT_SECURITY_KEYWORDS
    key = (api_key or os.getenv("DEEPSEEK_API_KEY") or DEEPSEEK_API_KEY or "").strip()
    n_filter = filter_workers if filter_workers is not None and filter_workers > 0 else cpu_count()
    n_deepseek = deepseek_workers if deepseek_workers is not None and deepseek_workers > 0 else 8
    commits_dir = (commits_dir or output_dir / "commits").resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    commits_dir.mkdir(parents=True, exist_ok=True)

    processed, existing_rows = _load_processed_and_existing(output_dir, commits_dir)
    files = _collect_commit_files(input_dir)
    if limit and limit > 0:
        files = files[:limit]

    stats = {
        "total": len(files),
        "skipped": 0,
        "skipped_keyword_matched": 0,
        "skipped_security": 0,
        "keyword_matched": 0,
        "deepseek_called": 0,
        "deepseek_security": 0,
        "deepseek_errors": 0,
    }
    new_rows: List[Dict[str, Any]] = []
    n_processed_this_run = 0
    use_model = (model or DEEPSEEK_MODEL).strip() or DEEPSEEK_MODEL
    batch: List[Tuple[str, Dict[str, Any]]] = []

    print(f"Input: {input_dir}, {len(files)} commits")
    print(f"Already processed (skip): {len(processed)}")
    print(f"Mode: batch size {batch_size}, filter workers={n_filter}, DeepSeek workers={n_deepseek}")
    print(f"Keywords: {kw[:8]}...")
    print(f"DeepSeek model: {use_model} (V3.2)")
    print(f"Output: {output_dir}, single commit -> {commits_dir}")
    existing_kw, existing_sec = _count_from_commits_dir(commits_dir)
    print(f"Existing (commits dir): keyword_matched {existing_kw}, security {existing_sec}")
    print()

    for path in files:
        try:
            with open(path, "r", encoding="utf-8") as f:
                commit = json.load(f)
        except Exception:
            continue
        fname = _commit_filename(commit)
        if fname in processed:
            stats["skipped"] += 1
            try:
                with open(commits_dir / fname, "r", encoding="utf-8") as f:
                    d = json.load(f)
                v = d.get("keyword_matched")
                if v is True or (isinstance(v, str) and str(v).strip().lower() in ("true", "1")):
                    stats["skipped_keyword_matched"] += 1
                v = d.get("deepseek_security")
                if v is True or (isinstance(v, str) and str(v).strip().lower() in ("true", "1")):
                    stats["skipped_security"] += 1
            except Exception:
                pass
            continue
        batch.append((str(path), commit))
        if len(batch) >= batch_size:
            n_processed_this_run += len(batch)
            _process_batch(batch, kw, key, use_model, commits_dir, processed, new_rows, stats, n_filter, n_deepseek)
            total_kw = stats["skipped_keyword_matched"] + stats["keyword_matched"]
            total_sec = stats["skipped_security"] + stats["deepseek_security"]
            print(f"  Processed {n_processed_this_run} | skipped {stats['skipped']} (keyword_matched {stats['skipped_keyword_matched']}, security {stats['skipped_security']}) | keyword_matched {total_kw} (existing {stats['skipped_keyword_matched']} + this run {stats['keyword_matched']}) | security {total_sec} (existing {stats['skipped_security']} + this run {stats['deepseek_security']})")
            batch = []

    if batch:
        n_processed_this_run += len(batch)
        _process_batch(batch, kw, key, use_model, commits_dir, processed, new_rows, stats, n_filter, n_deepseek)
        total_kw = stats["skipped_keyword_matched"] + stats["keyword_matched"]
        total_sec = stats["skipped_security"] + stats["deepseek_security"]
        print(f"  Processed {n_processed_this_run} | skipped {stats['skipped']} (keyword_matched {stats['skipped_keyword_matched']}, security {stats['skipped_security']}) | keyword_matched {total_kw} (existing {stats['skipped_keyword_matched']} + this run {stats['keyword_matched']}) | security {total_sec} (existing {stats['skipped_security']} + this run {stats['deepseek_security']})")

    if n_processed_this_run == 0:
        print("  No new commits to process this run (all skipped)")

    csv_path = output_dir / "keyword_deepseek_security_summary.csv"
    fieldnames = ["commit_id", "origin_repo", "fork_repo", "keyword_matched", "deepseek_security", "deepseek_reason", "deepseek_error"]
    all_rows = existing_rows + new_rows
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(all_rows)

    total_commits = len(all_rows)
    total_security = _count_security(all_rows)
    new_security = stats["deepseek_security"]

    total_kw_final = stats["skipped_keyword_matched"] + stats["keyword_matched"]
    total_sec_final = stats["skipped_security"] + stats["deepseek_security"]
    print()
    print("Stats:")
    print(f"  This run input: {stats['total']} | skipped: {stats['skipped']} (keyword_matched {stats['skipped_keyword_matched']}, security {stats['skipped_security']}) | newly processed: {n_processed_this_run}")
    print(f"  This run keyword_matched: {stats['keyword_matched']} | DeepSeek calls: {stats['deepseek_called']}, security: {new_security}, errors: {stats['deepseek_errors']}")
    print()
    print("Summary (this run scope, existing=skipped + this run=newly processed):")
    print(f"  keyword_matched: {total_kw_final} (existing {stats['skipped_keyword_matched']} + this run {stats['keyword_matched']})")
    print(f"  security: {total_sec_final} (existing {stats['skipped_security']} + this run {stats['deepseek_security']})")
    print(f"  CSV total: {total_commits} commits, security: {total_security}")
    if existing_rows:
        print(f"  Total {total_commits}, new this run {n_processed_this_run}, new security {new_security}")
    print(f"  Output: {commits_dir} (single commit JSON) + {csv_path}")
    return stats


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser(
        description="generate_syncability_input output -> keyword filter -> DeepSeek security check",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python keyword_then_deepseek_security.py --input-dir syncability_input_dir --output-dir keyword_deepseek_out
  python keyword_then_deepseek_security.py -i syncability_input_dir -o out --keywords CVE,vuln,exploit --limit 100
        """,
    )
    ap.add_argument("-i", "--input-dir", type=str, required=True, help="generate_syncability_input output directory")
    ap.add_argument("-o", "--output-dir", type=str, required=True, help="Output directory (summary CSV etc.)")
    ap.add_argument("--commits-dir", type=str, default=None, help="Single commit result dir (default output_dir/commits), same naming as input origin__fork__hash.json")
    ap.add_argument("--keywords", type=str, default=None, help="Security keywords, comma-separated; default use built-in list")
    ap.add_argument("--api-key", type=str, default=None, help="DeepSeek API key (prefer; else env DEEPSEEK_API_KEY or code DEEPSEEK_API_KEY)")
    ap.add_argument(
        "--model",
        type=str,
        default=None,
        help="DeepSeek model (default deepseek-reasoner, V3.2 reasoning); or deepseek-chat (V3.2 non-reasoning)",
    )
    ap.add_argument("--limit", type=int, default=None, help="Max commits to process (for testing)")
    ap.add_argument("--batch-size", type=int, default=1000, help="Commits per batch (default 1000)")
    ap.add_argument("--filter-workers", type=int, default=None, help="Keyword filter process count (default CPU count)")
    ap.add_argument("--deepseek-workers", type=int, default=None, help="DeepSeek call thread count (default 8)")
    args = ap.parse_args()

    inp = Path(args.input_dir)
    if not inp.exists():
        print(f"Error: input directory does not exist: {inp}")
        return 1

    out = Path(args.output_dir)
    kw = [s.strip() for s in args.keywords.split(",") if s.strip()] if args.keywords else None
    run(
        inp,
        out,
        keywords=kw,
        api_key=args.api_key,
        model=args.model,
        limit=args.limit,
        batch_size=args.batch_size,
        filter_workers=args.filter_workers,
        deepseek_workers=args.deepseek_workers,
        commits_dir=Path(args.commits_dir) if args.commits_dir else None,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
