#!/usr/bin/env python3
"""
Actualized.org Wiki Pipeline
Fetches transcripts via youtube-transcript-api, generates summaries via Claude API,
writes MkDocs-compatible markdown files, updates MAP.md and Conclusions/*.md.

Usage:
  python3 pipeline.py                    # process next pending video
  python3 pipeline.py --limit 10         # process up to 10 videos
  python3 pipeline.py --video wirV265ZYSw # process specific video ID
  python3 pipeline.py --dry-run          # show what would be done
"""

import json
import os
import re
import sys
import time
import argparse
from pathlib import Path
from datetime import datetime

# ── Config ──────────────────────────────────────────────────────────────────
BASE = Path(__file__).parent
PROGRESS_FILE = BASE / "PROGRESS.json"
DOCS_DIR = BASE / "docs"
CONCLUSIONS_DIR = DOCS_DIR / "Conclusions"
MAP_FILE = DOCS_DIR / "MAP.md"
GUESTS_FILE = DOCS_DIR / "GUESTS.md"
EPISODES_DIR = DOCS_DIR / "Episodes"
EPISODES_DIR.mkdir(exist_ok=True)

TOPIC_FILES = {
    "consciousness-and-enlightenment": CONCLUSIONS_DIR / "consciousness-and-enlightenment.md",
    "self-actualization-and-personal-development": CONCLUSIONS_DIR / "self-actualization-and-personal-development.md",
    "psychology-and-emotions": CONCLUSIONS_DIR / "psychology-and-emotions.md",
    "philosophy-and-epistemology": CONCLUSIONS_DIR / "philosophy-and-epistemology.md",
    "life-purpose-and-career": CONCLUSIONS_DIR / "life-purpose-and-career.md",
    "relationships-and-dating": CONCLUSIONS_DIR / "relationships-and-dating.md",
    "psychedelics-and-spirituality": CONCLUSIONS_DIR / "psychedelics-and-spirituality.md",
    "society-politics-and-culture": CONCLUSIONS_DIR / "society-politics-and-culture.md",
    "spiral-dynamics": CONCLUSIONS_DIR / "spiral-dynamics.md",
    "meditation-and-mindfulness": CONCLUSIONS_DIR / "meditation-and-mindfulness.md",
    "health-and-wellness": CONCLUSIONS_DIR / "health-and-wellness.md",
    "money-success-and-business": CONCLUSIONS_DIR / "money-success-and-business.md",
}

TOPIC_LABELS = {
    "consciousness-and-enlightenment": "Consciousness & Enlightenment",
    "self-actualization-and-personal-development": "Self-Actualization & Personal Development",
    "psychology-and-emotions": "Psychology & Emotions",
    "philosophy-and-epistemology": "Philosophy & Epistemology",
    "life-purpose-and-career": "Life Purpose & Career",
    "relationships-and-dating": "Relationships & Dating",
    "psychedelics-and-spirituality": "Psychedelics & Spirituality",
    "society-politics-and-culture": "Society, Politics & Culture",
    "spiral-dynamics": "Spiral Dynamics",
    "meditation-and-mindfulness": "Meditation & Mindfulness",
    "health-and-wellness": "Health & Wellness",
    "money-success-and-business": "Money, Success & Business",
}

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
COOKIES_FILE = BASE / "cookies.txt"

SUMMARY_PROMPT = """\
You are building an Actualized.org knowledge base wiki. Given the transcript of a Leo Gura video, produce a structured markdown summary.

Video title: {title}
Video ID: {video_id}
YouTube URL: https://www.youtube.com/watch?v={video_id}

Transcript:
{transcript}

---

Output ONLY valid markdown (no code fences, no preamble) with this exact structure:

# {title}

> 📄 [View Full Transcript](transcript.md)

**YouTube:** [Watch on YouTube](https://www.youtube.com/watch?v={video_id})

## Overview

[2-3 sentence overview of what this video covers and its core thesis]

## Key Insights

[5-10 bullet points with the most important insights, arguments, or ideas from this video. Be specific and substantive.]

## Core Concepts

[List of key concepts/frameworks discussed with 1-2 sentence explanations each]

## Practical Takeaways

[3-7 actionable takeaways or exercises Leo recommends]

## Topics

topics: [comma-separated list of 1-3 most relevant topic slugs from this list:
consciousness-and-enlightenment, self-actualization-and-personal-development, psychology-and-emotions, philosophy-and-epistemology, life-purpose-and-career, relationships-and-dating, psychedelics-and-spirituality, society-politics-and-culture, spiral-dynamics, meditation-and-mindfulness, health-and-wellness, money-success-and-business]

guest: [full name of guest if this is an interview, else "none"]

---

Important: The Topics and guest lines MUST appear at the end exactly as shown (they are parsed by the pipeline).
"""


def load_progress():
    with open(PROGRESS_FILE) as f:
        return json.load(f)


def save_progress(data):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def _parse_vtt(path):
    import re as _re
    vtt = open(path).read()
    parts, seen = [], set()
    for line in vtt.split("\n"):
        line = line.strip()
        if not line or line.startswith("WEBVTT") or "-->" in line or line.isdigit():
            continue
        clean = _re.sub(r"<[^>]+>", "", line).strip()
        if clean and clean not in seen:
            seen.add(clean)
            parts.append(clean)
    return " ".join(parts).strip() or False


def _get_transcript_ytapi(video_id):
    """Tier 1: youtube-transcript-api — lightweight API call, no IP risk."""
    try:
        from youtube_transcript_api import YouTubeTranscriptApi, NoTranscriptFound, TranscriptsDisabled
        api = YouTubeTranscriptApi()
        t = api.fetch(video_id)
        text = " ".join(s["text"] for s in t.to_raw_data() if s.get("text")).strip()
        return text if text else False
    except Exception as e:
        name = type(e).__name__
        if "NoTranscript" in name or "TranscriptsDisabled" in name or "NotTranslatable" in name:
            return False  # definitively no transcript
        return None  # network/other error — fall through


def _get_transcript_ytdlp(video_id, retries=3):
    """Tier 2: yt-dlp with cookies — retries on 429, never stops batch."""
    import subprocess, glob, tempfile
    url = f"https://www.youtube.com/watch?v={video_id}"
    cookies_arg = ["--cookies", str(COOKIES_FILE)] if COOKIES_FILE.exists() else []

    for attempt in range(retries):
        with tempfile.TemporaryDirectory() as tmpdir:
            cmd = [
                "python3", "-m", "yt_dlp",
                "--write-auto-sub", "--skip-download",
                "--sub-lang", "en", "--sub-format", "vtt",
                *cookies_arg,
                "--quiet", "--no-warnings",
                "-o", f"{tmpdir}/%(id)s", url
            ]
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=90)
            except subprocess.TimeoutExpired:
                if attempt < retries - 1:
                    time.sleep(30)
                    continue
                return None

            stderr = result.stderr
            if "429" in stderr or "Too Many Requests" in stderr:
                if attempt < retries - 1:
                    wait = 60 * (attempt + 1)
                    print(f"    yt-dlp 429 — sleeping {wait}s (attempt {attempt+1}/{retries})")
                    time.sleep(wait)
                    continue
                return None  # all retries exhausted — leave as pending

            if "no subtitles" in stderr.lower() or "no captions" in result.stdout.lower():
                return False

            files = glob.glob(f"{tmpdir}/*.vtt")
            if not files:
                return False
            return _parse_vtt(files[0])

    return None


def _get_transcript_playwright(video_id):
    """Tier 3: CDP → real Chrome session — native clicks bypass trusted-gesture guard."""
    import asyncio, re as _re
    from playwright.async_api import async_playwright

    async def _run():
        url = f"https://www.youtube.com/watch?v={video_id}&hl=en"
        async with async_playwright() as p:
            cdp_url = "http://127.0.0.1:9223"
            use_cdp = False
            try:
                import urllib.request
                urllib.request.urlopen(f"{cdp_url}/json/version", timeout=2)
                use_cdp = True
            except Exception:
                pass

            if use_cdp:
                browser = await p.chromium.connect_over_cdp(cdp_url)
                context = browser.contexts[0] if browser.contexts else await browser.new_context()
                page = await context.new_page()
            else:
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context(
                    locale="en-US",
                    extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
                )
                page = await context.new_page()
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(3)

                # Step 1: native click "More actions" (trusted gesture — JS click won't work)
                try:
                    more_btn = page.locator(
                        'button[aria-label="More actions"], button[aria-label="More"]'
                    ).first
                    await more_btn.wait_for(state="visible", timeout=5000)
                    await more_btn.click(force=True)
                except Exception:
                    buttons = page.locator('button')
                    count = await buttons.count()
                    clicked = False
                    for i in range(count):
                        btn = buttons.nth(i)
                        label = await btn.get_attribute('aria-label') or ''
                        if 'more' in label.lower():
                            try:
                                await btn.click(force=True)
                                clicked = True
                                break
                            except Exception:
                                continue
                    if not clicked:
                        return False

                await asyncio.sleep(1.5)

                # Step 2: native click "Show transcript" / "Transcript" in dropdown
                try:
                    transcript_item = page.locator(
                        'tp-yt-paper-item, ytd-menu-service-item-renderer, yt-formatted-string'
                    ).filter(has_text="transcript").first
                    await transcript_item.wait_for(state="visible", timeout=5000)
                    await transcript_item.click(force=True)
                except Exception:
                    return False

                await asyncio.sleep(2)

                try:
                    await page.wait_for_selector("ytd-transcript-segment-renderer", timeout=8000)
                except Exception:
                    panel_text = await page.evaluate("""() => {
                        const panel = document.querySelector(
                            'ytd-transcript-renderer, ytd-engagement-panel-section-list-renderer'
                        );
                        return panel ? panel.innerText : '';
                    }""")
                    if not _re.search(r'\d+:\d+', panel_text):
                        return False

                segments = await page.evaluate("""() => {
                    const segs = document.querySelectorAll('ytd-transcript-segment-renderer');
                    return Array.from(segs).map(s => {
                        const textEl = s.querySelector('.segment-text, [class*="segment-text"]');
                        return textEl ? textEl.innerText.trim() : s.innerText.trim();
                    }).filter(t => t.length > 0);
                }""")

                if not segments or len(segments) < 5:
                    return False

                joined = " ".join(segments)
                joined = _re.sub(r'\s{2,}', ' ', joined).strip()
                return joined if len(joined) > 500 else False

            except Exception as e:
                print(f" playwright err: {e}", end=" ")
                return None
            finally:
                await page.close()
                if not use_cdp:
                    await browser.close()

    try:
        return asyncio.run(_run())
    except Exception as e:
        print(f" playwright fatal: {e}", end=" ")
        return None


def get_transcript(video_id):
    """Three-tier transcript fetch. Never stops the batch on IP block."""
    # Tier 1 — youtube-transcript-api
    result = _get_transcript_ytapi(video_id)
    if result is not None:
        return result

    # Tier 2 — yt-dlp with retry
    print("    ytapi miss — trying yt-dlp...", end=" ", flush=True)
    result = _get_transcript_ytdlp(video_id)
    if result is not None:
        return result

    # Tier 3 — Playwright browser DOM extraction (bypasses timedtext rate limit)
    print("    yt-dlp miss — trying Playwright...", end=" ", flush=True)
    result = _get_transcript_playwright(video_id)
    if result is not None:
        return result

    # All tiers failed — leave video as pending, continue
    print("    WARN: all transcript tiers failed — leaving as pending")
    return "SKIP_KEEP_PENDING"


def call_llm(prompt):
    """Call Claude Haiku for summarization."""
    import anthropic
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}],
    )
    return msg.content[0].text


def parse_summary_metadata(summary_text):
    """Extract topics list and guest from summary."""
    topics = []
    guest = None

    topics_match = re.search(r"topics:\s*(.+)", summary_text, re.IGNORECASE)
    if topics_match:
        raw = topics_match.group(1).strip().rstrip(".")
        topics = [t.strip() for t in raw.split(",") if t.strip() in TOPIC_FILES]

    guest_match = re.search(r"guest:\s*(.+)", summary_text, re.IGNORECASE)
    if guest_match:
        val = guest_match.group(1).strip().rstrip(".").strip("*").strip()
        if val.lower() not in ("none", "n/a", "-", "", "none.", "*none*"):
            guest = val

    return topics, guest


def clean_summary(summary_text):
    """Remove the metadata footer from the summary (it's for internal use only)."""
    # Remove the ## Topics section and everything after it
    cleaned = re.sub(r"\n## Topics\n[\s\S]*$", "", summary_text).strip()
    return cleaned


def safe_folder_name(title):
    name = title.replace("&", "and").replace("|", "-").replace("#", "").replace("%", "")
    name = re.sub(r'[<>:"/\\?*]', "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def write_transcript_md(folder, video_id, title, transcript_text):
    content = f"""---
title: "Transcript: {title}"
search:
  exclude: true
---

# Transcript: {title}

**YouTube:** [Watch on YouTube](https://www.youtube.com/watch?v={video_id})

---

{transcript_text}
"""
    (folder / "transcript.md").write_text(content, encoding="utf-8")


def write_summary_md(folder, summary_text):
    (folder / "summary.md").write_text(summary_text, encoding="utf-8")


def append_to_map(num, title, video_id, folder_name):
    line = f"- [{title}](Episodes/{folder_name}/summary.md) — [▶](https://www.youtube.com/watch?v={video_id})\n"
    with open(MAP_FILE, "a", encoding="utf-8") as f:
        f.write(line)


def append_to_topic(topic_slug, num, title, video_id, folder_name, insight):
    topic_file = TOPIC_FILES[topic_slug]
    entry = f"\n### [{title}](../Episodes/{folder_name}/summary.md)\n\n{insight}\n\n[▶ Watch](https://www.youtube.com/watch?v={video_id})\n"
    with open(topic_file, "a", encoding="utf-8") as f:
        f.write(entry)


def append_to_guests(guest_name, title, video_id, folder_name):
    entry = f"\n## {guest_name}\n\n**Video:** [{title}](Episodes/{folder_name}/summary.md) — [▶ Watch](https://www.youtube.com/watch?v={video_id})\n"
    with open(GUESTS_FILE, "a", encoding="utf-8") as f:
        f.write(entry)


def extract_overview(summary_text):
    m = re.search(r"## Overview\n\n(.+?)(?=\n##|\Z)", summary_text, re.DOTALL)
    if m:
        text = m.group(1).strip()
        # Return first 2 sentences
        sentences = re.split(r"(?<=[.!?])\s+", text)
        return " ".join(sentences[:2]).strip()
    return ""


def process_video(video, dry_run=False):
    num = video["num"]
    video_id = video["id"]
    title = video["title"]
    folder_name = f"{num:03d} - {safe_folder_name(title)}"
    folder = EPISODES_DIR / folder_name

    print(f"\n{'='*60}")
    print(f"[{num}/505] {title} ({video_id})")
    print(f"  Folder: Episodes/{folder_name}")

    if dry_run:
        print("  DRY RUN — skipping")
        return True

    # 1. Fetch transcript
    print("  Fetching transcript...", end=" ", flush=True)
    transcript = get_transcript(video_id)
    if transcript is False:
        print("NO TRANSCRIPT — skipping")
        return False
    if transcript == "SKIP_KEEP_PENDING":
        return "pending"  # leave status unchanged, continue batch
    print(f"OK ({len(transcript):,} chars)")

    # 2. Generate summary
    print("  Generating summary via Gemini...", end=" ", flush=True)
    prompt = SUMMARY_PROMPT.format(
        title=title,
        video_id=video_id,
        transcript=transcript[:60000],  # Cap at 60k chars (~15k tokens)
    )
    summary_raw = call_llm(prompt)
    print("OK")

    # 3. Parse metadata
    topics, guest = parse_summary_metadata(summary_raw)
    summary_clean = clean_summary(summary_raw)
    overview = extract_overview(summary_clean)

    print(f"  Topics: {topics or ['(none)']}")
    if guest:
        print(f"  Guest: {guest}")

    # 4. Write files
    folder.mkdir(parents=True, exist_ok=True)
    write_transcript_md(folder, video_id, title, transcript)
    write_summary_md(folder, summary_clean)
    print(f"  Written: Episodes/{folder_name}/summary.md + transcript.md")

    # 5. Update MAP.md
    append_to_map(num, title, video_id, folder_name)

    # 6. Update topic Conclusions files
    for topic_slug in topics:
        append_to_topic(topic_slug, num, title, video_id, folder_name, overview)

    # 7. Update GUESTS.md if interview
    if guest:
        append_to_guests(guest, title, video_id, folder_name)

    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=1, help="Max videos to process (default: 1)")
    parser.add_argument("--video", type=str, help="Process specific video ID")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--delay", type=float, default=3.0, help="Seconds between videos")
    args = parser.parse_args()

    if not GEMINI_API_KEY and not ANTHROPIC_API_KEY and not args.dry_run:
        print("ERROR: No API key set. Set GEMINI_API_KEY or ANTHROPIC_API_KEY.")
        sys.exit(1)

    data = load_progress()
    videos = data["videos"]

    if args.video:
        targets = [v for v in videos if v["id"] == args.video]
    else:
        targets = [v for v in videos if v["status"] == "pending"][:args.limit]

    if not targets:
        print("No pending videos.")
        return

    print(f"Processing {len(targets)} video(s)...")
    done = 0
    skipped = 0

    for i, video in enumerate(targets):
        success = process_video(video, dry_run=args.dry_run)

        if not args.dry_run:
            if success is True:
                video["status"] = "done"
                video["processed"] = datetime.now().strftime("%Y-%m-%d")
                done += 1
            elif success == "pending":
                # All tiers failed — leave as pending, continue batch
                skipped += 1
            elif success is None:
                # Unexpected — leave as pending, continue
                skipped += 1
            else:
                video["status"] = "no-transcript"
                skipped += 1
            data["last_updated"] = datetime.now().strftime("%Y-%m-%d")
            save_progress(data)

        if i < len(targets) - 1:
            time.sleep(args.delay)

    pending = sum(1 for v in videos if v["status"] == "pending")
    print(f"\nDone. Processed: {done}, Skipped: {skipped}, Remaining: {pending}/505")


if __name__ == "__main__":
    main()
