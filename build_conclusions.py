#!/usr/bin/env python3
"""
build_conclusions.py — Rebuild all 15 Conclusions files from scratch.

Reads every done episode summary, classifies it into 1–3 of the 15 user-defined
categories using keyword matching + old-topic fallback, then writes the Conclusions
files. Safe to re-run at any time — completely overwrites previous output.

Usage:
  python3 build_conclusions.py
"""

import json
import re
from pathlib import Path

BASE = Path(__file__).parent
PROGRESS_FILE = BASE / "PROGRESS.json"
DOCS_DIR = BASE / "docs"
EPISODES_DIR = DOCS_DIR / "Episodes"
CONCLUSIONS_DIR = DOCS_DIR / "Conclusions"
CONCLUSIONS_DIR.mkdir(exist_ok=True)

# ── Category definitions ─────────────────────────────────────────────────────

CATEGORIES = [
    {
        "slug": "foundational",
        "label": "Foundational",
        "icon": "🧱",
        "overview": (
            "Leo Gura's foundational teachings form the philosophical bedrock of Actualized.org — "
            "covering consciousness, self-actualization, epistemology, Spiral Dynamics, and the "
            "nature of reality itself. These videos build the mental models required to understand "
            "everything else Leo teaches. If you're new to Actualized.org, start here."
        ),
        "keywords": [
            "spiral dynamics", "epistemology", "philosophy", "paradigm", "worldview",
            "self-actualization", "personal development", "actualized.org", "reality",
            "holism", "holistic", "metaphysics", "nonduality", "non-dual", "absolute",
            "infinity", "truth", "what is", "nature of", "understanding", "deconstructing",
            "model of", "big picture", "how reality", "structure of reality",
        ],
        "topics": ["philosophy-and-epistemology", "spiral-dynamics", "self-actualization-and-personal-development"],
    },
    {
        "slug": "happiness",
        "label": "Happiness",
        "icon": "😊",
        "overview": (
            "What is happiness, and how do you actually get it? Leo dissects the psychology of "
            "happiness, separating shallow pleasures from deep fulfillment. These episodes challenge "
            "conventional wisdom about what makes life good and offer a more mature model of joy, "
            "contentment, and positive emotion."
        ),
        "keywords": [
            "happiness", "happy", "joy", "positive thinking", "positivity", "wellbeing",
            "well-being", "contentment", "fulfillment", "gratitude", "pleasure", "delight",
            "feel good", "optimism", "optimistic",
        ],
        "topics": [],
    },
    {
        "slug": "motivation",
        "label": "Motivation",
        "icon": "🔥",
        "overview": (
            "Motivation is misunderstood. Leo goes deep into why people procrastinate, lose drive, "
            "or chase the wrong goals — and what it actually takes to build a life fueled by genuine "
            "purpose. These episodes cover goal-setting, life purpose, ambition, and the psychology "
            "of sustained drive."
        ),
        "keywords": [
            "motivation", "motivat", "procrastinat", "ambition", "drive", "persistence",
            "goal setting", "goal-setting", "dreams", "vision", "life purpose", "purpose",
            "hunger", "hungry", "staying hungry", "passionate", "passion",
        ],
        "topics": ["life-purpose-and-career"],
    },
    {
        "slug": "productivity",
        "label": "Productivity",
        "icon": "⚡",
        "overview": (
            "High performance isn't about working harder — it's about understanding your own mind. "
            "Leo's productivity teachings cover habits, routines, discipline, focus, and the "
            "psychology of getting things done. These episodes will transform how you structure "
            "your days and approach your work."
        ),
        "keywords": [
            "productiv", "habit", "routine", "discipline", "time management", "efficiency",
            "results", "workaholic", "delegate", "focus", "concentration", "get shit done",
            "effective", "prolific", "systems", "infrastructure", "peak performance",
            "backsliding", "stop procrastinat",
        ],
        "topics": [],
    },
    {
        "slug": "money",
        "label": "Money",
        "icon": "💰",
        "overview": (
            "Leo's perspective on money, business, and success cuts through both naive positivity "
            "and cynical materialism. These episodes explore the psychology of wealth, what it "
            "actually takes to become financially free, and why most people's relationship with "
            "money keeps them stuck."
        ),
        "keywords": [
            "money", "wealth", "financial", "business", "entrepreneur", "income", "millionaire",
            "rich", "passive income", "career", "job", "work", "success", "how to start a business",
            "bootstrap", "value", "true value", "marketing", "selling", "resume",
        ],
        "topics": ["money-success-and-business"],
    },
    {
        "slug": "emotions",
        "label": "Emotions",
        "icon": "🌊",
        "overview": (
            "Emotions run your life more than logic ever will. Leo digs into the mechanics of "
            "anger, fear, anxiety, jealousy, guilt, and shame — explaining why they arise and "
            "how to work with them rather than against them. These episodes offer a mature, "
            "psychologically sophisticated approach to emotional life."
        ),
        "keywords": [
            "emotion", "anger", "fear", "anxiety", "stress", "jealous", "guilt", "shame",
            "feeling", "emotional", "vulnerability", "trauma", "crying", "emotional intelligence",
            "negative emotion", "strong emotion", "moralizing", "victim", "blame",
        ],
        "topics": ["psychology-and-emotions"],
    },
    {
        "slug": "confidence",
        "label": "Confidence",
        "icon": "💪",
        "overview": (
            "Confidence is not something you're born with — it's built. Leo breaks down self-esteem, "
            "self-image, and the deep psychological roots of insecurity. These episodes cover "
            "masculinity, assertiveness, shyness, and how to develop genuine unshakeable confidence "
            "from the inside out."
        ),
        "keywords": [
            "confidence", "self-esteem", "self-image", "insecur", "shy", "shyness",
            "self-worth", "self-doubt", "assertiv", "masculine", "alpha", "be a man",
            "how to be a man", "extrovert", "introvert", "social anxiety",
        ],
        "topics": [],
    },
    {
        "slug": "psychedelics",
        "label": "Psychedelics",
        "icon": "🍄",
        "overview": (
            "Leo is one of the most serious and rigorous public researchers of psychedelics for "
            "consciousness work. These episodes cover trip reports, safety protocols, the mechanism "
            "of action, and why psychedelics are not recreational — they are tools for confronting "
            "the deepest nature of reality. Not for the faint of heart."
        ),
        "keywords": [
            "psychedelic", "mushroom", "dmt", "5-meo", "lsd", "al-lad", "psilocybin",
            "trip", "ketamine", "mdma", "ayahuasca", "drug", "trip report",
        ],
        "topics": ["psychedelics-and-spirituality"],
    },
    {
        "slug": "enlightenment-and-meditation",
        "label": "Enlightenment & Meditation",
        "icon": "🔮",
        "overview": (
            "The core thread of Actualized.org is consciousness work — the direct investigation "
            "of what you are at the deepest level. These episodes cover enlightenment, meditation, "
            "non-duality, God-realization, awakening, and the practical methods for doing this "
            "work. This is Leo at his most serious and most controversial."
        ),
        "keywords": [
            "enlighten", "meditation", "mindfulness", "awakening", "god", "infinity",
            "spiritual", "nondual", "non-dual", "awareness", "being", "presence", "zen",
            "consciousness work", "infinite", "absolute", "god-realization", "awakened",
            "samadhi", "void", "nothingness", "pure consciousness",
        ],
        "topics": ["consciousness-and-enlightenment", "meditation-and-mindfulness"],
    },
    {
        "slug": "health-and-fitness",
        "label": "Health & Fitness",
        "icon": "🏃",
        "overview": (
            "Physical health is the foundation everything else is built on. Leo covers diet, "
            "nutrition, supplementation, fitness, and the psychology of maintaining consistent "
            "healthy habits. These episodes take a no-nonsense approach to keeping your body "
            "functioning at its best."
        ),
        "keywords": [
            "health", "diet", "nutrition", "fitness", "exercise", "supplement", "food",
            "sleep", "workout", "weight", "fat", "muscle", "detox", "vitamin", "nootropic",
            "healthy", "eat healthy", "shop for food", "smoothie", "soup",
        ],
        "topics": ["health-and-wellness"],
    },
    {
        "slug": "dating-and-relationships",
        "label": "Dating & Relationships",
        "icon": "❤️",
        "overview": (
            "Leo brings the same unflinching analysis to dating and relationships that he applies "
            "to everything else. These episodes cover attraction, love, what men and women actually "
            "want, how to build healthy relationships, and the psychology beneath surface-level "
            "dating advice. Honest, sometimes brutal, always illuminating."
        ),
        "keywords": [
            "dating", "relationship", "love", "attraction", "attract", "women", "men cheat",
            "girlfriend", "boyfriend", "marriage", "pickup", "romance", "heartbreak",
            "breakup", "partner", "get a girlfriend", "how to get laid", "pua",
            "how to be attractive", "what women want", "what men want",
        ],
        "topics": ["relationships-and-dating"],
    },
    {
        "slug": "sex",
        "label": "Sex",
        "icon": "🌹",
        "overview": (
            "Leo approaches sexuality with the same philosophical seriousness he brings to "
            "everything. These episodes cover sexual psychology, intimacy, and the deeper "
            "dimensions of sexual experience that most people never explore."
        ),
        "keywords": [
            "sex", "orgasm", "sexual", "intimacy", "get laid", "squirt", "libido",
            "masturbat", "how to have sex", "amazing sex",
        ],
        "topics": [],
    },
    {
        "slug": "depression",
        "label": "Depression",
        "icon": "🌧️",
        "overview": (
            "Depression, loneliness, and mental suffering are not character flaws — they're signals. "
            "Leo addresses the root causes of depression, addiction, and psychological suffering, "
            "going deeper than surface-level therapy to explain what's actually happening and what "
            "genuine healing looks like."
        ),
        "keywords": [
            "depress", "lonely", "loneliness", "addiction", "mental health", "therapy",
            "suicid", "grief", "overcome addiction", "toxic", "neurotic", "neurosis",
            "victim", "stuck in life",
        ],
        "topics": [],
    },
    {
        "slug": "life-skills",
        "label": "Life Skills",
        "icon": "🛠️",
        "overview": (
            "Life is a skill-set, not a lottery. Leo covers the practical competencies that "
            "separate people who thrive from people who struggle — communication, leadership, "
            "learning, critical thinking, social intelligence, and navigating the real world. "
            "These episodes are dense with actionable frameworks."
        ),
        "keywords": [
            "communication", "how to", "leadership", "social", "friend", "integrity",
            "advice", "learning", "study", "skill", "college", "wisdom", "decision",
            "critical think", "creativ", "humor", "funny", "public speaking", "openmind",
            "life advice", "giving advice", "how to deal", "how to stop", "how to make",
            "how to be", "how to get", "how to become",
        ],
        "topics": ["society-politics-and-culture"],
    },
    {
        "slug": "miscellaneous",
        "label": "Miscellaneous",
        "icon": "📦",
        "overview": (
            "Everything that doesn't fit neatly into one category — announcements, Q&As, "
            "unique one-off topics, and videos that cross so many themes they resist simple "
            "classification. Still worth watching."
        ),
        "keywords": [],
        "topics": [],
    },
]

SLUG_TO_CAT = {c["slug"]: c for c in CATEGORIES}

# Priority order for classification (most specific → least specific)
PRIORITY = [
    "sex", "psychedelics", "enlightenment-and-meditation", "health-and-fitness",
    "depression", "confidence", "money", "productivity", "motivation", "happiness",
    "dating-and-relationships", "emotions", "life-skills", "foundational", "miscellaneous",
]


# ── Parsing helpers ──────────────────────────────────────────────────────────

def parse_summary(path: Path):
    """Return dict with title, video_id, overview, insights, old_topics."""
    text = path.read_text(encoding="utf-8")

    # title from H1
    m = re.search(r"^# (.+)$", text, re.MULTILINE)
    title = m.group(1).strip() if m else path.parent.name

    # video_id from YouTube link
    m = re.search(r"youtube\.com/watch\?v=([\w-]+)", text)
    video_id = m.group(1) if m else ""

    # overview — first 2 sentences
    m = re.search(r"## Overview\n\n(.+?)(?=\n##|\Z)", text, re.DOTALL)
    if m:
        overview_full = m.group(1).strip()
        sentences = re.split(r"(?<=[.!?])\s+", overview_full)
        overview = " ".join(sentences[:2]).strip()
    else:
        overview = ""

    # key insights — first bullet
    m = re.search(r"## Key Insights\n\n(.+?)(?=\n##|\Z)", text, re.DOTALL)
    first_insight = ""
    if m:
        bullets = re.findall(r"^- \*\*[^*]+\*\*: (.+)$", m.group(1), re.MULTILINE)
        if bullets:
            first_insight = bullets[0].strip()

    # old topics from footer
    m = re.search(r"^topics:\s*\[(.+?)\]", text, re.MULTILINE | re.IGNORECASE)
    old_topics = [t.strip() for t in m.group(1).split(",")] if m else []

    return {
        "title": title,
        "video_id": video_id,
        "overview": overview,
        "first_insight": first_insight,
        "old_topics": old_topics,
    }


def classify(title: str, overview: str, old_topics: list) -> list:
    """Return list of category slugs (1–3) for an episode."""
    combined = (title + " " + overview).lower()
    matched = set()

    # Keyword matching in priority order
    for slug in PRIORITY:
        cat = SLUG_TO_CAT[slug]
        if any(kw in combined for kw in cat["keywords"]):
            matched.add(slug)
        if len(matched) >= 3:
            break

    # Old-topic fallback
    if not matched:
        topic_map = {
            "consciousness-and-enlightenment": "enlightenment-and-meditation",
            "self-actualization-and-personal-development": "foundational",
            "psychology-and-emotions": "emotions",
            "philosophy-and-epistemology": "foundational",
            "life-purpose-and-career": "motivation",
            "relationships-and-dating": "dating-and-relationships",
            "psychedelics-and-spirituality": "psychedelics",
            "society-politics-and-culture": "life-skills",
            "spiral-dynamics": "foundational",
            "meditation-and-mindfulness": "enlightenment-and-meditation",
            "health-and-wellness": "health-and-fitness",
            "money-success-and-business": "money",
        }
        for t in old_topics:
            if t in topic_map:
                matched.add(topic_map[t])

    if not matched:
        matched.add("miscellaneous")

    # Return sorted by priority
    result = [s for s in PRIORITY if s in matched]
    return result[:3]


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    with open(PROGRESS_FILE) as f:
        data = json.load(f)

    done_videos = [v for v in data["videos"] if v["status"] == "done"]
    done_videos.sort(key=lambda v: v["num"])
    print(f"Processing {len(done_videos)} done episodes...")

    # Collect entries per category
    cat_entries = {c["slug"]: [] for c in CATEGORIES}
    skipped = 0

    for video in done_videos:
        num = video["num"]
        title = video["title"]
        video_id = video["id"]

        # Find episode folder
        folder_candidates = list(EPISODES_DIR.glob(f"{num:03d} - *"))
        if not folder_candidates:
            skipped += 1
            continue
        folder = folder_candidates[0]
        summary_path = folder / "summary.md"
        if not summary_path.exists():
            skipped += 1
            continue

        parsed = parse_summary(summary_path)
        categories = classify(parsed["title"], parsed["overview"], parsed["old_topics"])

        folder_name = folder.name
        insight = parsed["overview"] or parsed["first_insight"] or f"Leo Gura discusses {title}."

        for slug in categories:
            cat_entries[slug].append({
                "num": num,
                "title": parsed["title"] or title,
                "video_id": video_id,
                "folder_name": folder_name,
                "insight": insight,
            })

    # Write Conclusions files
    for cat in CATEGORIES:
        slug = cat["slug"]
        entries = cat_entries[slug]
        lines = [
            f"# {cat['label']}",
            "",
            cat["overview"],
            "",
            f"**{len(entries)} episodes** in this category.",
            "",
            "---",
            "",
            "## Key Learnings & Conclusions",
            "",
        ]
        for e in entries:
            link = f"../Episodes/{e['folder_name']}/summary.md"
            lines.append(f"### [{e['title']}]({link})")
            lines.append("")
            lines.append(e["insight"])
            lines.append("")
            lines.append(f"[▶ Watch](https://www.youtube.com/watch?v={e['video_id']})")
            lines.append("")

        out_path = CONCLUSIONS_DIR / f"{slug}.md"
        out_path.write_text("\n".join(lines), encoding="utf-8")
        print(f"  {slug}.md — {len(entries)} episodes")

    # Write Conclusions/index.md
    index_lines = [
        "# Topic Conclusions",
        "",
        "Key learnings from Leo Gura's complete video library, aggregated by topic.",
        "",
        "---",
        "",
    ]
    for cat in CATEGORIES:
        slug = cat["slug"]
        count = len(cat_entries[slug])
        index_lines.append(f"- [{cat['icon']} {cat['label']}]({slug}.md) — {count} episodes")

    (CONCLUSIONS_DIR / "index.md").write_text("\n".join(index_lines), encoding="utf-8")
    print(f"\nDone. Skipped {skipped} episodes (folder or summary missing).")
    print(f"Conclusions/index.md written with {len(CATEGORIES)} categories.")


if __name__ == "__main__":
    main()
