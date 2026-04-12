#!/usr/bin/env python3
"""
set_mode.py — Toggle Sprint Engine between private and public mode.

    python set_mode.py public    # sanitize workspace for GitHub
    python set_mode.py private   # restore your real company data

How it works:
  - "public"  moves your real files into _private/ (gitignored) and puts
               demo content from _demo/ in their place.
  - "private" restores everything from _private/ back to original locations.

Nothing is ever deleted — only moved. Running public → private → public
leaves your data intact.
"""

import sys
import shutil
import json
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).parent
PRIVATE_DIR = ROOT / "_private"
DEMO_DIR = ROOT / "_demo"
MODE_FILE = ROOT / ".current_mode"

# ── What gets swapped (real ↔ demo) ──────────────────────────────────────────
# These files are replaced with demo versions in public mode and restored in
# private mode. Paths are relative to ROOT.
SWAPPABLE_FILES = [
    "workspace/sprint_plan.md",
    "workspace/sprint_state.json",
]

# These entire directories are swapped (real ↔ demo).
SWAPPABLE_DIRS = [
    "workspace/daily_logs",
]

# ── What gets stashed (moved away, not replaced with demo content) ────────────
# These files/dirs are moved to _private/ in public mode and not replaced.
# They will be absent from the repo in public mode (covered by .gitignore).
STASH_ONLY_FILES = [
    ".env",
    "workspace/sprint_faq.json",
    "workspace/sprint_faq.md",
]

STASH_ONLY_DIRS = [
    "workspace/generated_tables",
    "workspace/reports",
    "workspace/backups",
]

# Glob patterns for extra files to stash (e.g. backup json files in workspace/)
STASH_GLOB_PATTERNS = [
    ("workspace", "sprint_state.backup-*.json"),
    ("workspace", "*_vectors.json"),
    ("workspace", "*_events.jsonl"),
]

# ── Gitignore content ─────────────────────────────────────────────────────────

GITIGNORE_PRIVATE = """\
# Python
__pycache__/
*.py[cod]
.venv/

# Environment
.env

# macOS
.DS_Store

# Private stash (always keep out of git)
_private/

# Large generated workspace files
workspace/*_vectors.json
workspace/*_events.jsonl
workspace/sprint_state.backup-*.json
workspace/backups/
workspace/generated_tables/
workspace/reports/
workspace/daily_logs/

# Company document
*.docx

# Mode tracking
.current_mode
"""

GITIGNORE_PUBLIC = """\
# Python
__pycache__/
*.py[cod]
.venv/

# Environment — never commit real keys
.env

# macOS
.DS_Store

# Private stash — contains real company data, never committed
_private/

# Large generated workspace files
workspace/*_vectors.json
workspace/*_events.jsonl
workspace/sprint_state.backup-*.json
workspace/backups/
workspace/generated_tables/
workspace/reports/

# Company document
*.docx

# Mode tracking
.current_mode
"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def current_mode() -> str:
    if MODE_FILE.exists():
        return MODE_FILE.read_text().strip()
    return "unknown"


def write_mode(mode: str):
    MODE_FILE.write_text(mode + "\n")


def stash_path(rel: str) -> Path:
    """Return the _private/ mirror path for a given relative path."""
    return PRIVATE_DIR / rel


def log(msg: str):
    print(f"  {msg}")


def move_to_stash(rel: str):
    """Move ROOT/rel → _private/rel. Creates parent dirs as needed."""
    src = ROOT / rel
    dst = stash_path(rel)
    if not src.exists():
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src), str(dst))
    log(f"stashed   {rel}")


def restore_from_stash(rel: str):
    """Move _private/rel → ROOT/rel. Creates parent dirs as needed."""
    src = stash_path(rel)
    dst = ROOT / rel
    if not src.exists():
        log(f"[skip]    {rel}  (not in stash)")
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        if dst.is_dir():
            shutil.rmtree(dst)
        else:
            dst.unlink()
    shutil.move(str(src), str(dst))
    log(f"restored  {rel}")


def copy_demo(rel: str):
    """Copy _demo/rel → ROOT/rel. Overwrites if already present."""
    src = DEMO_DIR / rel
    dst = ROOT / rel
    if not src.exists():
        log(f"[skip]    demo/{rel}  (not found in _demo/)")
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        if dst.is_dir():
            shutil.rmtree(dst)
        else:
            dst.unlink()
    if src.is_dir():
        shutil.copytree(str(src), str(dst))
    else:
        shutil.copy2(str(src), str(dst))
    log(f"demo copy {rel}")


def remove_demo_copy(rel: str):
    """Delete ROOT/rel (the demo copy) if it exists — real version will be restored separately."""
    dst = ROOT / rel
    if not dst.exists():
        return
    if dst.is_dir():
        shutil.rmtree(dst)
    else:
        dst.unlink()


def glob_stash_files():
    """Return list of relative path strings matching STASH_GLOB_PATTERNS."""
    matches = []
    for (folder, pattern) in STASH_GLOB_PATTERNS:
        base = ROOT / folder
        if base.exists():
            for p in base.glob(pattern):
                matches.append(str(p.relative_to(ROOT)))
    return matches


# ── Mode: public ──────────────────────────────────────────────────────────────

def go_public():
    if current_mode() == "public":
        print("Already in public mode. Nothing to do.")
        return

    print("Switching to PUBLIC mode...")
    PRIVATE_DIR.mkdir(exist_ok=True)

    # 1. Swap: stash real files, copy demo versions in
    print("\n[swappable files]")
    for rel in SWAPPABLE_FILES:
        move_to_stash(rel)
        copy_demo(rel)

    print("\n[swappable dirs]")
    for rel in SWAPPABLE_DIRS:
        move_to_stash(rel)
        copy_demo(rel)

    # 2. Stash-only: move away, no replacement
    print("\n[stash-only files]")
    for rel in STASH_ONLY_FILES:
        move_to_stash(rel)

    print("\n[stash-only dirs]")
    for rel in STASH_ONLY_DIRS:
        move_to_stash(rel)

    print("\n[glob patterns]")
    for rel in glob_stash_files():
        move_to_stash(rel)

    # 3. Write public .gitignore
    (ROOT / ".gitignore").write_text(GITIGNORE_PUBLIC)
    log("wrote     .gitignore  (public profile)")

    write_mode("public")
    print("\nDone. You are now in PUBLIC mode.")
    print("Your real files are safely stored in _private/ (gitignored).")
    print("Safe to commit and push to your public GitHub repo.\n")


# ── Mode: private ─────────────────────────────────────────────────────────────

def go_private():
    if current_mode() == "private":
        print("Already in private mode. Nothing to do.")
        return

    if not PRIVATE_DIR.exists():
        print("Error: _private/ directory not found.")
        print("Have you run 'python set_mode.py public' at least once?")
        sys.exit(1)

    print("Switching to PRIVATE mode...")

    # 1. Remove demo copies, restore real files
    print("\n[swappable files]")
    for rel in SWAPPABLE_FILES:
        remove_demo_copy(rel)
        restore_from_stash(rel)

    print("\n[swappable dirs]")
    for rel in SWAPPABLE_DIRS:
        remove_demo_copy(rel)
        restore_from_stash(rel)

    # 2. Restore stash-only items
    print("\n[stash-only files]")
    for rel in STASH_ONLY_FILES:
        restore_from_stash(rel)

    print("\n[stash-only dirs]")
    for rel in STASH_ONLY_DIRS:
        restore_from_stash(rel)

    # 3. Restore glob-stashed files
    print("\n[glob patterns]")
    if PRIVATE_DIR.exists():
        for p in PRIVATE_DIR.rglob("*"):
            if p.is_file():
                rel = str(p.relative_to(PRIVATE_DIR))
                # Only restore files that match glob patterns (already moved)
                restore_from_stash(rel)

    # 4. Write private .gitignore
    (ROOT / ".gitignore").write_text(GITIGNORE_PRIVATE)
    log("wrote     .gitignore  (private profile)")

    write_mode("private")
    print("\nDone. You are now in PRIVATE mode.")
    print("All your real company files have been restored.\n")


# ── Status ────────────────────────────────────────────────────────────────────

def show_status():
    mode = current_mode()
    print(f"Current mode: {mode}")
    if mode == "public":
        print("  Workspace contains demo data. Real files are in _private/.")
    elif mode == "private":
        print("  Workspace contains your real company data.")
    else:
        print("  Mode not set. Run 'python set_mode.py public' or 'python set_mode.py private'.")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) < 2:
        show_status()
        print("\nUsage:")
        print("  python set_mode.py public    # prepare for public GitHub")
        print("  python set_mode.py private   # restore real company data")
        print("  python set_mode.py status    # show current mode")
        sys.exit(0)

    cmd = sys.argv[1].lower()
    if cmd == "public":
        go_public()
    elif cmd == "private":
        go_private()
    elif cmd == "status":
        show_status()
    else:
        print(f"Unknown command: {cmd}")
        print("Use: public | private | status")
        sys.exit(1)
