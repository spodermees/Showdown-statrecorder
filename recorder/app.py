from __future__ import annotations

import json
import os
import re
import sqlite3
import sys
import urllib.request
from urllib.parse import urlparse
from datetime import datetime
from pathlib import Path
from typing import Iterable

from flask import Flask, g, redirect, render_template, request, url_for

BASE_DIR = Path(__file__).resolve().parent


def _resolve_data_dir() -> Path:
    override = os.environ.get("RECORDER_DATA_DIR", "").strip()
    if override:
        return Path(override).expanduser()
    if getattr(sys, "frozen", False):
        local_app_data = os.environ.get("LOCALAPPDATA") or os.environ.get("APPDATA")
        if local_app_data:
            return Path(local_app_data) / "EuicStatRecorder"
    return BASE_DIR


DATA_DIR = _resolve_data_dir()
DB_PATH = DATA_DIR / "recorder.db"

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 5 * 1024 * 1024

MY_POKEMON_PRESET = [
    "Eddie bear",
    "TornWithoutOgre",
    "Does not care",
    "The healing moon",
    "The stupid cat",
    "Giraffe",
]


@app.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return response

ITEM_PATTERNS = [
    re.compile(r"(?P<actor>.+?)'s (?P<item>[A-Za-z0-9' -]+?) (?:restored|activated|went|made|triggered)", re.IGNORECASE),
    re.compile(r"(?P<actor>.+?) had its (?P<item>[A-Za-z0-9' -]+?) (?:restored|activated|used|triggered)", re.IGNORECASE),
    re.compile(r"(?P<actor>.+?) used its (?P<item>[A-Za-z0-9' -]+?)", re.IGNORECASE),
]

DAMAGE_RANGE_FROM_PATTERN = re.compile(
    r"(?P<target>.+?) lost (?P<low>\d+(?:\.\d+)?)% - (?P<high>\d+(?:\.\d+)?)%.*?from (?P<actor>.+?)'s (?P<move>.+)",
    re.IGNORECASE,
)
DAMAGE_FROM_PATTERN = re.compile(
    r"(?P<target>.+?) lost (?P<low>\d+(?:\.\d+)?)%.*?from (?P<actor>.+?)'s (?P<move>.+)",
    re.IGNORECASE,
)
DAMAGE_PATTERN = re.compile(
    r"(?P<target>.+?) lost (?P<low>\d+(?:\.\d+)?)%(?:\s*\(.*?\))?", re.IGNORECASE
)
DAMAGE_RANGE_PATTERN = re.compile(
    r"(?P<target>.+?) lost (?P<low>\d+(?:\.\d+)?)% - (?P<high>\d+(?:\.\d+)?)%", re.IGNORECASE
)

TURN_PATTERN = re.compile(r"^Turn\s+(?P<turn>\d+)", re.IGNORECASE)
MOVE_USED_PATTERN = re.compile(r"^(?P<actor>.+?) used (?P<move>.+?)!", re.IGNORECASE)
REPLAY_HP_PATTERN = re.compile(r"(?P<hp>\d+(?:\.\d+)?)\s*/\s*(?P<max>\d+(?:\.\d+)?)")
REPLAY_PERCENT_PATTERN = re.compile(r"(?P<pct>\d+(?:\.\d+)?)%")
REPLAY_URL_PATTERN = re.compile(r"https?://\S+", re.IGNORECASE)
FORMAT_PATTERN = re.compile(r"^Format:\s*(?P<format>.+)$", re.IGNORECASE)
START_PATTERN = re.compile(
    r"^Battle started between (?P<player1>.+?) and (?P<player2>.+?)!$",
    re.IGNORECASE,
)
WIN_PATTERN = re.compile(r"^(?P<winner>.+?) won the battle!$", re.IGNORECASE)
RATING_STRONG_PATTERN = re.compile(
    r"^(?P<user>.+?)'s rating:\s*(?P<before>\d+).*?<strong>(?P<after>\d+)</strong>",
    re.IGNORECASE,
)
RAW_RATING_PATTERN = re.compile(
    r"^\|raw\|\s*(?P<user>.+?)'s rating:\s*(?P<before>\d+).*?<strong>(?P<after>\d+)</strong>",
    re.IGNORECASE,
)


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
    return g.db


@app.teardown_appcontext
def close_db(_exception: Exception | None) -> None:
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db() -> None:
    db = get_db()
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        """
    )
    _ensure_column(db, "matches", "format", "TEXT")
    _ensure_column(db, "matches", "player1", "TEXT")
    _ensure_column(db, "matches", "player2", "TEXT")
    _ensure_column(db, "matches", "winner", "TEXT")
    _ensure_column(db, "matches", "result", "TEXT")
    _ensure_column(db, "matches", "replay_url", "TEXT")
    _ensure_column(db, "matches", "my_side", "TEXT")
    _ensure_column(db, "matches", "rating_user", "TEXT")
    _ensure_column(db, "matches", "rating_after", "INTEGER")
    _ensure_column(db, "matches", "team_id", "INTEGER")
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS team_pokemon (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            team_id INTEGER NOT NULL,
            nickname TEXT NOT NULL,
            species TEXT,
            source_url TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(team_id) REFERENCES prep_teams(id)
        );
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS match_nicknames (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_id INTEGER NOT NULL,
            side TEXT NOT NULL,
            nickname TEXT NOT NULL,
            FOREIGN KEY(match_id) REFERENCES matches(id)
        );
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS log_lines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_id INTEGER NOT NULL,
            turn INTEGER,
            raw_line TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(match_id) REFERENCES matches(id)
        );
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS match_state (
            match_id INTEGER PRIMARY KEY,
            last_turn INTEGER,
            last_actor TEXT,
            last_move TEXT,
            FOREIGN KEY(match_id) REFERENCES matches(id)
        );
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            actor TEXT,
            target TEXT,
            move TEXT,
            turn INTEGER,
            value_low REAL,
            value_high REAL,
            raw_line TEXT NOT NULL,
            FOREIGN KEY(match_id) REFERENCES matches(id)
        );
        """
    )
    _ensure_column(db, "events", "move", "TEXT")
    _ensure_column(db, "events", "turn", "INTEGER")
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS prep_notes (
            section TEXT PRIMARY KEY,
            content TEXT,
            updated_at TEXT NOT NULL
        );
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS prep_teams (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS prep_matchups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """
    )
    _ensure_column(db, "prep_matchups", "team_id", "INTEGER")
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS prep_matchup_notes (
            matchup_id INTEGER NOT NULL,
            section TEXT NOT NULL,
            content TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (matchup_id, section),
            FOREIGN KEY(matchup_id) REFERENCES prep_matchups(id)
        );
        """
    )
    db.commit()


@app.before_request
def _ensure_db() -> None:
    init_db()


def _ensure_column(db: sqlite3.Connection, table: str, column: str, column_type: str) -> None:
    try:
        db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")
    except sqlite3.OperationalError:
        pass


def normalize_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.casefold()).strip()


def parse_nickname_field(value: str) -> list[str]:
    if not value:
        return []
    parts = re.split(r"[\n,]+", value)
    return [part.strip() for part in parts if part.strip()]


def get_match_nicknames(match_id: int) -> dict[str, list[str]]:
    db = get_db()
    rows = db.execute(
        "SELECT side, nickname FROM match_nicknames WHERE match_id = ?",
        (match_id,),
    ).fetchall()
    nicknames: dict[str, list[str]] = {"mine": [], "opponent": []}
    for row in rows:
        nicknames.setdefault(row["side"], []).append(row["nickname"])
    return nicknames


def _detect_side_token(normalized: str) -> str | None:
    if not normalized:
        return None
    for token in normalized.split():
        if token.startswith("p1"):
            return "p1"
        if token.startswith("p2"):
            return "p2"
    return None


def classify_owner(text: str | None, nicknames: dict[str, list[str]], my_side: str | None = None) -> str | None:
    if not text:
        return None
    normalized = normalize_name(text)
    if "opposing" in normalized or "foe" in normalized:
        return "opponent"

    if my_side:
        side_token = _detect_side_token(normalized)
        if side_token:
            return "mine" if side_token == my_side else "opponent"

    for side, names in nicknames.items():
        for name in names:
            if not name:
                continue
            if normalize_name(name) and normalize_name(name) in normalized:
                return side
    return None


def infer_my_side(db: sqlite3.Connection, match_id: int, my_names: Iterable[str]) -> str | None:
    name_list = [name for name in my_names if name]
    if not name_list:
        return None
    rows = db.execute(
        "SELECT raw_line FROM log_lines WHERE match_id = ? ORDER BY id DESC LIMIT 2000",
        (match_id,),
    ).fetchall()
    p1_hits = 0
    p2_hits = 0
    for row in rows:
        normalized = normalize_name(row["raw_line"])
        side = _detect_side_token(normalized)
        if not side:
            continue
        for name in name_list:
            if normalize_name(name) and normalize_name(name) in normalized:
                if side == "p1":
                    p1_hits += 1
                elif side == "p2":
                    p2_hits += 1
                break
    if p1_hits == 0 and p2_hits == 0:
        return None
    if p1_hits == p2_hits:
        return None
    return "p1" if p1_hits > p2_hits else "p2"


def parse_log_lines(lines: Iterable[str]) -> list[dict]:
    events, _state, _lines = parse_log_stream(lines, state={})
    return events


def parse_match_meta(lines: Iterable[str]) -> dict:
    meta = {
        "format": None,
        "player1": None,
        "player2": None,
        "winner": None,
        "rating_user": None,
        "rating_after": None,
    }
    for raw in lines:
        line = raw.strip()
        if not line:
            continue
        if line.startswith("|"):
            parts = line.split("|")
            if len(parts) > 2 and parts[1] == "tier":
                meta["format"] = parts[2].strip()
            if len(parts) > 3 and parts[1] == "player":
                slot = parts[2].strip()
                name = parts[3].strip()
                if slot == "p1":
                    meta["player1"] = name
                if slot == "p2":
                    meta["player2"] = name
            if len(parts) > 2 and parts[1] == "win":
                meta["winner"] = parts[2].strip()
            if len(parts) > 2 and parts[1] == "raw":
                raw_html = parts[2].strip()
                rating_match = RATING_STRONG_PATTERN.match(raw_html)
                if rating_match:
                    meta["rating_user"] = rating_match.group("user").strip()
                    meta["rating_after"] = int(rating_match.group("after"))
            continue

        format_match = FORMAT_PATTERN.match(line)
        if format_match:
            meta["format"] = format_match.group("format").strip()
            continue

        start_match = START_PATTERN.match(line)
        if start_match:
            meta["player1"] = start_match.group("player1").strip()
            meta["player2"] = start_match.group("player2").strip()
            continue

        win_match = WIN_PATTERN.match(line)
        if win_match:
            meta["winner"] = win_match.group("winner").strip()

    return meta


def compute_result(meta: dict) -> str | None:
    winner = meta.get("winner")
    player1 = meta.get("player1")
    player2 = meta.get("player2")
    if not winner or not player1:
        return None
    if normalize_name(winner) == normalize_name(player1):
        return "Won"
    if player2 and normalize_name(winner) == normalize_name(player2):
        return "Lost"
    return None


def apply_match_meta(match_id: int, meta: dict) -> None:
    db = get_db()
    existing = db.execute(
        "SELECT format, player1, player2, winner, result, rating_user, rating_after FROM matches WHERE id = ?",
        (match_id,),
    ).fetchone()
    updated = {
        "format": meta.get("format") or (existing["format"] if existing else None),
        "player1": meta.get("player1") or (existing["player1"] if existing else None),
        "player2": meta.get("player2") or (existing["player2"] if existing else None),
        "winner": meta.get("winner") or (existing["winner"] if existing else None),
        "rating_user": meta.get("rating_user") or (existing["rating_user"] if existing else None),
        "rating_after": meta.get("rating_after") or (existing["rating_after"] if existing else None),
    }
    result = compute_result(updated)
    updated["result"] = result or (existing["result"] if existing else None)

    db.execute(
        """
        UPDATE matches
        SET format = ?, player1 = ?, player2 = ?, winner = ?, result = ?, rating_user = ?, rating_after = ?
        WHERE id = ?
        """,
        (
            updated["format"],
            updated["player1"],
            updated["player2"],
            updated["winner"],
            updated["result"],
            updated["rating_user"],
            updated["rating_after"],
            match_id,
        ),
    )
    db.commit()


def parse_log_stream(lines: Iterable[str], state: dict) -> tuple[list[dict], dict, list[dict]]:
    events: list[dict] = []
    log_lines: list[dict] = []
    current_turn = state.get("turn")
    last_actor = state.get("last_actor")
    last_move = state.get("last_move")
    hp_pct_map = state.get("hp_pct", {})

    for raw in lines:
        line = raw.strip()
        if not line:
            continue

        if line.startswith("|"):
            parsed = parse_replay_line(line)
            if parsed.get("turn") is not None:
                current_turn = parsed["turn"]
            if parsed.get("actor"):
                last_actor = parsed["actor"]
            if parsed.get("move"):
                last_move = parsed["move"]
            if parsed.get("event"):
                if parsed.get("event") == "hp_update":
                    target_key = parsed.get("target_key")
                    current_hp_pct = parsed.get("current_hp_pct")
                    if target_key and current_hp_pct is not None:
                        hp_pct_map[target_key] = current_hp_pct
                    parsed["event"] = None
                elif parsed.get("event") == "damage":
                    parsed.setdefault("actor", last_actor)
                    parsed.setdefault("move", last_move)
                    target_key = parsed.get("target_key")
                    current_hp_pct = parsed.get("current_hp_pct")
                    if target_key and current_hp_pct is not None:
                        prev_hp_pct = hp_pct_map.get(target_key)
                        hp_pct_map[target_key] = current_hp_pct
                        if prev_hp_pct is not None:
                            damage_pct = max(prev_hp_pct - current_hp_pct, 0.0)
                            parsed["value_low"] = damage_pct
                            parsed["value_high"] = damage_pct
                        else:
                            parsed["value_low"] = None
                            parsed["value_high"] = None
                if parsed.get("event"):
                    events.append(
                        {
                            "event_type": parsed["event"],
                            "actor": parsed.get("actor"),
                            "target": parsed.get("target"),
                            "move": parsed.get("move"),
                            "turn": current_turn,
                            "value_low": parsed.get("value_low"),
                            "value_high": parsed.get("value_high"),
                            "raw_line": line,
                        }
                    )

            log_lines.append(
                {
                    "turn": current_turn,
                    "raw_line": line,
                }
            )
            continue

        turn_match = TURN_PATTERN.search(line)
        if turn_match:
            current_turn = int(turn_match.group("turn"))

        used_match = MOVE_USED_PATTERN.search(line)
        if used_match:
            last_actor = used_match.group("actor").strip()
            last_move = used_match.group("move").strip()

        log_lines.append(
            {
                "turn": current_turn,
                "raw_line": line,
            }
        )

        for pattern in ITEM_PATTERNS:
            match = pattern.search(line)
            if match:
                events.append(
                    {
                        "event_type": "item",
                        "actor": match.group("actor").strip(),
                        "target": None,
                        "move": None,
                        "turn": current_turn,
                        "value_low": None,
                        "value_high": None,
                        "raw_line": line,
                    }
                )
                break

        range_from_match = DAMAGE_RANGE_FROM_PATTERN.search(line)
        if range_from_match:
            events.append(
                {
                    "event_type": "damage",
                    "actor": range_from_match.group("actor").strip(),
                    "target": range_from_match.group("target").strip(),
                    "move": range_from_match.group("move").strip(),
                    "turn": current_turn,
                    "value_low": float(range_from_match.group("low")),
                    "value_high": float(range_from_match.group("high")),
                    "raw_line": line,
                }
            )
            continue

        range_match = DAMAGE_RANGE_PATTERN.search(line)
        if range_match:
            events.append(
                {
                    "event_type": "damage",
                    "actor": last_actor,
                    "target": clean_damage_target(range_match.group("target")),
                    "move": last_move,
                    "turn": current_turn,
                    "value_low": float(range_match.group("low")),
                    "value_high": float(range_match.group("high")),
                    "raw_line": line,
                }
            )
            continue

        dmg_from_match = DAMAGE_FROM_PATTERN.search(line)
        if dmg_from_match:
            events.append(
                {
                    "event_type": "damage",
                    "actor": dmg_from_match.group("actor").strip(),
                    "target": dmg_from_match.group("target").strip(),
                    "move": dmg_from_match.group("move").strip(),
                    "turn": current_turn,
                    "value_low": float(dmg_from_match.group("low")),
                    "value_high": float(dmg_from_match.group("low")),
                    "raw_line": line,
                }
            )
            continue

        dmg_match = DAMAGE_PATTERN.search(line)
        if dmg_match:
            events.append(
                {
                    "event_type": "damage",
                    "actor": last_actor,
                    "target": clean_damage_target(dmg_match.group("target")),
                    "move": last_move,
                    "turn": current_turn,
                    "value_low": float(dmg_match.group("low")),
                    "value_high": float(dmg_match.group("low")),
                    "raw_line": line,
                }
            )

    new_state = {
        "turn": current_turn,
        "last_actor": last_actor,
        "last_move": last_move,
        "hp_pct": hp_pct_map,
    }
    return events, new_state, log_lines


def _strip_replay_prefix(value: str) -> str:
    if ": " in value:
        return value.split(": ", 1)[1].strip()
    return value.strip()


def clean_damage_target(value: str) -> str:
    cleaned = value.strip()
    cleaned = cleaned.lstrip("(")
    cleaned = cleaned.replace("The opposing ", "").replace("the opposing ", "")
    cleaned = cleaned.replace("The ", "").replace("the ", "")
    return cleaned.rstrip(")").strip()


def _parse_replay_hp(hp_text: str) -> float | None:
    if not hp_text:
        return None
    percent_match = REPLAY_PERCENT_PATTERN.search(hp_text)
    if percent_match:
        return float(percent_match.group("pct"))
    ratio_match = REPLAY_HP_PATTERN.search(hp_text)
    if ratio_match:
        hp = float(ratio_match.group("hp"))
        max_hp = float(ratio_match.group("max"))
        if max_hp > 0:
            return (hp / max_hp) * 100.0
    return None


def parse_replay_line(line: str) -> dict:
    parts = line.split("|")
    result: dict = {}
    if len(parts) < 2:
        return result

    tag = parts[1]
    if tag == "turn" and len(parts) > 2:
        try:
            result["turn"] = int(parts[2])
        except ValueError:
            pass
        return result

    if tag == "move" and len(parts) > 3:
        result["actor"] = _strip_replay_prefix(parts[2])
        result["move"] = parts[3].strip()
        if len(parts) > 4:
            result["target"] = _strip_replay_prefix(parts[4])
        return result

    if tag in {"switch", "drag", "replace"} and len(parts) > 4:
        target_key = parts[2].strip()
        hp_text = parts[4]
        pct = _parse_replay_hp(hp_text)
        if pct is not None:
            result.update(
                {
                    "event": "hp_update",
                    "target_key": target_key,
                    "current_hp_pct": pct,
                }
            )
        return result

    if tag == "-damage" and len(parts) > 3:
        target_raw = parts[2]
        target = clean_damage_target(_strip_replay_prefix(target_raw))
        hp_text = parts[3]
        pct = _parse_replay_hp(hp_text)
        if pct is not None:
            result.update(
                {
                    "event": "damage",
                    "target": target,
                    "target_key": target_raw.strip(),
                    "current_hp_pct": pct,
                    "value_low": pct,
                    "value_high": pct,
                }
            )
        return result

    return result


def get_or_create_live_match(team_id: int) -> int:
    db = get_db()
    row = db.execute(
        "SELECT id FROM matches WHERE name = ? AND team_id = ? ORDER BY id DESC LIMIT 1",
        ("Live", team_id),
    ).fetchone()
    if row:
        return row["id"]
    cursor = db.execute(
        "INSERT INTO matches (name, created_at, team_id) VALUES (?, ?, ?)",
        ("Live", datetime.utcnow().isoformat(timespec="seconds"), team_id),
    )
    match_id = cursor.lastrowid
    db.execute(
        "INSERT OR REPLACE INTO match_state (match_id, last_turn, last_actor, last_move) VALUES (?, ?, ?, ?)",
        (match_id, None, None, None),
    )
    db.commit()
    return match_id


def get_match_state(match_id: int) -> dict:
    db = get_db()
    row = db.execute(
        "SELECT last_turn, last_actor, last_move FROM match_state WHERE match_id = ?",
        (match_id,),
    ).fetchone()
    if row is None:
        db.execute(
            "INSERT OR REPLACE INTO match_state (match_id, last_turn, last_actor, last_move) VALUES (?, ?, ?, ?)",
            (match_id, None, None, None),
        )
        db.commit()
        return {"turn": None, "last_actor": None, "last_move": None}
    return {
        "turn": row["last_turn"],
        "last_actor": row["last_actor"],
        "last_move": row["last_move"],
    }


def update_match_state(match_id: int, state: dict) -> None:
    db = get_db()
    db.execute(
        "INSERT OR REPLACE INTO match_state (match_id, last_turn, last_actor, last_move) VALUES (?, ?, ?, ?)",
        (
            match_id,
            state.get("turn"),
            state.get("last_actor"),
            state.get("last_move"),
        ),
    )
    db.commit()


PREP_SECTIONS = [
    "Lead",
    "Wincon",
    "Threats",
    "Tera",
    "Speed control",
    "Items/sets",
    "Flowchart",
    "Notes",
]


def get_prep_notes() -> dict[str, str]:
    db = get_db()
    rows = db.execute("SELECT section, content FROM prep_notes").fetchall()
    return {row["section"]: row["content"] or "" for row in rows}


def save_prep_notes(payload: dict[str, str]) -> None:
    db = get_db()
    now = datetime.utcnow().isoformat(timespec="seconds")
    for section, content in payload.items():
        db.execute(
            """
            INSERT OR REPLACE INTO prep_notes (section, content, updated_at)
            VALUES (?, ?, ?)
            """,
            (section, content, now),
        )
    db.commit()


def list_prep_matchups() -> list[dict]:
    db = get_db()
    rows = db.execute(
        "SELECT id, title, updated_at FROM prep_matchups ORDER BY updated_at DESC, id DESC"
    ).fetchall()
    return [dict(row) for row in rows]


def list_prep_matchups_for_team(team_id: int) -> list[dict]:
    db = get_db()
    rows = db.execute(
        """
        SELECT id, title, updated_at
        FROM prep_matchups
        WHERE team_id = ?
        ORDER BY updated_at DESC, id DESC
        """,
        (team_id,),
    ).fetchall()
    return [dict(row) for row in rows]


def list_prep_teams() -> list[dict]:
    db = get_db()
    rows = db.execute(
        "SELECT id, name, updated_at FROM prep_teams ORDER BY updated_at DESC, id DESC"
    ).fetchall()
    return [dict(row) for row in rows]


def get_team_by_id(team_id: int) -> dict | None:
    db = get_db()
    row = db.execute(
        "SELECT id, name, updated_at FROM prep_teams WHERE id = ?",
        (team_id,),
    ).fetchone()
    return dict(row) if row else None


def get_or_create_default_team() -> dict:
    db = get_db()
    row = db.execute(
        "SELECT id, name, updated_at FROM prep_teams ORDER BY updated_at DESC, id DESC LIMIT 1"
    ).fetchone()
    if row:
        return dict(row)
    now = datetime.utcnow().isoformat(timespec="seconds")
    cursor = db.execute(
        "INSERT INTO prep_teams (name, created_at, updated_at) VALUES (?, ?, ?)",
        ("Team 1", now, now),
    )
    db.commit()
    return {"id": cursor.lastrowid, "name": "Team 1", "updated_at": now}


def backfill_prep_matchups_team(team_id: int) -> None:
    db = get_db()
    db.execute(
        "UPDATE prep_matchups SET team_id = ? WHERE team_id IS NULL",
        (team_id,),
    )
    db.commit()


def backfill_matches_team(team_id: int) -> None:
    db = get_db()
    db.execute(
        "UPDATE matches SET team_id = ? WHERE team_id IS NULL",
        (team_id,),
    )
    db.commit()


def resolve_team_id(raw_value: object | None) -> int:
    default_team = get_or_create_default_team()
    backfill_prep_matchups_team(default_team["id"])
    backfill_matches_team(default_team["id"])
    raw = str(raw_value or "").strip()
    if raw.isdigit():
        team = get_team_by_id(int(raw))
        if team:
            return team["id"]
    return default_team["id"]


def mark_team_active(team_id: int) -> None:
    db = get_db()
    now = datetime.utcnow().isoformat(timespec="seconds")
    db.execute(
        "UPDATE prep_teams SET updated_at = ? WHERE id = ?",
        (now, team_id),
    )
    db.commit()


def list_team_pokemon(team_id: int) -> list[dict]:
    db = get_db()
    rows = db.execute(
        """
        SELECT nickname, species, source_url, created_at
        FROM team_pokemon
        WHERE team_id = ?
        ORDER BY id DESC
        """,
        (team_id,),
    ).fetchall()
    return [dict(row) for row in rows]


def save_team_pokemon(team_id: int, entries: list[dict], source_url: str | None) -> None:
    db = get_db()
    db.execute("DELETE FROM team_pokemon WHERE team_id = ?", (team_id,))
    now = datetime.utcnow().isoformat(timespec="seconds")
    if entries:
        db.executemany(
            """
            INSERT INTO team_pokemon (team_id, nickname, species, source_url, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (
                    team_id,
                    entry.get("nickname", ""),
                    entry.get("species"),
                    source_url,
                    now,
                )
                for entry in entries
                if entry.get("nickname")
            ],
        )
    db.commit()


def _normalize_pokepaste_url(url: str) -> str | None:
    value = url.strip()
    if not value:
        return None
    parsed = urlparse(value)
    if not parsed.netloc:
        return None
    if "pokepast.es" not in parsed.netloc:
        return None
    if parsed.path.endswith("/raw"):
        return value
    return value.rstrip("/") + "/raw"


def _parse_pokepaste_nicknames(raw_text: str) -> list[dict]:
    results: list[dict] = []
    header_pattern = re.compile(r"^(?P<name>.+?)\s+@\s+.+$")
    nickname_pattern = re.compile(r"^(?P<nick>.+?)\s+\((?P<species>[^)]+)\)$")
    for line in raw_text.splitlines():
        line = line.strip()
        if not line:
            continue
        header_match = header_pattern.match(line)
        if not header_match:
            continue
        name_part = header_match.group("name").strip()
        nick_match = nickname_pattern.match(name_part)
        if nick_match:
            nickname = nick_match.group("nick").strip()
            species = nick_match.group("species").strip()
            if nickname and nickname != species:
                results.append({"nickname": nickname, "species": species})
        else:
            # No nickname, skip because user wants nicknames only
            continue
    return results


def get_prep_matchup_notes(matchup_id: int) -> dict[str, str]:
    db = get_db()
    rows = db.execute(
        "SELECT section, content FROM prep_matchup_notes WHERE matchup_id = ?",
        (matchup_id,),
    ).fetchall()
    return {row["section"]: row["content"] or "" for row in rows}


@app.route("/")
def index():
    db = get_db()
    active_team_id = resolve_team_id(request.args.get("team_id"))
    active_team = get_team_by_id(active_team_id)
    if active_team:
        mark_team_active(active_team_id)
        active_team = get_team_by_id(active_team_id)
    teams = list_prep_teams()
    team_pokemon = list_team_pokemon(active_team_id)

    matches = db.execute(
        """
        SELECT id, name, created_at, format, winner, result, replay_url
        FROM matches
        WHERE team_id = ?
        ORDER BY id DESC
        """,
        (active_team_id,),
    ).fetchall()
    totals = db.execute(
        """
        SELECT COUNT(*) AS total_events
        FROM events
        INNER JOIN matches ON matches.id = events.match_id
        WHERE matches.team_id = ?
        """,
        (active_team_id,),
    ).fetchone()
    damage_stats = db.execute(
        """
        SELECT
            COUNT(*) AS total_hits,
            MIN(events.value_low) AS min_damage,
            MAX(events.value_high) AS max_damage,
            AVG((events.value_low + events.value_high) / 2.0) AS avg_damage
        FROM events
        INNER JOIN matches ON matches.id = events.match_id
        WHERE matches.team_id = ?
          AND events.event_type = 'damage'
          AND events.value_low IS NOT NULL
          AND events.value_high IS NOT NULL
        """,
        (active_team_id,),
    ).fetchone()
    attacker = request.args.get("attacker", "").strip()
    defender = request.args.get("defender", "").strip()

    name_rows = db.execute(
        """
        SELECT events.actor, events.target
        FROM events
        INNER JOIN matches ON matches.id = events.match_id
        WHERE matches.team_id = ?
          AND events.event_type = 'damage'
          AND (events.actor IS NOT NULL OR events.target IS NOT NULL)
        """,
        (active_team_id,),
    ).fetchall()
    unique_names = sorted(
        {
            name
            for row in name_rows
            for name in (row["actor"], row["target"])
            if name
        }
    )

    damage_lookup = None
    if attacker and defender:
        damage_rows = db.execute(
            """
            SELECT events.actor, events.target, events.move, events.value_low, events.value_high, matches.replay_url
            FROM events
            INNER JOIN matches ON matches.id = events.match_id
            WHERE matches.team_id = ?
              AND events.event_type = 'damage'
              AND events.value_low IS NOT NULL
              AND events.value_high IS NOT NULL
            """,
            (active_team_id,),
        ).fetchall()

        def build_breakdown(attacker_name: str, defender_name: str) -> list[dict]:
            buckets: dict[str, dict] = {}
            for row in damage_rows:
                if not row["actor"] or not row["target"]:
                    continue
                if normalize_name(row["actor"]) != normalize_name(attacker_name):
                    continue
                if normalize_name(row["target"]) != normalize_name(defender_name):
                    continue
                move = row["move"] or "-"
                key = move
                entry = buckets.get(key)
                if not entry:
                    entry = {
                        "move": move,
                        "min_low": row["value_low"],
                        "max_high": row["value_high"],
                        "sum_mid": 0.0,
                        "count": 0,
                        "replay_url": row["replay_url"],
                    }
                    buckets[key] = entry
                entry["count"] += 1
                if row["value_low"] is not None:
                    entry["min_low"] = min(entry["min_low"], row["value_low"])
                if row["value_high"] is not None:
                    entry["max_high"] = max(entry["max_high"], row["value_high"])
                if not entry["replay_url"] and row["replay_url"]:
                    entry["replay_url"] = row["replay_url"]
                if row["value_low"] is not None and row["value_high"] is not None:
                    entry["sum_mid"] += (row["value_low"] + row["value_high"]) / 2.0

            for entry in buckets.values():
                if entry["count"]:
                    entry["avg"] = entry["sum_mid"] / entry["count"]
                else:
                    entry["avg"] = 0.0
                entry.pop("sum_mid", None)

            return sorted(buckets.values(), key=lambda item: item["max_high"], reverse=True)

        forward = build_breakdown(attacker, defender)
        reverse = build_breakdown(defender, attacker)
        damage_lookup = {
            "attacker": attacker,
            "defender": defender,
            "forward": forward,
            "reverse": reverse,
        }

    attacker_options = MY_POKEMON_PRESET
    opponent_options = [name for name in unique_names if name not in MY_POKEMON_PRESET]
    if not opponent_options:
        opponent_options = unique_names

    return render_template(
        "index.html",
        matches=matches,
        totals=totals,
        damage_stats=damage_stats,
        attacker=attacker,
        defender=defender,
        damage_lookup=damage_lookup,
        attacker_options=attacker_options,
        opponent_options=opponent_options,
        teams=teams,
        active_team=active_team,
        team_pokemon=team_pokemon,
    )


@app.route("/prep", methods=["GET", "POST"])
def prep():
    active_team_id = resolve_team_id(request.args.get("team_id"))
    active_team = get_team_by_id(active_team_id)
    if active_team:
        mark_team_active(active_team_id)
        active_team = get_team_by_id(active_team_id)
    matchups = list_prep_matchups_for_team(active_team_id)
    return render_template(
        "prep.html",
        sections=PREP_SECTIONS,
        matchups=matchups,
        active_team=active_team,
    )


@app.route("/api/prep_matchups", methods=["POST"])
def api_create_prep_matchup():
    data = request.get_json(silent=True) or {}
    title = str(data.get("title", "")).strip()
    team_id_raw = str(data.get("team_id", "")).strip()
    if not title:
        return {"ok": False, "error": "missing title"}, 400
    db = get_db()
    default_team = get_or_create_default_team()
    backfill_prep_matchups_team(default_team["id"])
    team_id = default_team["id"]
    if team_id_raw.isdigit():
        team = get_team_by_id(int(team_id_raw))
        if team:
            team_id = team["id"]
    now = datetime.utcnow().isoformat(timespec="seconds")
    cursor = db.execute(
        "INSERT INTO prep_matchups (title, updated_at, team_id) VALUES (?, ?, ?)",
        (title, now, team_id),
    )
    db.commit()
    return {"ok": True, "id": cursor.lastrowid, "title": title, "team_id": team_id}


@app.route("/api/prep_teams", methods=["GET", "POST"])
def api_create_prep_team():
    if request.method == "GET":
        teams = list_prep_teams()
        return {"ok": True, "teams": teams}

    data = request.get_json(silent=True) or {}
    name = str(data.get("name", "")).strip()
    if not name:
        return {"ok": False, "error": "missing name"}, 400
    db = get_db()
    now = datetime.utcnow().isoformat(timespec="seconds")
    cursor = db.execute(
        "INSERT INTO prep_teams (name, created_at, updated_at) VALUES (?, ?, ?)",
        (name, now, now),
    )
    db.commit()
    return {"ok": True, "id": cursor.lastrowid, "name": name}


@app.route("/api/team_pokepaste", methods=["POST"])
def api_team_pokepaste():
    data = request.get_json(silent=True) or {}
    url_value = str(data.get("url", "")).strip()
    team_id = resolve_team_id(data.get("team_id"))
    raw_url = _normalize_pokepaste_url(url_value)
    if not raw_url:
        return {"ok": False, "error": "invalid url"}, 400

    try:
        req = urllib.request.Request(
            raw_url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Accept": "text/plain,*/*;q=0.8",
            },
        )
        with urllib.request.urlopen(req, timeout=10) as response:
            raw_text = response.read().decode("utf-8", errors="ignore")
    except Exception:
        return {"ok": False, "error": "failed to fetch"}, 502

    nicknames = _parse_pokepaste_nicknames(raw_text)
    if not nicknames:
        return {"ok": False, "error": "no nicknames found"}, 400

    save_team_pokemon(team_id, nicknames, url_value)
    return {"ok": True, "count": len(nicknames), "nicknames": nicknames}


@app.route("/api/prep_matchups/<int:matchup_id>", methods=["GET"])
def api_get_prep_matchup(matchup_id: int):
    db = get_db()
    row = db.execute(
        "SELECT id, title, updated_at FROM prep_matchups WHERE id = ?",
        (matchup_id,),
    ).fetchone()
    if row is None:
        return {"ok": False, "error": "not found"}, 404
    notes = get_prep_matchup_notes(matchup_id)
    return {
        "ok": True,
        "matchup": dict(row),
        "notes": notes,
    }


@app.route("/api/prep_matchups/<int:matchup_id>", methods=["POST"])
def api_update_prep_matchup(matchup_id: int):
    data = request.get_json(silent=True) or {}
    notes = data.get("notes") or {}
    title = str(data.get("title", "")).strip()
    db = get_db()
    now = datetime.utcnow().isoformat(timespec="seconds")

    existing = db.execute(
        "SELECT id FROM prep_matchups WHERE id = ?",
        (matchup_id,),
    ).fetchone()
    if existing is None:
        return {"ok": False, "error": "not found"}, 404

    if title:
        db.execute(
            "UPDATE prep_matchups SET title = ?, updated_at = ? WHERE id = ?",
            (title, now, matchup_id),
        )
    else:
        db.execute(
            "UPDATE prep_matchups SET updated_at = ? WHERE id = ?",
            (now, matchup_id),
        )

    for section, content in notes.items():
        if section not in PREP_SECTIONS:
            continue
        db.execute(
            """
            INSERT OR REPLACE INTO prep_matchup_notes (matchup_id, section, content, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            (matchup_id, section, str(content).strip(), now),
        )
    db.commit()
    return {"ok": True}


@app.route("/match/<int:match_id>")
def match_detail(match_id: int):
    db = get_db()
    match = db.execute(
        "SELECT id, name, created_at, my_side FROM matches WHERE id = ?", (match_id,)
    ).fetchone()
    if match is None:
        return redirect(url_for("index"))

    nicknames = get_match_nicknames(match_id)
    my_names = nicknames["mine"] or MY_POKEMON_PRESET
    my_side = match["my_side"] or infer_my_side(db, match_id, my_names)
    effective_nicknames = {
        "mine": my_names,
        "opponent": nicknames["opponent"],
    }

    events = db.execute(
        """
        SELECT event_type, actor, target, move, turn, value_low, value_high, raw_line
        FROM events
        WHERE match_id = ?
        ORDER BY id ASC
        """,
        (match_id,),
    ).fetchall()
    decorated_events = []
    for event in events:
        owner = classify_owner(event["actor"], effective_nicknames) or classify_owner(
            event["target"], effective_nicknames
        ) or classify_owner(event["raw_line"], effective_nicknames, my_side=my_side)
        decorated_events.append({**dict(event), "owner": owner})

    attacker = request.args.get("attacker", "").strip()
    defender = request.args.get("defender", "").strip()
    damage_summary = None
    if attacker and defender:
        matching = [
            event
            for event in decorated_events
            if event["event_type"] == "damage"
            and event["actor"]
            and event["target"]
            and normalize_name(event["actor"]) == normalize_name(attacker)
            and normalize_name(event["target"]) == normalize_name(defender)
        ]
        if matching:
            lows = [event["value_low"] for event in matching if event["value_low"] is not None]
            highs = [event["value_high"] for event in matching if event["value_high"] is not None]
            min_low = min(lows) if lows else 0.0
            max_high = max(highs) if highs else 0.0
            damage_summary = {
                "attacker": attacker,
                "defender": defender,
                "min_low": min_low,
                "max_high": max_high,
                "count": len(matching),
            }
        else:
            damage_summary = {
                "attacker": attacker,
                "defender": defender,
                "min_low": None,
                "max_high": None,
                "count": 0,
            }

    unique_names = sorted(
        {
            name
            for event in decorated_events
            for name in (event.get("actor"), event.get("target"))
            if name
        }
    )

    mine_options = effective_nicknames["mine"] or [
        name for name in unique_names if classify_owner(name, effective_nicknames, my_side=my_side) == "mine"
    ]
    opponent_options = effective_nicknames["opponent"] or [
        name for name in unique_names if classify_owner(name, effective_nicknames, my_side=my_side) == "opponent"
    ]
    if not mine_options:
        mine_options = unique_names
    if not opponent_options:
        opponent_options = unique_names

    return render_template(
        "match.html",
        match=match,
        events=decorated_events,
        nicknames=nicknames,
        mine_options=mine_options,
        opponent_options=opponent_options,
        attacker=attacker,
        defender=defender,
        damage_summary=damage_summary,
        my_side=my_side,
    )


@app.route("/match/<int:match_id>/side", methods=["POST"])
def update_match_side(match_id: int):
    side = (request.form.get("my_side") or "").strip().lower()
    if side not in {"p1", "p2"}:
        side = None
    db = get_db()
    db.execute(
        "UPDATE matches SET my_side = ? WHERE id = ?",
        (side, match_id),
    )
    db.commit()
    return redirect(url_for("match_detail", match_id=match_id))


@app.route("/match/<int:match_id>/log")
def match_log(match_id: int):
    db = get_db()
    match = db.execute(
        "SELECT id, name, created_at, replay_url FROM matches WHERE id = ?",
        (match_id,),
    ).fetchone()
    if match is None:
        return redirect(url_for("index"))

    log_rows = db.execute(
        "SELECT turn, raw_line FROM log_lines WHERE match_id = ? ORDER BY id ASC",
        (match_id,),
    ).fetchall()

    return render_template("log.html", match=match, log_rows=log_rows)


@app.route("/upload", methods=["POST"])
def upload_log():
    uploaded = request.files.get("log_file")

    if not uploaded or uploaded.filename == "":
        return redirect(url_for("index"))

    content = uploaded.read().decode("utf-8", errors="ignore")
    lines = content.splitlines()

    team_id = resolve_team_id(request.form.get("team_id"))
    db = get_db()
    meta = parse_match_meta(lines)
    result = compute_result(meta)
    cursor = db.execute(
        """
        INSERT INTO matches (name, created_at, format, player1, player2, winner, result, team_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "Upload",
            datetime.utcnow().isoformat(timespec="seconds"),
            meta.get("format"),
            meta.get("player1"),
            meta.get("player2"),
            meta.get("winner"),
            result,
            team_id,
        ),
    )
    match_id = cursor.lastrowid

    events, state, log_lines = parse_log_stream(lines, state={})
    if events:
        db.executemany(
            """
            INSERT INTO events (match_id, event_type, actor, target, move, turn, value_low, value_high, raw_line)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    match_id,
                    event["event_type"],
                    event["actor"],
                    event["target"],
                    event.get("move"),
                    event.get("turn"),
                    event["value_low"],
                    event["value_high"],
                    event["raw_line"],
                )
                for event in events
            ],
        )
    if log_lines:
        db.executemany(
            """
            INSERT INTO log_lines (match_id, turn, raw_line, created_at)
            VALUES (?, ?, ?, ?)
            """,
            [
                (
                    match_id,
                    entry["turn"],
                    entry["raw_line"],
                    datetime.utcnow().isoformat(timespec="seconds"),
                )
                for entry in log_lines
            ],
        )
    update_match_state(match_id, state)
    db.commit()

    return redirect(url_for("match_detail", match_id=match_id))


@app.route("/match/<int:match_id>/nicknames", methods=["POST"])
def update_nicknames(match_id: int):
    mine_field = request.form.get("mine_nicknames", "")
    opponent_field = request.form.get("opponent_nicknames", "")

    mine_names = parse_nickname_field(mine_field)
    opponent_names = parse_nickname_field(opponent_field)

    db = get_db()
    db.execute("DELETE FROM match_nicknames WHERE match_id = ?", (match_id,))
    payload = [
        (match_id, "mine", name) for name in mine_names
    ] + [
        (match_id, "opponent", name) for name in opponent_names
    ]
    if payload:
        db.executemany(
            "INSERT INTO match_nicknames (match_id, side, nickname) VALUES (?, ?, ?)",
            payload,
        )
    db.commit()
    return redirect(url_for("match_detail", match_id=match_id))


@app.route("/api/ingest", methods=["POST"])
def api_ingest():
    data = request.get_json(silent=True) or {}
    log_text = str(data.get("log", ""))
    team_id = resolve_team_id(data.get("team_id"))

    lines = log_text.splitlines()
    events, state, log_lines = parse_log_stream(lines, state={})

    meta = parse_match_meta(lines)
    result = compute_result(meta)

    db = get_db()
    cursor = db.execute(
        """
        INSERT INTO matches (name, created_at, format, player1, player2, winner, result, team_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "API Upload",
            datetime.utcnow().isoformat(timespec="seconds"),
            meta.get("format"),
            meta.get("player1"),
            meta.get("player2"),
            meta.get("winner"),
            result,
            team_id,
        ),
    )
    match_id = cursor.lastrowid

    if events:
        db.executemany(
            """
            INSERT INTO events (match_id, event_type, actor, target, move, turn, value_low, value_high, raw_line)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    match_id,
                    event["event_type"],
                    event["actor"],
                    event["target"],
                    event.get("move"),
                    event.get("turn"),
                    event["value_low"],
                    event["value_high"],
                    event["raw_line"],
                )
                for event in events
            ],
        )

    if log_lines:
        db.executemany(
            """
            INSERT INTO log_lines (match_id, turn, raw_line, created_at)
            VALUES (?, ?, ?, ?)
            """,
            [
                (
                    match_id,
                    entry["turn"],
                    entry["raw_line"],
                    datetime.utcnow().isoformat(timespec="seconds"),
                )
                for entry in log_lines
            ],
        )

    update_match_state(match_id, state)

    db.commit()
    return {"match_id": match_id, "events": len(events)}


@app.route("/api/ingest", methods=["OPTIONS"])
def api_ingest_options():
    return {"status": "ok"}


@app.route("/api/ingest_line", methods=["POST"])
def api_ingest_line():
    data = request.get_json(silent=True) or {}
    line = str(data.get("line", "")).strip()
    if not line:
        return {"status": "ignored"}

    team_id = resolve_team_id(data.get("team_id") or request.args.get("team_id"))
    match_id = get_or_create_live_match(team_id)
    state = get_match_state(match_id)
    events, new_state, log_lines = parse_log_stream([line], state=state)
    apply_match_meta(match_id, parse_match_meta([line]))

    db = get_db()
    if events:
        db.executemany(
            """
            INSERT INTO events (match_id, event_type, actor, target, move, turn, value_low, value_high, raw_line)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    match_id,
                    event["event_type"],
                    event["actor"],
                    event["target"],
                    event.get("move"),
                    event.get("turn"),
                    event["value_low"],
                    event["value_high"],
                    event["raw_line"],
                )
                for event in events
            ],
        )

    if log_lines:
        db.executemany(
            """
            INSERT INTO log_lines (match_id, turn, raw_line, created_at)
            VALUES (?, ?, ?, ?)
            """,
            [
                (
                    match_id,
                    entry["turn"],
                    entry["raw_line"],
                    datetime.utcnow().isoformat(timespec="seconds"),
                )
                for entry in log_lines
            ],
        )

    db.commit()
    update_match_state(match_id, new_state)
    return {"status": "ok", "events": len(events)}


@app.route("/api/ingest_line", methods=["OPTIONS"])
def api_ingest_line_options():
    return {"status": "ok"}


@app.route("/api/poke", methods=["POST"])
def api_poke():
    data = request.get_json(silent=True) or {}
    return {
        "status": "ok",
        "source": str(data.get("source", "extension")),
        "reason": str(data.get("reason", "poke")),
        "time": datetime.utcnow().isoformat(timespec="seconds"),
    }


@app.route("/api/poke", methods=["OPTIONS"])
def api_poke_options():
    return {"status": "ok"}


@app.route("/api/live_status")
def api_live_status():
    db = get_db()
    match_id_raw = (request.args.get("match_id") or "").strip()

    if match_id_raw.isdigit():
        match_id = int(match_id_raw)
        event_row = db.execute(
            "SELECT COALESCE(MAX(id), 0) AS max_id FROM events WHERE match_id = ?",
            (match_id,),
        ).fetchone()
        log_row = db.execute(
            "SELECT COALESCE(MAX(id), 0) AS max_id FROM log_lines WHERE match_id = ?",
            (match_id,),
        ).fetchone()
        return {
            "ok": True,
            "scope": "match",
            "match_id": match_id,
            "last_event_id": int(event_row["max_id"] if event_row else 0),
            "last_log_id": int(log_row["max_id"] if log_row else 0),
        }

    team_id = resolve_team_id(request.args.get("team_id"))
    match_row = db.execute(
        "SELECT id FROM matches WHERE team_id = ? ORDER BY id DESC LIMIT 1",
        (team_id,),
    ).fetchone()
    latest_match_id = int(match_row["id"]) if match_row else 0

    event_row = db.execute(
        """
        SELECT COALESCE(MAX(events.id), 0) AS max_id
        FROM events
        INNER JOIN matches ON matches.id = events.match_id
        WHERE matches.team_id = ?
        """,
        (team_id,),
    ).fetchone()
    log_row = db.execute(
        """
        SELECT COALESCE(MAX(log_lines.id), 0) AS max_id
        FROM log_lines
        INNER JOIN matches ON matches.id = log_lines.match_id
        WHERE matches.team_id = ?
        """,
        (team_id,),
    ).fetchone()

    return {
        "ok": True,
        "scope": "team",
        "team_id": team_id,
        "match_id": latest_match_id,
        "last_event_id": int(event_row["max_id"] if event_row else 0),
        "last_log_id": int(log_row["max_id"] if log_row else 0),
    }


@app.route("/api/showdown_rating")
def api_showdown_rating():
    username = (request.args.get("user") or "").strip()
    if not username:
        return {"ok": False, "error": "missing user"}, 400
    team_id = resolve_team_id(request.args.get("team_id"))

    db = get_db()
    rating_rows = db.execute(
        """
        SELECT rating_user, rating_after, format, created_at
        FROM matches
                WHERE rating_user IS NOT NULL AND rating_after IS NOT NULL
                    AND team_id = ?
        ORDER BY id DESC
        LIMIT 200
        """,
                (team_id,),
    ).fetchall()
    normalized_user = normalize_name(username)
    for row in rating_rows:
        if normalize_name(row["rating_user"]) == normalized_user:
            return {
                "ok": True,
                "user": row["rating_user"],
                "rating": {
                    "format": row["format"],
                    "elo": row["rating_after"],
                    "source": "match_log",
                },
            }

    raw_rows = db.execute(
        """
                SELECT log_lines.raw_line, log_lines.match_id
                FROM log_lines
                INNER JOIN matches ON matches.id = log_lines.match_id
                WHERE log_lines.raw_line LIKE '%rating:%'
                    AND matches.team_id = ?
                ORDER BY log_lines.id DESC
                LIMIT 500
        """,
                (team_id,),
    ).fetchall()
    for row in raw_rows:
        raw_line = row["raw_line"]
        rating_match = RAW_RATING_PATTERN.match(raw_line)
        if not rating_match:
            continue
        if normalize_name(rating_match.group("user")) != normalized_user:
            continue
        match_row = db.execute(
            "SELECT format FROM matches WHERE id = ?",
            (row["match_id"],),
        ).fetchone()
        return {
            "ok": True,
            "user": rating_match.group("user").strip(),
            "rating": {
                "format": match_row["format"] if match_row else None,
                "elo": int(rating_match.group("after")),
                "source": "raw_log",
            },
        }

    url = f"https://pokemonshowdown.com/users/{username}.json"
    try:
        with urllib.request.urlopen(url, timeout=8) as response:
            payload = json.loads(response.read().decode("utf-8", errors="ignore"))
    except Exception:
        return {"ok": False, "error": "failed to fetch"}, 502

    ratings = payload.get("ratings") or {}
    best = None
    for format_name, info in ratings.items():
        if not isinstance(info, dict):
            continue
        elo = info.get("elo")
        if elo is None:
            continue
        if best is None or elo > best["elo"]:
            best = {
                "format": format_name,
                "elo": elo,
                "gxe": info.get("gxe"),
                "rpr": info.get("rpr"),
                "rprd": info.get("rprd"),
            }

    return {
        "ok": True,
        "user": payload.get("user") or username,
        "rating": best,
    }


@app.route("/api/rating_history")
def api_rating_history():
    username = (request.args.get("user") or "").strip()
    if not username:
        return {"ok": False, "error": "missing user"}, 400
    team_id = resolve_team_id(request.args.get("team_id"))

    format_filter = (request.args.get("format") or "").strip()
    normalized_user = normalize_name(username)
    normalized_format = normalize_name(format_filter) if format_filter else ""

    db = get_db()
    rows = db.execute(
        """
        SELECT rating_user, rating_after, format, created_at
        FROM matches
                WHERE rating_user IS NOT NULL AND rating_after IS NOT NULL
                    AND team_id = ?
        ORDER BY id ASC
        LIMIT 500
        """,
                (team_id,),
    ).fetchall()

    points = []
    for row in rows:
        if normalize_name(row["rating_user"]) != normalized_user:
            continue
        if normalized_format and normalize_name(row["format"] or "") != normalized_format:
            continue
        points.append(
            {
                "user": row["rating_user"],
                "elo": int(row["rating_after"]),
                "format": row["format"],
                "created_at": row["created_at"],
            }
        )

    if not points:
        raw_rows = db.execute(
            """
            SELECT log_lines.raw_line, log_lines.created_at, matches.format
            FROM log_lines
            LEFT JOIN matches ON matches.id = log_lines.match_id
            WHERE log_lines.raw_line LIKE '%rating:%'
              AND matches.team_id = ?
            ORDER BY log_lines.id ASC
            LIMIT 800
            """,
            (team_id,),
        ).fetchall()
        for row in raw_rows:
            rating_match = RAW_RATING_PATTERN.match(row["raw_line"])
            if not rating_match:
                continue
            if normalize_name(rating_match.group("user")) != normalized_user:
                continue
            if normalized_format and normalize_name(row["format"] or "") != normalized_format:
                continue
            points.append(
                {
                    "user": rating_match.group("user").strip(),
                    "elo": int(rating_match.group("after")),
                    "format": row["format"],
                    "created_at": row["created_at"],
                }
            )

    return {
        "ok": True,
        "user": username,
        "points": points,
    }


def _normalize_replay_url(value: str) -> str:
    url = value.strip()
    if not url:
        return ""
    if url.endswith(".json"):
        return url
    if url.endswith("/"):
        url = url[:-1]
    return f"{url}.json"


def _strip_replay_json(value: str) -> str:
    url = value.strip()
    if url.endswith(".json"):
        return url[:-5]
    return url


def _extract_replay_urls(text: str) -> list[str]:
    if not text:
        return []
    candidates: list[str] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        matches = REPLAY_URL_PATTERN.findall(line)
        if matches:
            candidates.extend(matches)
        else:
            candidates.append(line)
    seen = set()
    urls: list[str] = []
    for item in candidates:
        normalized = _normalize_replay_url(item)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        urls.append(normalized)
    return urls


def _ingest_replay_url(replay_url: str, team_id: int) -> dict:
    normalized = _normalize_replay_url(replay_url)
    if not normalized:
        return {"ok": False, "error": "missing url"}

    try:
        req = urllib.request.Request(
            normalized,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Accept": "application/json,text/plain;q=0.9,*/*;q=0.8",
            },
        )
        with urllib.request.urlopen(req, timeout=12) as response:
            payload = json.loads(response.read().decode("utf-8", errors="ignore"))
    except Exception:
        return {
            "ok": False,
            "error": "failed to fetch replay",
            "url": _strip_replay_json(normalized),
        }

    log_text = str(payload.get("log", ""))
    lines = log_text.splitlines()
    events, state, log_lines = parse_log_stream(lines, state={})

    meta = parse_match_meta(lines)
    result = compute_result(meta)

    db = get_db()
    cursor = db.execute(
        """
        INSERT INTO matches (name, created_at, format, player1, player2, winner, result, replay_url, team_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "Replay",
            datetime.utcnow().isoformat(timespec="seconds"),
            meta.get("format"),
            meta.get("player1"),
            meta.get("player2"),
            meta.get("winner"),
            result,
            _strip_replay_json(normalized),
            team_id,
        ),
    )
    match_id = cursor.lastrowid

    if events:
        db.executemany(
            """
            INSERT INTO events (match_id, event_type, actor, target, move, turn, value_low, value_high, raw_line)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    match_id,
                    event["event_type"],
                    event["actor"],
                    event["target"],
                    event.get("move"),
                    event.get("turn"),
                    event["value_low"],
                    event["value_high"],
                    event["raw_line"],
                )
                for event in events
            ],
        )

    if log_lines:
        db.executemany(
            """
            INSERT INTO log_lines (match_id, turn, raw_line, created_at)
            VALUES (?, ?, ?, ?)
            """,
            [
                (
                    match_id,
                    entry["turn"],
                    entry["raw_line"],
                    datetime.utcnow().isoformat(timespec="seconds"),
                )
                for entry in log_lines
            ],
        )

    update_match_state(match_id, state)
    db.commit()

    return {
        "ok": True,
        "match_id": match_id,
        "events": len(events),
        "url": _strip_replay_json(normalized),
    }


@app.route("/api/ingest_replay", methods=["POST"])
def api_ingest_replay():
    data = request.get_json(silent=True) or {}
    replay_url = str(data.get("url", "")).strip()
    team_id = resolve_team_id(data.get("team_id") or request.args.get("team_id"))
    if not replay_url:
        replay_url = (request.form.get("url") or "").strip()
    if not replay_url:
        raw_text = request.get_data(as_text=True) or ""
        urls = _extract_replay_urls(raw_text)
        if urls:
            replay_url = urls[0]
    if not replay_url:
        return {"status": "error", "message": "missing url"}, 400

    result = _ingest_replay_url(replay_url, team_id)
    if not result.get("ok"):
        return {"status": "error", "message": result.get("error", "failed to fetch replay")}, 400
    return {"status": "ok", "match_id": result["match_id"], "events": result["events"]}


@app.route("/api/ingest_replay_bulk", methods=["POST"])
def api_ingest_replay_bulk():
    data = request.get_json(silent=True) or {}
    urls = data.get("urls")
    text = str(data.get("text", ""))
    team_id = resolve_team_id(data.get("team_id") or request.args.get("team_id"))
    url_list: list[str] = []

    if isinstance(urls, list):
        url_list = [str(item) for item in urls]
    elif isinstance(urls, str) and urls.strip():
        url_list = [line.strip() for line in urls.splitlines() if line.strip()]
    elif text.strip():
        url_list = _extract_replay_urls(text)

    if not url_list:
        return {"status": "error", "message": "no urls found"}, 400

    results = []
    ok_count = 0
    for url in url_list:
        result = _ingest_replay_url(url, team_id)
        if result.get("ok"):
            ok_count += 1
        results.append(result | {"input": url})

    summary = {
        "total": len(results),
        "ok": ok_count,
        "failed": len(results) - ok_count,
    }
    return {"status": "ok", "summary": summary, "results": results}


@app.route("/api/ingest_replay_file", methods=["POST"])
def api_ingest_replay_file():
    data = request.get_json(silent=True) or {}
    urls = data.get("urls")
    text = str(data.get("text", ""))
    team_id = resolve_team_id(data.get("team_id") or request.args.get("team_id"))

    url_list: list[str] = []
    if isinstance(urls, list):
        url_list = [str(item) for item in urls]
    elif isinstance(urls, str) and urls.strip():
        url_list = [line.strip() for line in urls.splitlines() if line.strip()]
    elif text.strip():
        url_list = _extract_replay_urls(text)
    else:
        raw_text = request.get_data(as_text=True) or ""
        if raw_text.strip():
            url_list = _extract_replay_urls(raw_text)

    replay_file = DATA_DIR / "replays.txt"
    cleared = False
    if not url_list:
        if not replay_file.exists():
            return {"status": "error", "message": "replays.txt not found"}, 404
        file_text = replay_file.read_text(encoding="utf-8", errors="ignore")
        url_list = _extract_replay_urls(file_text)
        replay_file.write_text("", encoding="utf-8")
        cleared = True

    if not url_list:
        return {"status": "error", "message": "no urls found", "cleared": cleared}, 400

    results = []
    ok_count = 0
    for url in url_list:
        result = _ingest_replay_url(url, team_id)
        if result.get("ok"):
            ok_count += 1
        results.append(result | {"input": url})

    summary = {
        "total": len(results),
        "ok": ok_count,
        "failed": len(results) - ok_count,
    }
    return {"status": "ok", "summary": summary, "results": results, "cleared": cleared}


@app.route("/api/ingest_replay", methods=["OPTIONS"])
def api_ingest_replay_options():
    return {"status": "ok"}


@app.route("/live")
def live_match():
    team_id = resolve_team_id(request.args.get("team_id"))
    match_id = get_or_create_live_match(team_id)
    return redirect(url_for("match_detail", match_id=match_id))


if __name__ == "__main__":
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    host = os.environ.get("RECORDER_HOST", "127.0.0.1")
    port = int(os.environ.get("RECORDER_PORT", "5000"))
    debug = os.environ.get("RECORDER_DEBUG", "").strip().lower() in {"1", "true", "yes"}
    app.run(host=host, port=port, debug=debug)
