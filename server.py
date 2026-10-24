#!/usr/bin/env python3
"""
Spelling Bee Server
A local network spelling competition for classrooms
No authentication, no ads
Requires: websockets  (pip install websockets)
Uses: http.server + threading for static files

Avatars powered by DiceBear API (https://api.dicebear.com)

Features:
  - Teacher can PAUSE / RESUME at any time
  - After each round the game STOPS at round_result until teacher
    clicks "Next Round" / "Finish Match"
  - Teacher can REMOVE a student from the session at any time
"""

import asyncio
import websockets
import json
import random
import string
from http.server import HTTPServer, SimpleHTTPRequestHandler
import threading
import os
import time
from urllib.parse import quote

# ---------------------------------------------------------------------------
# Word list (word, definition)
# ---------------------------------------------------------------------------
WORD_LIST = [
    ("calendar", "a chart that shows the days, weeks, and months of a year"),
    ("necessary", "required or needed; essential"),
    ("knowledge", "information and understanding gained through learning"),
    ("character", "the qualities that define a person"),
    ("vegetable", "a plant used as food"),
    ("geography", "the study of Earth's land and features"),
    ("ancient", "very old; from long ago"),
    ("courage", "bravery in facing difficulty"),
    ("imagination", "the ability to form new ideas or images"),
    ("environment", "the natural world around us"),
    ("government", "the system that runs a country or state"),
    ("favorite", "liked the most"),
    ("exercise", "physical activity for fitness"),
    ("umbrella", "a tool used to stay dry in the rain"),
    ("treasure", "valuable items that are hidden or found"),
    ("alphabet", "the set of letters used in a language"),
    ("adventure", "an exciting or unusual experience"),
    ("furniture", "movable items like tables and chairs"),
    ("celebrate", "to honor an occasion with joy"),
    ("mountain", "a very high landform"),
    ("chocolate", "a sweet food made from cacao beans"),
    ("elephant", "a large gray animal with a trunk"),
    ("bicycle", "a two-wheeled vehicle you pedal"),
    ("hospital", "a place where sick people are treated"),
    ("library", "a place where books are kept and borrowed"),
    ("sandwich", "food made of two slices of bread with filling"),
    ("whisper", "to speak very softly"),
    ("journey", "a trip from one place to another"),
    ("triangle", "a shape with three sides"),
    ("architect", "a person who designs buildings"),
    ("brilliant", "very bright or very smart"),
    ("decision", "a choice that is made"),
    ("fossil", "the remains of an ancient plant or animal"),
    ("grammar", "the rules of a language"),
    ("horizon", "the line where the earth and sky appear to meet"),
    ("jealous", "feeling envy of someone else's success"),
    ("kingdom", "a country ruled by a king or queen"),
    ("lantern", "a portable light with a protective case"),
    ("migrate", "to move from one region to another"),
    ("nervous", "feeling worried or anxious"),
    ("observe", "to watch carefully"),
    ("parade", "a public procession of people and vehicles"),
    ("quarter", "one of four equal parts"),
    ("rescue", "to save someone from danger"),
    ("schedule", "a plan of times for events"),
    ("unusual", "not common or ordinary"),
    ("volcano", "a mountain that can erupt lava"),
    ("warning", "a statement that alerts about danger"),
    ("appreciate", "to be thankful for something"),
    ("camouflage", "coloring that helps something blend in"),
    ("delicious", "very pleasant to taste"),
    ("enthusiastic", "showing great excitement or interest"),
    ("genuine", "real and authentic"),
    ("hesitate", "to pause before doing something"),
    ("invisible", "unable to be seen"),
    ("literature", "written works such as novels and poems"),
    ("marvelous", "extremely good or wonderful"),
    ("negotiate", "to discuss in order to reach an agreement"),
    ("outrageous", "shockingly bad or excessive"),
    ("participate", "to take part in something"),
    ("quantity", "an amount of something"),
    ("recommend", "to suggest that something is good"),
    ("signature", "a person's name written in their own handwriting"),
    ("tournament", "a series of contests to determine a winner"),
    ("unanimous", "fully in agreement"),
    ("valuable", "worth a lot of money or important"),
    ("xylophone", "a musical instrument with wooden bars"),
    ("zoology", "the scientific study of animals"),
]

# Allowed DiceBear styles
DICEBEAR_STYLES = [
    "avataaars", "bottts", "lorelei", "notionists",
    "adventurer", "big-smile", "fun-emoji", "thumbs",
    "identicon", "shapes"
]

# ---------------------------------------------------------------------------
# Global state
# ---------------------------------------------------------------------------
games = {}          # game_code -> Game
connections = {}    # client_id -> websocket


# ---------------------------------------------------------------------------
# Helper: build DiceBear avatar URL
# ---------------------------------------------------------------------------
def make_avatar_url(style: str, seed: str) -> str:
    style = style if style in DICEBEAR_STYLES else "avataaars"
    seed = quote(seed or "player")
    return f"https://api.dicebear.com/9.x/{style}/svg?seed={seed}&size=128"


def normalize_code(raw) -> str:
    """Always return uppercase game code string (never crash on None)."""
    return str(raw or "").strip().upper()


def normalize_pid(raw) -> str:
    """Always return player_id as string."""
    return str(raw or "").strip()


# ---------------------------------------------------------------------------
# Game class
# ---------------------------------------------------------------------------
class Game:
    def __init__(self, game_id, teacher_websocket=None):
        self.game_id = game_id
        self.teacher_websocket = teacher_websocket
        self.players = {}           # player_id -> dict
        self.num_rounds = 5
        self.timer_seconds = 20
        self.words = []             # list of (word, definition)
        self.current_round = 0
        self.phase = "lobby"        # lobby | countdown | playing | round_result | game_over
        self.round_ends_at = 0
        self.last_results = {}
        self.ranking = []
        self._round_task = None

        # ---- Pause support ----
        self.paused = False
        self.paused_remaining = 0.0

        # ---- Manual next-round gate ----
        self.advance_event = asyncio.Event()

    # ------------------------------------------------------------------
    def add_player(self, player_name, websocket, avatar_style="avataaars", avatar_seed=None):
        player_id = "".join(random.choices(string.ascii_letters + string.digits, k=8))
        seed = avatar_seed or player_name or player_id
        avatar_url = make_avatar_url(avatar_style, seed)

        self.players[player_id] = {
            "name": player_name,
            "websocket": websocket,
            "score": 0,
            "answered": False,
            "last_answer": None,
            "live_input": "",
            "avatar": avatar_url,
            "avatar_style": avatar_style,
            "avatar_seed": seed,
        }
        return player_id

    def remove_player(self, player_id):
        """
        Remove a player from the session.
        Returns (True, name, websocket) on success, or (False, None, None).
        """
        player_id = normalize_pid(player_id)
        if not player_id or player_id not in self.players:
            return False, None, None

        p = self.players.pop(player_id)
        name = p.get("name", "???")
        ws = p.get("websocket")

        # Drop from current round results if present
        if player_id in self.last_results:
            del self.last_results[player_id]

        return True, name, ws

    def set_config(self, rounds, timer):
        if isinstance(rounds, int) and 1 <= rounds <= 20:
            self.num_rounds = rounds
        if isinstance(timer, int) and 5 <= timer <= 120:
            self.timer_seconds = timer

    def prepare_words(self):
        self.words = random.sample(WORD_LIST, min(self.num_rounds, len(WORD_LIST)))
        self.num_rounds = len(self.words)

    # ------------------------------------------------------------------
    # Pause / resume
    # ------------------------------------------------------------------
    def can_pause(self):
        return self.phase in ("countdown", "playing", "round_result") and not self.paused

    def pause(self):
        if not self.can_pause():
            return False
        if self.phase == "playing":
            self.paused_remaining = max(0.0, self.round_ends_at - time.time())
        self.paused = True
        return True

    def resume(self):
        if not self.paused:
            return False
        if self.phase == "playing":
            self.round_ends_at = time.time() + self.paused_remaining
        self.paused = False
        return True

    def time_left(self):
        if self.phase != "playing":
            return self.timer_seconds
        if self.paused:
            return max(0, int(round(self.paused_remaining)))
        return max(0, int(self.round_ends_at - time.time()))

    # ------------------------------------------------------------------
    def get_state(self, for_teacher=False):
        players_public = {}
        for pid, p in self.players.items():
            player_info = {
                "name": p["name"],
                "score": p["score"],
                "answered": p.get("answered", False),
                "avatar": p.get("avatar"),
            }
            if for_teacher:
                player_info["live_input"] = p.get("live_input", "")
            players_public[pid] = player_info

        state = {
            "game_id": self.game_id,
            "phase": self.phase,
            "paused": self.paused,
            "num_rounds": self.num_rounds,
            "timer_seconds": self.timer_seconds,
            "current_round": self.current_round,
            "total_rounds": self.num_rounds,
            "players": players_public,
            "last_results": self.last_results,
            "ranking": self.ranking,
        }

        if self.phase in ("playing", "countdown", "round_result") and self.current_round > 0:
            word, definition = self.words[self.current_round - 1]
            state["time_left"] = self.time_left()
            state["definition"] = definition
            state["letter_count"] = len(word)
            state["first_letter"] = word[0].upper()
            if for_teacher:
                state["word"] = word
            if self.phase == "round_result":
                state["correct_word"] = word

        if self.phase == "game_over":
            state["ranking"] = self.ranking

        if for_teacher and self.words:
            state["word_list"] = [
                {"word": w, "definition": d} for w, d in self.words
            ]

        return state


def generate_game_code():
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=6))


# ---------------------------------------------------------------------------
# WebSocket helpers
# ---------------------------------------------------------------------------
async def send_json(ws, obj):
    try:
        await ws.send(json.dumps(obj))
    except Exception:
        pass


async def broadcast_game_state(game_code):
    if game_code not in games:
        return
    game = games[game_code]

    if game.teacher_websocket:
        await send_json(game.teacher_websocket, {
            "type": "game_state",
            "game_state": game.get_state(for_teacher=True),
        })

    for pid, p in list(game.players.items()):
        await send_json(p["websocket"], {
            "type": "game_state",
            "game_state": game.get_state(for_teacher=False),
            "player_id": pid,
        })


async def pausable_sleep(game, seconds):
    remaining = float(seconds)
    last = time.time()
    while remaining > 0:
        await asyncio.sleep(0.1)
        now = time.time()
        if not game.paused:
            remaining -= (now - last)
        last = now


async def run_rounds(game_code):
    game = games.get(game_code)
    if not game:
        return

    game.prepare_words()
    game.phase = "countdown"
    await broadcast_game_state(game_code)
    await pausable_sleep(game, 3)

    for r in range(1, game.num_rounds + 1):
        game = games.get(game_code)
        if not game:
            return

        word, definition = game.words[r - 1]
        game.current_round = r
        game.phase = "playing"
        game.round_ends_at = time.time() + game.timer_seconds
        game.paused_remaining = float(game.timer_seconds)

        if game.paused:
            game.paused_remaining = float(game.timer_seconds)

        for p in game.players.values():
            p["answered"] = False
            p["last_answer"] = None
            p["live_input"] = ""

        print(f"[{game_code}] Round {r}/{game.num_rounds}: {word}")
        await broadcast_game_state(game_code)

        last_broadcast = 0
        while True:
            await asyncio.sleep(0.25)

            if not game.paused:
                if time.time() >= game.round_ends_at:
                    break
                # Only remaining connected players count
                if game.players and all(p.get("answered") for p in game.players.values()):
                    break

            now = time.time()
            if now - last_broadcast >= 1.0:
                await broadcast_game_state(game_code)
                last_broadcast = now

        # Score this round
        results = {}
        for pid, p in game.players.items():
            ans = (p.get("last_answer") or "").strip().lower()
            correct = ans == word.lower()
            if correct:
                p["score"] += 1
            results[pid] = {
                "name": p["name"],
                "answer": p.get("last_answer") or "(no answer)",
                "correct": correct,
                "score": p["score"],
                "avatar": p.get("avatar"),
            }
        game.last_results = results
        game.phase = "round_result"
        print(f"[{game_code}] Round {r} results: {results}")
        await broadcast_game_state(game_code)

        # Wait for teacher "Next Round"
        game.advance_event.clear()
        print(f"[{game_code}] Waiting for teacher to click Next Round…")
        await game.advance_event.wait()
        print(f"[{game_code}] Teacher advanced — continuing")

    # Game over
    game = games.get(game_code)
    if not game:
        return

    ranking = sorted(
        [
            {
                "name": p["name"],
                "score": p["score"],
                "avatar": p.get("avatar"),
            }
            for p in game.players.values()
        ],
        key=lambda x: -x["score"],
    )
    game.ranking = ranking
    game.paused = False
    game.phase = "game_over"
    print(f"[{game_code}] GAME OVER: {ranking}")
    await broadcast_game_state(game_code)


# ---------------------------------------------------------------------------
# WebSocket handler
# ---------------------------------------------------------------------------
async def handle_websocket(websocket):
    client_id = id(websocket)
    connections[client_id] = websocket

    try:
        remote = websocket.remote_address
        print(f"New connection from {remote}")
    except Exception:
        print("New connection")

    try:
        async for message in websocket:
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                continue

            action = data.get("action")

            # ---- Teacher creates game ----
            if action == "create_game":
                code = generate_game_code()
                while code in games:
                    code = generate_game_code()
                games[code] = Game(code, teacher_websocket=websocket)
                await send_json(websocket, {
                    "type": "game_created",
                    "game_code": code,
                    "game_state": games[code].get_state(for_teacher=True),
                })
                print(f"Game created: {code}")

            # ---- Teacher sets config ----
            elif action == "set_config":
                code = normalize_code(data.get("game_code"))
                if code in games:
                    game = games[code]
                    if game.teacher_websocket is websocket and game.phase == "lobby":
                        game.set_config(data.get("rounds"), data.get("timer"))
                        await broadcast_game_state(code)
                        print(f"[{code}] Config -> rounds={game.num_rounds} timer={game.timer_seconds}")

            # ---- Student joins (with avatar) ----
            elif action == "join_game":
                code = normalize_code(data.get("game_code"))
                name = (data.get("player_name") or "Anonymous").strip()[:20]
                avatar_style = data.get("avatar_style", "avataaars")
                avatar_seed = data.get("avatar_seed")

                print(f"Join: {name} -> {code} (style={avatar_style})")

                if code not in games:
                    await send_json(websocket, {"type": "error", "message": "Game not found"})
                    continue
                game = games[code]
                if game.phase != "lobby":
                    await send_json(websocket, {"type": "error", "message": "Game already started"})
                    continue
                if any(p["name"].lower() == name.lower() for p in game.players.values()):
                    await send_json(websocket, {"type": "error", "message": "Name already taken"})
                    continue

                pid = game.add_player(name, websocket, avatar_style, avatar_seed)
                await send_json(websocket, {
                    "type": "joined",
                    "player_id": pid,
                    "game_state": game.get_state(for_teacher=False),
                })
                await broadcast_game_state(code)
                print(f"  {name} joined ({pid}) avatar={game.players[pid]['avatar']}")

            # ---- Teacher starts ----
            elif action == "start_game":
                code = normalize_code(data.get("game_code"))
                if code in games:
                    game = games[code]
                    if game.teacher_websocket is websocket and game.phase == "lobby":
                        if not game.players:
                            await send_json(websocket, {"type": "error", "message": "No players yet"})
                            continue
                        game.paused = False
                        game.phase = "countdown"
                        game._round_task = asyncio.create_task(run_rounds(code))
                        print(f"[{code}] Game started")

            # ---- Teacher PAUSES ----
            elif action == "pause_game":
                code = normalize_code(data.get("game_code"))
                if code in games:
                    game = games[code]
                    if game.teacher_websocket is websocket and game.pause():
                        print(f"[{code}] PAUSED")
                        await broadcast_game_state(code)

            # ---- Teacher RESUMES ----
            elif action == "resume_game":
                code = normalize_code(data.get("game_code"))
                if code in games:
                    game = games[code]
                    if game.teacher_websocket is websocket and game.resume():
                        print(f"[{code}] RESUMED")
                        await broadcast_game_state(code)

            # ---- Teacher toggles pause/play ----
            elif action == "toggle_pause":
                code = normalize_code(data.get("game_code"))
                if code in games:
                    game = games[code]
                    if game.teacher_websocket is websocket:
                        changed = game.resume() if game.paused else game.pause()
                        if changed:
                            print(f"[{code}] {'PAUSED' if game.paused else 'RESUMED'}")
                            await broadcast_game_state(code)

            # ---- Teacher advances to next round ----
            elif action == "next_round":
                code = normalize_code(data.get("game_code"))
                if code in games:
                    game = games[code]
                    if (game.teacher_websocket is websocket
                            and game.phase == "round_result"):
                        game.advance_event.set()
                        print(f"[{code}] Teacher clicked Next Round / Finish Match")

            # ---- Teacher REMOVES a student from the session ----
            elif action == "remove_player":
                code = normalize_code(data.get("game_code"))
                pid = normalize_pid(data.get("player_id"))

                print(f"[remove_player] code={code!r} pid={pid!r}")

                if not code or code not in games:
                    await send_json(websocket, {
                        "type": "error",
                        "message": "Game not found",
                    })
                    continue

                game = games[code]

                # Only the teacher of this room can remove
                if game.teacher_websocket is not websocket:
                    await send_json(websocket, {
                        "type": "error",
                        "message": "Only the teacher can remove players",
                    })
                    continue

                if not pid:
                    await send_json(websocket, {
                        "type": "error",
                        "message": "Missing player_id",
                    })
                    continue

                ok, name, target_ws = game.remove_player(pid)
                if ok:
                    print(f"[{code}] Teacher removed player: {name} ({pid})")
                    # Notify kicked student
                    if target_ws:
                        await send_json(target_ws, {
                            "type": "kicked",
                            "message": "You were removed from the game by the teacher.",
                        })
                        # Optionally close their socket so they can't keep sending
                        try:
                            await target_ws.close()
                        except Exception:
                            pass
                    # Update everyone else (teacher + remaining students)
                    await broadcast_game_state(code)
                else:
                    print(f"[{code}] remove_player failed — player_id not found: {pid!r}")
                    print(f"  Current players: {list(game.players.keys())}")
                    await send_json(websocket, {
                        "type": "error",
                        "message": "Player not found",
                    })

            # ---- Student live typing streaming ----
            elif action == "live_typing":
                code = normalize_code(data.get("game_code"))
                pid = normalize_pid(data.get("player_id"))
                current_input = data.get("current_input", "")
                if code in games:
                    game = games[code]
                    if game.phase == "playing" and not game.paused and pid in game.players:
                        game.players[pid]["live_input"] = current_input
                        if game.teacher_websocket:
                            await send_json(game.teacher_websocket, {
                                "type": "student_typing_update",
                                "player_id": pid,
                                "player_name": game.players[pid]["name"],
                                "current_input": current_input,
                            })

            # ---- Student submits answer ----
            elif action == "submit_answer":
                code = normalize_code(data.get("game_code"))
                pid = normalize_pid(data.get("player_id"))
                answer = (data.get("answer") or "").strip()
                if code in games:
                    game = games[code]
                    if game.paused:
                        await send_json(websocket, {"type": "error", "message": "Game is paused"})
                        continue
                    if game.phase == "playing" and pid in game.players:
                        if not game.players[pid].get("answered"):
                            game.players[pid]["last_answer"] = answer
                            game.players[pid]["live_input"] = answer
                            game.players[pid]["answered"] = True
                            print(f"  {game.players[pid]['name']} -> {answer}")
                            await broadcast_game_state(code)

            # ---- Request current state ----
            elif action == "get_game_state":
                code = normalize_code(data.get("game_code"))
                if code in games:
                    game = games[code]
                    is_teacher = game.teacher_websocket is websocket
                    await send_json(websocket, {
                        "type": "game_state",
                        "game_state": game.get_state(for_teacher=is_teacher),
                    })

            # ---- Teacher resets for new game ----
            elif action == "reset_game":
                code = normalize_code(data.get("game_code"))
                if code in games:
                    game = games[code]
                    if game.teacher_websocket is websocket:
                        if game._round_task and not game._round_task.done():
                            game._round_task.cancel()
                        game._round_task = None
                        game.advance_event.set()
                        game.phase = "lobby"
                        game.paused = False
                        game.paused_remaining = 0.0
                        game.current_round = 0
                        game.words = []
                        game.last_results = {}
                        game.ranking = []
                        for p in game.players.values():
                            p["score"] = 0
                            p["answered"] = False
                            p["last_answer"] = None
                            p["live_input"] = ""
                        await broadcast_game_state(code)
                        print(f"[{code}] Reset to lobby")

    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception as e:
        print(f"Handler error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if client_id in connections:
            del connections[client_id]

        for code, game in list(games.items()):
            if game.teacher_websocket is websocket:
                game.teacher_websocket = None
                print(f"[{code}] Teacher disconnected")
            for pid, p in list(game.players.items()):
                if p["websocket"] is websocket:
                    name = p["name"]
                    del game.players[pid]
                    if pid in game.last_results:
                        del game.last_results[pid]
                    print(f"[{code}] {name} left")
                    asyncio.create_task(broadcast_game_state(code))


# ---------------------------------------------------------------------------
# HTTP static server
# ---------------------------------------------------------------------------
def run_http_server():
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    server = HTTPServer(("0.0.0.0", 8000), SimpleHTTPRequestHandler)
    print("HTTP Server  -> http://0.0.0.0:8000")
    server.serve_forever()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def main():
    print("=" * 60)
    print("  SPELLING BEE SERVER  (Live Terminals, Avatars, Pause/Play)")
    print("  + Manual Next Round control")
    print("  + Teacher can remove students")
    print("=" * 60)

    http_thread = threading.Thread(target=run_http_server, daemon=True)
    http_thread.start()

    async with websockets.serve(handle_websocket, "0.0.0.0", 8001):
        print("WebSocket    -> ws://0.0.0.0:8001")
        print("=" * 60)
        print("  Teacher  : http://localhost:8000/teacher.html")
        print("  Students : http://localhost:8000/student.html")
        print("  (use your LAN IP for other devices)")
        print("=" * 60)
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutting down...")