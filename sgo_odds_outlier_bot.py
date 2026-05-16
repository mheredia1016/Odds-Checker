
import asyncio
import aiohttp
import os
import sqlite3
import statistics
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("SPORTSGAMEODDS_API_KEY", "") or os.getenv("SPORTS_ODDS_API_KEY_HEADER", "")
API_BASE = "https://api.sportsgameodds.com/v2/events"

GAME_WEBHOOK = os.getenv("GAME_DISCORD_WEBHOOK_URL", "") or os.getenv("DISCORD_WEBHOOK_URL", "")
PROP_WEBHOOK = os.getenv("PROP_DISCORD_WEBHOOK_URL", "") or GAME_WEBHOOK

LEAGUE_IDS = [x.strip() for x in os.getenv("LEAGUE_IDS", "NBA,MLB,NHL,EPL,MLS,WNBA").split(",") if x.strip()]
ALLOWED_BOOKS = set(x.strip().lower() for x in os.getenv("ALLOWED_BOOKS", "fanduel,draftkings,betmgm,caesars,bet365,espnbet,hardrockbet").split(",") if x.strip())

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "300"))
LIMIT_PER_LEAGUE = int(os.getenv("LIMIT_PER_LEAGUE", "50"))
INCLUDE_ALT_LINES = os.getenv("INCLUDE_ALT_LINES", "false").lower() in ("1", "true", "yes", "y")

MIN_BOOKS = int(os.getenv("MIN_BOOKS", "4"))
MONEYLINE_RATIO_THRESHOLD = float(os.getenv("MONEYLINE_RATIO_THRESHOLD", "1.50"))
SAME_LINE_RATIO_THRESHOLD = float(os.getenv("SAME_LINE_RATIO_THRESHOLD", "1.50"))
PROP_RATIO_THRESHOLD = float(os.getenv("PROP_RATIO_THRESHOLD", "1.60"))
COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "45"))

ENABLE_PROPS = os.getenv("ENABLE_PROPS", "true").lower() in ("1", "true", "yes", "y")
PROP_STAT_IDS = set(x.strip().lower() for x in os.getenv("PROP_STAT_IDS", "points,rebounds,assists,three_pointers,shots_on_goal,home_runs,hits,total_bases,strikeouts").split(",") if x.strip())

DB_PATH = os.getenv("DB_PATH", "sgo_odds_outliers.db")

BOOK_NAMES = {
    "fanduel": "FanDuel",
    "draftkings": "DraftKings",
    "betmgm": "BetMGM",
    "caesars": "Caesars",
    "bet365": "bet365",
    "espnbet": "ESPN BET",
    "hardrockbet": "Hard Rock Bet",
}

SPORTSBOOK_HOME_URLS = {
    "fanduel": "https://sportsbook.fanduel.com/",
    "draftkings": "https://sportsbook.draftkings.com/",
    "betmgm": "https://sports.betmgm.com/",
    "caesars": "https://www.caesars.com/sportsbook-and-casino",
    "bet365": "https://www.bet365.com/",
    "espnbet": "https://espnbet.com/",
    "hardrockbet": "https://app.hardrock.bet/",
}


def now_utc():
    return datetime.now(timezone.utc)


def american_to_decimal(odds):
    try:
        odds = float(odds)
    except Exception:
        return None
    if odds > 0:
        return odds / 100.0 + 1.0
    return 100.0 / abs(odds) + 1.0


def decimal_to_american(decimal_odds):
    if decimal_odds is None:
        return None
    if decimal_odds >= 2:
        return round((decimal_odds - 1) * 100)
    return round(-100 / (decimal_odds - 1))


def fmt_american(odds):
    if odds is None:
        return "N/A"
    try:
        odds = int(round(float(odds)))
    except Exception:
        return str(odds)
    return f"+{odds}" if odds > 0 else str(odds)


def parse_float(value):
    try:
        return float(value)
    except Exception:
        return None


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS alerts (alert_key TEXT PRIMARY KEY, created_at TEXT NOT NULL)")
    conn.commit()
    conn.close()


def recently_alerted(alert_key):
    cutoff = now_utc() - timedelta(minutes=COOLDOWN_MINUTES)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT created_at FROM alerts WHERE alert_key = ?", (alert_key,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return False
    try:
        return datetime.fromisoformat(row[0]) > cutoff
    except Exception:
        return False


def mark_alerted(alert_key):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO alerts (alert_key, created_at) VALUES (?, ?)", (alert_key, now_utc().isoformat()))
    conn.commit()
    conn.close()


def data_items(payload):
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    data = payload.get("data", payload)
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        if isinstance(data.get("items"), list):
            return data["items"]
        if isinstance(data.get("events"), list):
            return data["events"]
    if isinstance(payload.get("items"), list):
        return payload["items"]
    return []


async def fetch_events_for_league(session, league_id):
    params = {
        "leagueID": league_id,
        "oddsAvailable": "true",
        "includeAltLines": "true" if INCLUDE_ALT_LINES else "false",
        "limit": str(LIMIT_PER_LEAGUE),
    }
    headers = {"x-api-key": API_KEY}

    async with session.get(API_BASE, params=params, headers=headers, timeout=30) as resp:
        text = await resp.text()
        if resp.status != 200:
            print(f"[SGO ERROR] league={league_id} HTTP {resp.status}: {text[:500]}")
            return []
        try:
            payload = await resp.json()
        except Exception:
            print(f"[SGO JSON ERROR] league={league_id}: {text[:500]}")
            return []
        items = data_items(payload)
        print(f"[SGO OK] league={league_id} events={len(items)}")
        return items


def get_event_id(event):
    return event.get("eventID") or event.get("id") or event.get("event_id") or "unknown_event"


def get_team_name(event, side):
    teams = event.get("teams") or {}
    if isinstance(teams, dict):
        obj = teams.get(side) or {}
        return obj.get("name") or obj.get("teamName") or obj.get("teamID") or side.title()
    return side.title()


def get_event_name(event):
    away = get_team_name(event, "away")
    home = get_team_name(event, "home")
    if away != "Away" and home != "Home":
        return f"{away} @ {home}"
    return event.get("name") or event.get("activity") or get_event_id(event)


def get_start_time(event):
    status = event.get("status") or {}
    raw = status.get("startsAt") or status.get("startTime") or event.get("startsAt")
    if not raw:
        return "N/A"
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00")).strftime("%b %d, %I:%M %p UTC")
    except Exception:
        return str(raw)


def split_odd_id(odd_id):
    parts = str(odd_id).split("-")
    if len(parts) < 5:
        return None
    return {
        "stat_id": parts[0],
        "entity_id": parts[1],
        "period_id": parts[2],
        "bet_type_id": parts[3],
        "side_id": "-".join(parts[4:]),
    }


def entity_label(event, parsed):
    entity = parsed["entity_id"]
    side = parsed["side_id"]
    if entity == "home" or side == "home":
        return get_team_name(event, "home")
    if entity == "away" or side == "away":
        return get_team_name(event, "away")
    if entity == "all":
        return side.title() if side in ("over", "under") else "All"
    players = event.get("players") or {}
    player_obj = players.get(entity) if isinstance(players, dict) else None
    if isinstance(player_obj, dict):
        return player_obj.get("name") or player_obj.get("fullName") or entity
    return entity.replace("_", " ").title()


def is_game_market(parsed):
    return (
        parsed["stat_id"] == "points"
        and parsed["period_id"] == "game"
        and parsed["bet_type_id"] in ("ml", "ml3way", "sp", "ou")
        and parsed["entity_id"] in ("home", "away", "all")
    )


def is_prop_market(parsed):
    return (
        ENABLE_PROPS
        and parsed["period_id"] == "game"
        and parsed["entity_id"] not in ("home", "away", "all")
        and parsed["bet_type_id"] in ("ou", "yn")
        and parsed["stat_id"].lower() in PROP_STAT_IDS
    )


def extract_offers(event):
    rows = []
    odds_obj = event.get("odds") or {}
    if not isinstance(odds_obj, dict):
        return rows

    for odd_id, odd_info in odds_obj.items():
        if not isinstance(odd_info, dict):
            continue
        parsed = split_odd_id(odd_id)
        if not parsed:
            continue

        market_kind = "game" if is_game_market(parsed) else "prop" if is_prop_market(parsed) else None
        if not market_kind:
            continue

        by_book = odd_info.get("byBookmaker") or odd_info.get("by_bookmaker") or {}
        if not isinstance(by_book, dict):
            continue

        for book_key, offer in by_book.items():
            book_key = str(book_key).lower()
            if ALLOWED_BOOKS and book_key not in ALLOWED_BOOKS:
                continue
            if not isinstance(offer, dict) or offer.get("available", True) is False:
                continue

            price = offer.get("odds") or offer.get("price")
            if price is None:
                continue

            if parsed["bet_type_id"] == "sp":
                line = parse_float(offer.get("spread"))
            elif parsed["bet_type_id"] == "ou":
                line = parse_float(offer.get("overUnder") or offer.get("over_under"))
            else:
                line = None

            rows.append({
                "event_id": get_event_id(event),
                "odd_id": odd_id,
                "market_kind": market_kind,
                "stat_id": parsed["stat_id"],
                "entity_id": parsed["entity_id"],
                "period_id": parsed["period_id"],
                "bet_type_id": parsed["bet_type_id"],
                "side_id": parsed["side_id"],
                "selection": entity_label(event, parsed),
                "book_key": book_key,
                "book": BOOK_NAMES.get(book_key, book_key),
                "price": price,
                "line": line,
                "deeplink": offer.get("deeplink") or offer.get("deepLink") or offer.get("link"),
            })

    return rows


def group_rows(rows, fields):
    grouped = {}
    for row in rows:
        key = tuple(row.get(f) for f in fields)
        grouped.setdefault(key, []).append(row)
    return grouped


def median_decimal(offers):
    decs = [american_to_decimal(o["price"]) for o in offers]
    decs = [d for d in decs if d is not None]
    return statistics.median(decs) if decs else None


def find_moneyline_outliers(rows):
    alerts = []
    groups = group_rows([r for r in rows if r["market_kind"] == "game" and r["bet_type_id"] in ("ml", "ml3way")], ["odd_id"])
    for _, offers in groups.items():
        if len(offers) < MIN_BOOKS:
            continue
        med = median_decimal(offers)
        if not med:
            continue
        for offer in offers:
            od = american_to_decimal(offer["price"])
            if od and od / med >= MONEYLINE_RATIO_THRESHOLD:
                alerts.append(make_alert("game", "Moneyline Outlier", f'{offer["selection"]} ML', offer, offers, med, f"{od/med:.2f}x payout"))
    return alerts


def find_same_line_outliers(rows):
    alerts = []
    game_lines = [r for r in rows if r["market_kind"] == "game" and r["bet_type_id"] in ("sp", "ou") and r["line"] is not None]
    groups = group_rows(game_lines, ["odd_id", "line"])
    for _, offers in groups.items():
        if len(offers) < MIN_BOOKS:
            continue
        med = median_decimal(offers)
        if not med:
            continue
        for offer in offers:
            od = american_to_decimal(offer["price"])
            if od and od / med >= SAME_LINE_RATIO_THRESHOLD:
                label = "Spread Price Outlier" if offer["bet_type_id"] == "sp" else "Total Price Outlier"
                selection = f'{offer["selection"]} {offer["line"]}' if offer["bet_type_id"] == "sp" else f'{offer["side_id"].title()} {offer["line"]}'
                alerts.append(make_alert("game", label, selection, offer, offers, med, f"{od/med:.2f}x payout on same exact line"))
    return alerts


def find_prop_outliers(rows):
    alerts = []
    prop_rows = [r for r in rows if r["market_kind"] == "prop" and (r["bet_type_id"] != "ou" or r["line"] is not None)]
    groups = group_rows(prop_rows, ["stat_id", "entity_id", "period_id", "bet_type_id", "side_id", "line"])
    for _, offers in groups.items():
        if len(offers) < MIN_BOOKS:
            continue
        med = median_decimal(offers)
        if not med:
            continue
        for offer in offers:
            od = american_to_decimal(offer["price"])
            if od and od / med >= PROP_RATIO_THRESHOLD:
                line_text = "" if offer["line"] is None else f' {offer["line"]}'
                selection = f'{offer["selection"]} {offer["stat_id"].replace("_", " ").title()} {offer["side_id"].title()}{line_text}'
                alerts.append(make_alert("prop", "Player Prop Outlier", selection, offer, offers, med, f"{od/med:.2f}x payout"))
    return alerts


def make_alert(category, alert_type, selection, offer, offers, med, edge):
    return {
        "category": category,
        "type": alert_type,
        "selection": selection,
        "book": offer["book"],
        "book_key": offer["book_key"],
        "price": offer["price"],
        "line": offer.get("line"),
        "market_price": decimal_to_american(med),
        "edge": edge,
        "offers": offers,
        "deeplink": offer.get("deeplink"),
    }


def format_offers(alert):
    lines = []
    for o in sorted(alert["offers"], key=lambda x: x["book"]):
        if o["bet_type_id"] in ("sp", "ou") or o["market_kind"] == "prop":
            line_text = "" if o["line"] is None else f' {o["line"]}'
            lines.append(f'{o["book"]}: {o["selection"]} {o["side_id"].title()}{line_text} {fmt_american(o["price"])}')
        else:
            lines.append(f'{o["book"]}: {fmt_american(o["price"])}')
    return "\n".join(lines[:12])


def get_alert_link(alert):
    return alert.get("deeplink") or SPORTSBOOK_HOME_URLS.get(alert.get("book_key", ""))


def alert_key(event, alert):
    return "|".join([get_event_id(event), alert["type"], alert["selection"], alert["book_key"], str(alert["price"]), str(alert.get("line")), str(alert.get("market_price"))])


async def send_alert(session, webhook, event, alert):
    if not webhook:
        return
    link = get_alert_link(alert)
    fields = [{"name": "Books Checked", "value": format_offers(alert)[:1000] or "N/A", "inline": False}]
    if link:
        fields.append({"name": "Open Bet", "value": f'[Open {alert["book"]}]({link})', "inline": False})

    embed = {
        "title": f"🚨 {alert['type']}",
        "description": (
            f"**{get_event_name(event)}**\n{get_start_time(event)}\n\n"
            f"**{alert['selection']}**\n"
            f"Outlier: **{alert['book']} {fmt_american(alert['price'])}**\n"
            f"Market median: **{fmt_american(alert['market_price'])}**\n"
            f"Edge: **{alert['edge']}**"
        ),
        "color": 15844367 if alert["category"] == "prop" else 15158332,
        "fields": fields,
        "footer": {"text": "SportsGameOdds • Odds Outlier Bot"},
        "timestamp": now_utc().isoformat(),
    }
    if link:
        embed["url"] = link

    async with session.post(webhook, json={"embeds": [embed]}, timeout=20) as resp:
        if resp.status not in (200, 204):
            text = await resp.text()
            print(f"[DISCORD ERROR] {resp.status}: {text[:300]}")
        else:
            print(f"[ALERT SENT] {alert['type']} | {get_event_name(event)} | {alert['book']}")


async def scan_once(session):
    for league in LEAGUE_IDS:
        events = await fetch_events_for_league(session, league)
        for event in events:
            rows = extract_offers(event)
            if not rows:
                continue
            alerts = find_moneyline_outliers(rows) + find_same_line_outliers(rows) + find_prop_outliers(rows)
            for alert in alerts:
                key = alert_key(event, alert)
                if recently_alerted(key):
                    continue
                webhook = PROP_WEBHOOK if alert["category"] == "prop" else GAME_WEBHOOK
                await send_alert(session, webhook, event, alert)
                mark_alerted(key)


async def main():
    if not API_KEY:
        raise RuntimeError("Missing SPORTSGAMEODDS_API_KEY")
    if not GAME_WEBHOOK:
        raise RuntimeError("Missing GAME_DISCORD_WEBHOOK_URL")

    init_db()
    print("SportsGameOdds Outlier Bot started")
    print(f"Leagues: {LEAGUE_IDS}")
    print(f"Allowed books: {sorted(ALLOWED_BOOKS)}")
    print(f"Poll seconds: {POLL_SECONDS}")
    print(f"Include alt lines: {INCLUDE_ALT_LINES}")
    print(f"Enable props: {ENABLE_PROPS}")

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                await scan_once(session)
            except Exception as e:
                print(f"[SCAN ERROR] {type(e).__name__}: {e}")
            await asyncio.sleep(POLL_SECONDS)


if __name__ == "__main__":
    asyncio.run(main())
