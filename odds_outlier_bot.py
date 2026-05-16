import asyncio
import aiohttp
import os
import sqlite3
import statistics
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()

ODDS_API_KEY = os.getenv("ODDS_API_KEY", "")

GAME_WEBHOOK = os.getenv("GAME_DISCORD_WEBHOOK_URL", "") or os.getenv("DISCORD_WEBHOOK_URL", "")
PROP_WEBHOOK = os.getenv("PROP_DISCORD_WEBHOOK_URL", "") or GAME_WEBHOOK

GAME_SPORT_KEYS = [x.strip() for x in os.getenv(
    "GAME_SPORT_KEYS",
    "basketball_nba,basketball_wnba,baseball_mlb,icehockey_nhl,soccer_epl,soccer_usa_mls"
).split(",") if x.strip()]

GAME_MARKETS = [x.strip() for x in os.getenv("GAME_MARKETS", "h2h,spreads,totals").split(",") if x.strip()]
REGIONS = os.getenv("REGIONS", "us")
ALLOWED_BOOKS = set([x.strip().lower() for x in os.getenv("ALLOWED_BOOKS", "").split(",") if x.strip()])

GAME_POLL_SECONDS = int(os.getenv("GAME_POLL_SECONDS", "60"))
PROP_POLL_SECONDS = int(os.getenv("PROP_POLL_SECONDS", "300"))
PROP_HOURS_BEFORE_GAME = float(os.getenv("PROP_HOURS_BEFORE_GAME", "4"))

MIN_BOOKS = int(os.getenv("MIN_BOOKS", "4"))
MONEYLINE_RATIO_THRESHOLD = float(os.getenv("MONEYLINE_RATIO_THRESHOLD", "1.50"))
SPREAD_POINT_DIFF_THRESHOLD = float(os.getenv("SPREAD_POINT_DIFF_THRESHOLD", "2.0"))
TOTAL_POINT_DIFF_THRESHOLD = float(os.getenv("TOTAL_POINT_DIFF_THRESHOLD", "2.0"))

PROP_MIN_BOOKS = int(os.getenv("PROP_MIN_BOOKS", "4"))
PROP_RATIO_THRESHOLD = float(os.getenv("PROP_RATIO_THRESHOLD", "1.60"))

COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "45"))
DB_PATH = os.getenv("DB_PATH", "odds_outliers.db")

API_BASE = "https://api.the-odds-api.com/v4/sports/{sport}/odds"


def now_utc():
    return datetime.now(timezone.utc)


def american_to_decimal(odds):
    if odds is None:
        return None
    if odds > 0:
        return (odds / 100.0) + 1.0
    return (100.0 / abs(odds)) + 1.0


def decimal_to_american(decimal_odds):
    if decimal_odds is None:
        return None
    if decimal_odds >= 2:
        return round((decimal_odds - 1) * 100)
    return round(-100 / (decimal_odds - 1))


def fmt_american(odds):
    if odds is None:
        return "N/A"
    odds = int(round(odds))
    return f"+{odds}" if odds > 0 else str(odds)


def parse_prop_scan_config():
    """
    Format:
    basketball_nba:player_points|player_threes;baseball_mlb:batter_home_runs
    """
    raw = os.getenv(
        "PROP_SCAN_CONFIG",
        "basketball_nba:player_points|player_threes;"
        "basketball_wnba:player_points|player_threes;"
        "baseball_mlb:batter_home_runs;"
        "icehockey_nhl:player_shots_on_goal"
    )

    config = {}
    for chunk in raw.split(";"):
        chunk = chunk.strip()
        if not chunk or ":" not in chunk:
            continue
        sport, markets = chunk.split(":", 1)
        market_list = [m.strip() for m in markets.split("|") if m.strip()]
        if sport.strip() and market_list:
            config[sport.strip()] = market_list

    return config


PROP_SCAN_CONFIG = parse_prop_scan_config()


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS alerts (
            alert_key TEXT PRIMARY KEY,
            created_at TEXT NOT NULL
        )
        """
    )
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
        created_at = datetime.fromisoformat(row[0])
        return created_at > cutoff
    except Exception:
        return False


def mark_alerted(alert_key):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO alerts (alert_key, created_at) VALUES (?, ?)",
        (alert_key, now_utc().isoformat()),
    )
    conn.commit()
    conn.close()


async def fetch_odds(session, sport, markets):
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": REGIONS,
        "markets": ",".join(markets),
        "oddsFormat": "american",
        "dateFormat": "iso",
    }

    async with session.get(API_BASE.format(sport=sport), params=params, timeout=30) as resp:
        text = await resp.text()
        remaining = resp.headers.get("x-requests-remaining")
        used = resp.headers.get("x-requests-used")

        if resp.status == 429:
            print(f"[429] Rate limited | sport={sport} markets={markets} remaining={remaining} used={used}")
            return []

        if resp.status != 200:
            print(f"[ERROR] sport={sport} markets={markets} HTTP {resp.status}: {text[:500]}")
            return []

        print(f"[OK] sport={sport} markets={markets} remaining={remaining} used={used}")
        return await resp.json()


def game_is_within_prop_window(event):
    raw = event.get("commence_time")
    if not raw:
        return True

    try:
        start = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        delta_hours = (start - now_utc()).total_seconds() / 3600
        return -1 <= delta_hours <= PROP_HOURS_BEFORE_GAME
    except Exception:
        return True


def collect_market_prices(event, market_key):
    rows = []

    for bookmaker in event.get("bookmakers", []):
        book = bookmaker.get("title") or bookmaker.get("key")
        book_key = (bookmaker.get("key") or "").lower()

        if ALLOWED_BOOKS and book_key not in ALLOWED_BOOKS:
            continue

        for market in bookmaker.get("markets", []):
            if market.get("key") != market_key:
                continue

            for outcome in market.get("outcomes", []):
                name = outcome.get("name")
                price = outcome.get("price")
                point = outcome.get("point")
                description = outcome.get("description")

                if name is None or price is None:
                    continue

                rows.append({
                    "book": book,
                    "book_key": bookmaker.get("key"),
                    "market": market_key,
                    "name": name,
                    "description": description,
                    "price": price,
                    "point": point,
                })

    return rows


def group_game_rows(rows):
    grouped = {}
    for row in rows:
        grouped.setdefault(row["name"], []).append(row)
    return grouped


def group_point_market_rows(rows):
    """
    For spread/total markets, do NOT group only by team/name.
    That creates bad comparisons like Cardinals +1.5 vs Cardinals -1.5.

    We group by:
      selection/name + exact point/line

    Example:
      Cardinals -1.5 only compares to Cardinals -1.5
      Cardinals +1.5 only compares to Cardinals +1.5
      Over 8.5 only compares to Over 8.5
    """
    grouped = {}
    for row in rows:
        key = (row["name"], row.get("point"))
        grouped.setdefault(key, []).append(row)
    return grouped


def group_prop_rows(rows):
    """
    Props need exact-ish matching:
    market + player description + side/name + line/point
    Example:
    player_points + LeBron James + Over + 24.5
    """
    grouped = {}
    for row in rows:
        player = row.get("description") or "Unknown Player"
        side = row.get("name")
        point = row.get("point")
        key = (row["market"], player, side, point)
        grouped.setdefault(key, []).append(row)
    return grouped


def find_h2h_outliers(event):
    alerts = []
    rows = collect_market_prices(event, "h2h")
    grouped = group_game_rows(rows)

    for selection, offers in grouped.items():
        if len(offers) < MIN_BOOKS:
            continue

        decimal_prices = [american_to_decimal(o["price"]) for o in offers]
        market_median_decimal = statistics.median(decimal_prices)

        for offer in offers:
            offer_decimal = american_to_decimal(offer["price"])
            ratio = offer_decimal / market_median_decimal if market_median_decimal else 0

            if ratio >= MONEYLINE_RATIO_THRESHOLD:
                alerts.append({
                    "category": "game",
                    "type": "Moneyline Outlier",
                    "selection": selection,
                    "book": offer["book"],
                    "book_key": offer["book_key"],
                    "price": offer["price"],
                    "market_price": decimal_to_american(market_median_decimal),
                    "edge": f"{ratio:.2f}x payout",
                    "offers": offers,
                })

    return alerts


def find_point_market_outliers(event, market_key, point_threshold):
    """
    Spread/total outliers have two useful types:

    1. Same exact line, better price:
       Cardinals -1.5 +170 vs market +145

    2. Different line, same selection, meaningful line gap:
       Cardinals -1.5 vs market Cardinals +1.5

    To avoid fake alerts, we never put opposite lines in the same Books Checked list.
    """
    alerts = []
    rows = collect_market_prices(event, market_key)

    # Price outliers within the exact same line
    exact_line_groups = group_point_market_rows(rows)

    for (selection, point), offers in exact_line_groups.items():
        usable = [o for o in offers if o["point"] is not None]
        if len(usable) < MIN_BOOKS:
            continue

        decimal_prices = [american_to_decimal(o["price"]) for o in usable]
        market_median_decimal = statistics.median(decimal_prices)

        for offer in usable:
            offer_decimal = american_to_decimal(offer["price"])
            ratio = offer_decimal / market_median_decimal if market_median_decimal else 0

            # Use moneyline ratio threshold for same-line price outliers
            if ratio >= MONEYLINE_RATIO_THRESHOLD:
                alerts.append({
                    "category": "game",
                    "type": "Spread Price Outlier" if market_key == "spreads" else "Total Price Outlier",
                    "selection": selection,
                    "book": offer["book"],
                    "book_key": offer["book_key"],
                    "price": offer["price"],
                    "point": offer["point"],
                    "market_point": point,
                    "market_price": decimal_to_american(market_median_decimal),
                    "edge": f"{ratio:.2f}x payout on same line",
                    "offers": usable,
                })

    # True line outliers by selection, shown separately and carefully
    by_selection = group_game_rows(rows)

    for selection, offers in by_selection.items():
        usable = [o for o in offers if o["point"] is not None]
        if len(usable) < MIN_BOOKS:
            continue

        points = [float(o["point"]) for o in usable]
        market_median_point = statistics.median(points)

        for offer in usable:
            diff = abs(float(offer["point"]) - float(market_median_point))

            if diff >= point_threshold:
                # Only show books on the same exact line in Books Checked if possible.
                same_line_offers = [
                    o for o in usable
                    if o.get("point") == offer.get("point")
                ]

                alerts.append({
                    "category": "game",
                    "type": "Spread Line Outlier" if market_key == "spreads" else "Total Line Outlier",
                    "selection": selection,
                    "book": offer["book"],
                    "book_key": offer["book_key"],
                    "price": offer["price"],
                    "point": offer["point"],
                    "market_point": market_median_point,
                    "edge": f"{diff:.1f} pts off market",
                    "offers": same_line_offers if same_line_offers else [offer],
                })

    return alerts


def find_prop_outliers(event, market_key):
    alerts = []
    rows = collect_market_prices(event, market_key)
    grouped = group_prop_rows(rows)

    for (market, player, side, point), offers in grouped.items():
        if len(offers) < PROP_MIN_BOOKS:
            continue

        decimal_prices = [american_to_decimal(o["price"]) for o in offers]
        market_median_decimal = statistics.median(decimal_prices)

        for offer in offers:
            offer_decimal = american_to_decimal(offer["price"])
            ratio = offer_decimal / market_median_decimal if market_median_decimal else 0

            if ratio >= PROP_RATIO_THRESHOLD:
                alerts.append({
                    "category": "prop",
                    "type": "Player Prop Outlier",
                    "market": market,
                    "player": player,
                    "side": side,
                    "point": point,
                    "book": offer["book"],
                    "book_key": offer["book_key"],
                    "price": offer["price"],
                    "market_price": decimal_to_american(market_median_decimal),
                    "edge": f"{ratio:.2f}x payout",
                    "offers": offers,
                })

    return alerts


def format_event_name(event):
    away = event.get("away_team", "Away")
    home = event.get("home_team", "Home")
    return f"{away} @ {home}"


def format_commence_time(event):
    raw = event.get("commence_time")
    if not raw:
        return "N/A"
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt.strftime("%b %d, %I:%M %p UTC")
    except Exception:
        return raw


def pretty_market_key(key):
    return key.replace("_", " ").title()


def format_offers(alert):
    lines = []
    offers = sorted(alert["offers"], key=lambda x: x["book"])

    for o in offers[:12]:
        if alert["category"] == "prop":
            player = o.get("description") or alert.get("player") or ""
            point = "" if o.get("point") is None else f' {o.get("point")}'
            lines.append(f'{o["book"]}: {player} {o["name"]}{point} {fmt_american(o["price"])}')
        elif alert["type"] == "Moneyline Outlier":
            lines.append(f'{o["book"]}: {fmt_american(o["price"])}')
        else:
            lines.append(f'{o["book"]}: {o["name"]} {o.get("point")} {fmt_american(o["price"])}')

    return "\n".join(lines)


async def send_discord_alert(session, webhook, sport, event, alert):
    if not webhook:
        print("[WARN] Missing webhook; alert skipped.")
        return

    event_name = format_event_name(event)
    commence = format_commence_time(event)

    if alert["category"] == "prop":
        point = "" if alert.get("point") is None else f' {alert["point"]}'
        main_line = (
            f'**{alert["player"]} {alert["side"]}{point}**\n'
            f'Market: **{pretty_market_key(alert["market"])}**\n'
            f'Outlier: **{alert["book"]} {fmt_american(alert["price"])}**\n'
            f'Market median: **{fmt_american(alert["market_price"])}**\n'
            f'Edge: **{alert["edge"]}**'
        )
        color = 15844367
    elif alert["type"] == "Moneyline Outlier":
        main_line = (
            f'**{alert["selection"]} ML**\n'
            f'Outlier: **{alert["book"]} {fmt_american(alert["price"])}**\n'
            f'Market median: **{fmt_american(alert["market_price"])}**\n'
            f'Edge: **{alert["edge"]}**'
        )
        color = 15158332
    else:
        market_price_line = ""
        if alert.get("market_price") is not None:
            market_price_line = f'\nMarket median price: **{fmt_american(alert["market_price"])}**'

        main_line = (
            f'**{alert["selection"]}**\n'
            f'Outlier: **{alert["book"]} {alert["point"]} {fmt_american(alert["price"])}**\n'
            f'Market median line: **{alert["market_point"]}**'
            f'{market_price_line}\n'
            f'Edge: **{alert["edge"]}**'
        )
        color = 15158332

    embed = {
        "title": f"🚨 {alert['type']}",
        "description": f"**{event_name}**\n{commence}\n\n{main_line}",
        "color": color,
        "fields": [
            {
                "name": "Books Checked",
                "value": format_offers(alert)[:1000] or "N/A",
                "inline": False,
            }
        ],
        "footer": {
            "text": f"{sport} • Odds Outlier Bot"
        },
        "timestamp": now_utc().isoformat(),
    }

    async with session.post(webhook, json={"embeds": [embed]}, timeout=20) as resp:
        if resp.status not in (200, 204):
            text = await resp.text()
            print(f"[DISCORD ERROR] HTTP {resp.status}: {text[:300]}")
        else:
            print(f"[ALERT SENT] {alert['type']} | {event_name} | {alert['book']}")


def build_alert_key(sport, event, alert):
    event_id = event.get("id", format_event_name(event))

    if alert["category"] == "prop":
        parts = [
            sport,
            event_id,
            alert["type"],
            alert.get("market", ""),
            alert.get("player", ""),
            alert.get("side", ""),
            str(alert.get("point")),
            alert.get("book_key") or alert.get("book"),
            str(alert.get("price")),
            str(alert.get("market_price")),
        ]
    else:
        parts = [
            sport,
            event_id,
            alert["type"],
            alert.get("selection", ""),
            alert.get("book_key") or alert.get("book"),
            str(alert.get("price")),
            str(alert.get("point")),
            str(alert.get("market_point") or alert.get("market_price")),
        ]

    return "|".join(parts)


async def scan_game_once(session):
    for sport in GAME_SPORT_KEYS:
        events = await fetch_odds(session, sport, GAME_MARKETS)

        for event in events:
            alerts = []

            if "h2h" in GAME_MARKETS:
                alerts.extend(find_h2h_outliers(event))

            if "spreads" in GAME_MARKETS:
                alerts.extend(find_point_market_outliers(event, "spreads", SPREAD_POINT_DIFF_THRESHOLD))

            if "totals" in GAME_MARKETS:
                alerts.extend(find_point_market_outliers(event, "totals", TOTAL_POINT_DIFF_THRESHOLD))

            for alert in alerts:
                key = build_alert_key(sport, event, alert)
                if recently_alerted(key):
                    continue
                await send_discord_alert(session, GAME_WEBHOOK, sport, event, alert)
                mark_alerted(key)


async def scan_props_once(session):
    for sport, prop_markets in PROP_SCAN_CONFIG.items():
        # Fetch each sport's selected prop markets together.
        events = await fetch_odds(session, sport, prop_markets)

        for event in events:
            if not game_is_within_prop_window(event):
                continue

            alerts = []
            for market in prop_markets:
                alerts.extend(find_prop_outliers(event, market))

            for alert in alerts:
                key = build_alert_key(sport, event, alert)
                if recently_alerted(key):
                    continue
                await send_discord_alert(session, PROP_WEBHOOK, sport, event, alert)
                mark_alerted(key)


async def game_loop(session):
    while True:
        try:
            await scan_game_once(session)
        except Exception as e:
            print(f"[GAME SCAN ERROR] {type(e).__name__}: {e}")

        await asyncio.sleep(GAME_POLL_SECONDS)


async def prop_loop(session):
    # Small delay so game and prop scans do not hit at exact same second.
    await asyncio.sleep(10)

    while True:
        try:
            await scan_props_once(session)
        except Exception as e:
            print(f"[PROP SCAN ERROR] {type(e).__name__}: {e}")

        await asyncio.sleep(PROP_POLL_SECONDS)


async def main():
    if not ODDS_API_KEY:
        raise RuntimeError("Missing ODDS_API_KEY")
    if not GAME_WEBHOOK:
        raise RuntimeError("Missing GAME_DISCORD_WEBHOOK_URL or DISCORD_WEBHOOK_URL")

    init_db()

    print("Odds Outlier Bot Phase 1 Lite started")
    print(f"Game sports: {GAME_SPORT_KEYS}")
    print(f"Game markets: {GAME_MARKETS}")
    print(f"Prop config: {PROP_SCAN_CONFIG}")
    print(f"Game poll seconds: {GAME_POLL_SECONDS}")
    print(f"Prop poll seconds: {PROP_POLL_SECONDS}")
    print(f"Prop window hours: {PROP_HOURS_BEFORE_GAME}")

    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            game_loop(session),
            prop_loop(session),
        )


if __name__ == "__main__":
    asyncio.run(main())
