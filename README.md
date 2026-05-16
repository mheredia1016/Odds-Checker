# Odds Outlier Discord Bot — Phase 1 Lite

This is the credit-safer version.

It scans:

## Game Odds
Every 60 seconds:
- NBA
- WNBA
- MLB
- NHL
- EPL
- MLS

Markets:
- Moneyline
- Spreads
- Totals

## Player Props
Every 5 minutes:
- NBA player points
- NBA threes
- WNBA player points
- WNBA threes
- MLB batter home runs
- NHL shots on goal

Props are only scanned within 4 hours of game start by default.

## Why this version

The Odds API can burn credits quickly if you scan every sport, every market, every player prop, every minute.

This version separates:

```txt
game odds: faster
player props: slower and selective
```

## Railway setup

Upload this repo to Railway and set these variables:

```env
ODDS_API_KEY=
GAME_DISCORD_WEBHOOK_URL=
PROP_DISCORD_WEBHOOK_URL=
GAME_SPORT_KEYS=basketball_nba,basketball_wnba,baseball_mlb,icehockey_nhl,soccer_epl,soccer_usa_mls
GAME_MARKETS=h2h,spreads,totals
PROP_SCAN_CONFIG=basketball_nba:player_points|player_threes;basketball_wnba:player_points|player_threes;baseball_mlb:batter_home_runs;icehockey_nhl:player_shots_on_goal
REGIONS=us
GAME_POLL_SECONDS=60
PROP_POLL_SECONDS=300
PROP_HOURS_BEFORE_GAME=4
MIN_BOOKS=4
MONEYLINE_RATIO_THRESHOLD=1.50
SPREAD_POINT_DIFF_THRESHOLD=2.0
TOTAL_POINT_DIFF_THRESHOLD=2.0
PROP_MIN_BOOKS=4
PROP_RATIO_THRESHOLD=1.60
COOLDOWN_MINUTES=45
```

## Notes

- Leave `PROP_DISCORD_WEBHOOK_URL` blank if you want prop alerts to go to the game odds channel.
- Soccer props are not included in Phase 1 Lite because coverage and naming can vary heavily by league.
- Add more soccer leagues only when needed.
- If you hit rate limits, raise `PROP_POLL_SECONDS` to 600.


## Sportsbook Filtering

You can limit which sportsbooks are scanned.

Example:

```env
ALLOWED_BOOKS=fanduel,draftkings,betmgm,williamhill_us,bet365,espnbet,hardrockbet
```

This helps:
- reduce fake alerts
- ignore small/offshore books
- create cleaner market consensus
- focus on books you actually use


## Patch Notes

This version fixes spread/total grouping.

Before:
- Cardinals +1.5 and Cardinals -1.5 could appear in the same comparison.

Now:
- Same-line price checks compare only exact line matches.
- Line-difference checks do not mix opposite lines in the Books Checked section.
- Offshore/random books are still filtered by `ALLOWED_BOOKS`.
