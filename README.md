# SportsGameOdds Odds Outlier Bot

Uses `GET https://api.sportsgameodds.com/v2/events`.

SportsGameOdds response structure:
- main data is in `data`
- event odds are in `event.odds`
- bookmaker odds are in `odds.<oddID>.byBookmaker.<bookmakerID>`
- oddID format is `{statID}-{statEntityID}-{periodID}-{betTypeID}-{sideID}`

Railway variables are listed in `.env.example`.

This bot:
- filters sportsbooks with `ALLOWED_BOOKS`
- scans NBA, MLB, NHL, EPL, MLS, WNBA
- checks moneyline, spread, total, and selected props
- requires `MIN_BOOKS` before alerting
- compares spreads/totals/props only on the same exact line
- sends Discord webhook alerts
- includes deeplinks when SGO provides them
