# Data dictionary

Источник: Binance `GET /api/v3/klines` (свечи/klines, интервал 1 час). Одна запись = одна часовая свеча одной пары.

## Исходные поля (`data/raw_data.csv`)

| Поле | Тип | Описание |
|---|---|---|
| symbol | категориальное | торговая пара (BTCUSDT, ETHUSDT, ...) добавлено клиентом |
| interval | категориальное | интервал свечи, всегда `1h` (добавлено клиентом) |
| open_time | дата/время | начало свечи, Unix time в миллисекундах (UTC) |
| open | числовое | цена открытия, USDT |
| high | числовое | максимальная цена за час |
| low | числовое | минимальная цена за час |
| close | числовое | цена закрытия |
| volume | числовое | объём торгов в базовой монете (например, BTC) |
| close_time | дата/время | конец свечи, мс (UTC) |
| quote_volume | числовое | объём торгов в USDT |
| trades_count | числовое (целое) | число сделок за час |
| taker_buy_base_volume, taker_buy_quote_volume | числовое | объём покупок по рынку (в очищенные данные не переносятся) |

Служебное поле потока: `is_test` (bool), True только у искусственных неверных записей, добавленных продюсером.

## Производные поля (`data/clean_data.csv` и таблица SQLite `klines`)

| Поле | Тип | Формула / смысл |
|---|---|---|
| base_asset, quote_asset | категориальное | разбор пары: `BTCUSDT` → `BTC`, `USDT` |
| open_time, close_time | дата/время | `YYYY-MM-DD HH:MM:SS` (UTC) |
| log_return | числовое | `ln(close / close_prev)` внутри пары; NULL у первой свечи |
| price_range_pct | числовое | `(high − low) / open × 100`: часовая волатильность, % |
| body_pct | числовое | `(close − open) / open × 100` |
| price_trend | категориальное | `up` (> +0.1%), `down` (< −0.1%), иначе `flat` |
| volatility_level | категориальное | `low/medium/high` по 33.3 и 66.6 процентилям `price_range_pct` |
| volume_zscore | числовое | z-оценка `volume` внутри пары |
| coin_group | категориальное | `large_cap` (BTC, ETH, BNB) или `alt` |
| hour | целое | час суток открытия свечи (0–23, UTC) |
| day_of_week | категориальное | день недели (Monday ... Sunday) |

Первичный ключ таблицы: `(symbol, open_time)`.
