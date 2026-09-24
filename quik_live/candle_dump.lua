--[[
    candle_dump.lua — QUIK Lua glue для quik_feed сервиса.

    Что делает: каждые 5 секунд читает 1-min OHLCV завершённые свечи
    по 19 (+4 cross-asset) тикерам из QUIK CreateDataSource(INTERVAL_M1),
    пишет новые завершённые свечи в Excel через DDE poke (или в CSV
    файл fallback).

    Загрузка в QUIK Workstation:
      Сервисы → Lua скрипты → Добавить скрипт → выбрать этот файл → Запустить

    Output:
      D:\quik_sber\newsbot\newsbot3\quik_live\candles.csv   (always)
      DDE pokes в Excel "candles.xlsx" sheet "candles"      (если xls открыт)
]]--

-- Замени на путь к quik_live/ в своём клоне (Lua внутри QUIK не знает корень проекта).
local CSV_PATH = "D:\\quik_sber\\newsbot\\newsbot3\\quik_live\\candles.csv"
local POLL_MS = 5000  -- 5 seconds
local LOOKBACK_BARS = 5  -- сколько последних завершённых баров проверять (защита от gap)
-- ДОЗАГРУЗКА ВСЕГО РАБОЧЕГО ДНЯ при старте/ребуте: первые циклы берём большой
-- lookback. Полная сессия MOEX = утренний аукцион ~06:50 / торги 07:00 → 23:50
-- ≈ 1000 1m-баров (минус клиринги). Берём 2000 (≈весь день + запас в предыдущий),
-- пока QUIK datasource грузит историю; last_ts_per_ticker не даёт дублей. quik_feed
-- читает CSV с начала (bootstrap_mode=all), сверяет с candles:1m → публикует недостающее.
-- ⚠ Глубина зависит от истории, которую QUIK загрузил в CreateDataSource: чтобы утро
--   (07:00) восстанавливалось, в настройках QUIK history depth должна покрывать день.
local STARTUP_LOOKBACK_BARS = 2000
local STARTUP_FULL_CYCLES = 6   -- ~30с: datasource по 20+ инструментам грузит историю не мгновенно

-- (class, code) для 19 trade + 4 cross-asset.
--
-- ⚠ ФЬЮЧЕРСНАЯ РОЛЛИРОВКА: коды ниже — фронт U6 (сентябрь 2026). Перед использованием
--    замени их на текущие фронт-контракты (см. «Процедура переката» ниже).
--
-- ВНИМАНИЕ: разные контракты экспирируются с разной частотой!
--
-- ── КВАРТАЛЬНЫЕ (раз в 3 месяца) ───────────────────────────────────────
--    Si, MX, CR, GD — третий четверг марта/июня/сентября/декабря
--    Сейчас active: M6 (ИЮНЬ 2026), экспирация = 2026-06-18 (3-й чт июня)
--    Roll schedule (за неделю до expiry):
--       2026-06-15: M6 → U6 (Sep 2026)
--       2026-09-15: U6 → Z6 (Dec 2026)
--       2026-12-15: Z6 → H7 (Mar 2027)
--       2027-03-15: H7 → M7 (Jun 2027)
--
-- ── МЕСЯЧНЫЕ (каждый месяц) ────────────────────────────────────────────
--    BR (Brent), NG (NatGas) — экспирируются примерно 1-го числа месяца
--    БУКВЕННЫЕ КОДЫ МЕСЯЦА (F-Jan, G-Feb, H-Mar, J-Apr, K-May, M-Jun,
--                            N-Jul, Q-Aug, U-Sep, V-Oct, X-Nov, Z-Dec)
--    Сейчас active: M6 (Июнь), но экспирация СЕГОДНЯ 2026-06-01 в 19:00 MSK
--    Roll schedule (накануне или утром в день экспирации):
--       2026-06-02 утром: BRM6 → BRN6, NGM6 → NGN6 (July)
--       2026-07-02 утром: BRN6 → BRQ6, NGN6 → NGQ6 (Aug)
--       2026-08-04 утром: BRQ6 → BRU6, NGQ6 → NGU6 (Sep)
--       и т.д. каждый месяц
--
-- ── ПРОЦЕДУРА ПЕРЕКАТА ─────────────────────────────────────────────────
--   1. Заменить устаревшие коды в SECURITIES таблице ниже
--   2. Обновить ключи QUIK_TO_TICKER (например "SPBFUT/BRM6" → "SPBFUT/BRN6")
--   3. Обновить эту шапку: дата + текущие коды
--   4. Проверить в QUIK что новый код ликвиден (есть стакан + сделки)
--   5. Перезапустить candle_dump.lua в QUIK
local SECURITIES = {
    -- Trade whitelist (12)
    {"TQBR", "YDEX"},  -- YNDX legacy
    {"TQBR", "GAZP"},
    {"SPBFUT", "NGU6"},   -- NG futures (U6 сентябрь)
    {"SPBFUT", "BRU6"},   -- BR futures (U6 сентябрь)
    {"TQBR", "PLZL"},
    {"TQBR", "GMKN"},
    {"TQBR", "TATN"},
    {"TQBR", "MGNT"},
    {"TQBR", "VTBR"},
    {"TQBR", "NVTK"},
    {"TQBR", "ROSN"},
    {"TQBR", "LKOH"},
    -- Phase 2 reference (7) + cross-asset
    {"TQBR", "SBER"},
    {"TQBR", "MTSS"},
    {"SPBFUT", "SiU6"},   -- Si legacy → canonical SI (перекат M6→U6 2026-06-18, экспирация M6 19:00)
    {"SPBFUT", "MXU6"},   -- MX legacy → canonical MIX
    {"SPBFUT", "CRU6"},   -- CNY
    {"CETS",   "USD000UTSTOM"},  -- USDRUB
    {"SPBFUT", "GDU6"},   -- GOLD legacy → canonical GLDRUB (GOLD-9.26)
}

-- (class, code) → legacy ticker name для Python side (matches CSV_PREFIX_FOR_CANONICAL)
local QUIK_TO_TICKER = {
    ["TQBR/YDEX"]="YNDX", ["TQBR/GAZP"]="GAZP", ["SPBFUT/NGU6"]="NG",
    ["SPBFUT/BRU6"]="BR", ["TQBR/PLZL"]="PLZL", ["TQBR/GMKN"]="GMKN",
    ["TQBR/TATN"]="TATN", ["TQBR/MGNT"]="MGNT", ["TQBR/VTBR"]="VTBR",
    ["TQBR/NVTK"]="NVTK", ["TQBR/ROSN"]="ROSN", ["TQBR/LKOH"]="LKOH",
    ["TQBR/SBER"]="SBER", ["TQBR/MTSS"]="MTSS",
    ["SPBFUT/SiU6"]="Si", ["SPBFUT/MXU6"]="MX",
    ["SPBFUT/CRU6"]="CNY",
    ["CETS/USD000UTSTOM"]="USDRUB", ["SPBFUT/GDU6"]="GOLD",
}

local last_ts_per_ticker = {}  -- ticker → last completed bar timestamp seen
local is_run = true
local data_sources = {}  -- (class/code) → datasource handle
local cycle = 0          -- счётчик циклов: первые STARTUP_FULL_CYCLES дампим всю сессию

function OnInit(path)
    -- TRUNCATE на старте ("w"): свежий файл = только сегодняшняя сессия (её дозальём
    -- из datasource ниже). Старые свечи durable в candles:1m (Redis AOF). Так CSV не
    -- растёт безгранично, а quik_feed читает его с начала и сверяет с Redis.
    local f = io.open(CSV_PATH, "w")
    if f == nil then
        message("candle_dump.lua: cannot open "..CSV_PATH, 1)
        return
    end
    f:write("ticker,ts,open,high,low,close,volume\n")
    f:close()
    message("candle_dump.lua: initialized (truncated), CSV="..CSV_PATH, 1)
end

function OnStop()
    is_run = false
    for _, ds in pairs(data_sources) do
        if ds.Close then ds:Close() end
    end
end

local function get_or_create_ds(class_code, sec_code)
    local key = class_code.."/"..sec_code
    if data_sources[key] == nil then
        local ds, err = CreateDataSource(class_code, sec_code, INTERVAL_M1)
        if ds == nil then
            message("DataSource fail "..key..": "..tostring(err), 2)
            return nil
        end
        data_sources[key] = ds
        -- даём DS пару секунд на bootstrap
    end
    return data_sources[key]
end

local function ts_to_iso(t)
    -- t это таблица {year, month, day, hour, min, sec}
    return string.format("%04d-%02d-%02d %02d:%02d:00",
        t.year, t.month, t.day, t.hour, t.min)
end

local function dump_completed_bars()
    local f = io.open(CSV_PATH, "a")
    if f == nil then return 0 end

    local n_dumped = 0

    -- первые STARTUP_FULL_CYCLES циклов — большой lookback (вся сегодняшняя сессия),
    -- т.к. datasource грузит историю не мгновенно; дальше — обычный LOOKBACK_BARS.
    cycle = cycle + 1
    local lookback = LOOKBACK_BARS
    if cycle <= STARTUP_FULL_CYCLES then lookback = STARTUP_LOOKBACK_BARS end

    for _, sec in ipairs(SECURITIES) do
        local class_code, sec_code = sec[1], sec[2]
        local key = class_code.."/"..sec_code
        local ticker = QUIK_TO_TICKER[key] or sec_code
        local ds = get_or_create_ds(class_code, sec_code)
        if ds ~= nil then
            local n = ds:Size()
            -- последний бар может быть ещё не завершён (текущая минута) →
            -- берём предпоследний и несколько lookback (на старте — всю сессию)
            local start_idx = math.max(1, n - lookback - 1)
            local end_idx = n - 1  -- exclude текущую (incomplete) минуту
            local last_seen = last_ts_per_ticker[ticker] or ""
            for i = start_idx, end_idx do
                local ts = ds:T(i)
                if ts ~= nil then
                    local ts_str = ts_to_iso(ts)
                    if ts_str > last_seen then
                        local o = ds:O(i)
                        local h = ds:H(i)
                        local l = ds:L(i)
                        local c = ds:C(i)
                        local v = ds:V(i)
                        f:write(string.format("%s,%s,%.4f,%.4f,%.4f,%.4f,%d\n",
                            ticker, ts_str, o, h, l, c, v))
                        last_ts_per_ticker[ticker] = ts_str
                        n_dumped = n_dumped + 1
                    end
                end
            end
        end
    end

    f:close()
    return n_dumped
end

function main()
    -- bootstrap delay для DataSource'ов
    sleep(3000)
    message("candle_dump.lua: main loop starting", 1)
    while is_run do
        -- per-tick popup убран по запросу оператора: observability
        -- идёт через quik_feed heartbeat и Monitor, всплывающее окно
        -- каждые 5 секунд мешает работе в QUIK.
        dump_completed_bars()
        sleep(POLL_MS)
    end
    message("candle_dump.lua: main loop stopped", 1)
end
