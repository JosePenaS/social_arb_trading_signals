#!/usr/bin/env Rscript

# ================================================================
# Generate trade signals ONLY for threads not yet in tweet_signals
# and upsert into Supabase (tweet_signals)
# ================================================================

suppressPackageStartupMessages({
  library(dplyr); library(purrr); library(stringr); library(lubridate)
  library(jsonlite); library(glue); library(httr)
  library(DBI); library(RPostgres); library(tidyr)
})

`%||%` <- function(x, y) if (is.null(x)) y else x

# -------------------------- Config ------------------------------

OPENAI_MODEL  <- Sys.getenv("OPENAI_MODEL", "gpt-4o")
BATCH_LIMIT   <- as.integer(Sys.getenv("BATCH_LIMIT", "150"))  # max new threads per run

PG_HOST <- Sys.getenv("SUPABASE_HOST")
PG_PORT <- as.integer(Sys.getenv("SUPABASE_PORT", "5432"))
PG_DB   <- Sys.getenv("SUPABASE_DB", "postgres")
PG_USER <- Sys.getenv("SUPABASE_USER")
PG_PWD  <- Sys.getenv("SUPABASE_PWD")

if (PG_HOST == "" || PG_USER == "" || PG_PWD == "") {
  stop("Database env vars missing: SUPABASE_HOST/USER/PWD.")
}
if (Sys.getenv("OPENAI_API_KEY") == "") {
  stop("OPENAI_API_KEY env var is missing.")
}

# -------------------------- DB connect --------------------------

con <- dbConnect(
  RPostgres::Postgres(),
  host = PG_HOST, port = PG_PORT, dbname = PG_DB,
  user = PG_USER, password = PG_PWD, sslmode = "require"
)
on.exit(try(dbDisconnect(con), silent = TRUE))

# Ensure target table exists so the anti-join query works on first run
invisible(dbExecute(con, "
  CREATE TABLE IF NOT EXISTS tweet_signals (
    tweet_key         text PRIMARY KEY,
    username          text,
    conversation_id   text,
    created_et        timestamptz,
    text              text,
    ticker            text,
    conviction        numeric,
    direction         text,
    horizon           text,
    stop_loss_pct     integer,
    buy_zone          boolean,
    sentiment_score   numeric,
    rationale         text,
    specificity_score numeric,
    has_quant_data    boolean,
    evidence_score    numeric,
    evidence_types    text,
    evidence_snippet  text
  );
"))

# -------------------------- Pull ONLY unseen threads ------------

qry <- sprintf("
  WITH seen AS (
    SELECT DISTINCT conversation_id FROM tweet_signals
  )
  SELECT t.conversation_id, t.username, t.created, t.text
  FROM twitter_threads t
  LEFT JOIN seen s USING (conversation_id)
  WHERE s.conversation_id IS NULL
  ORDER BY t.created ASC
  LIMIT %d;
", BATCH_LIMIT)

collapsed_tbl <- dbGetQuery(con, qry)

if (nrow(collapsed_tbl) == 0) {
  message("No unseen threads to process. Exiting.")
  quit(status = 0)
}

# Normalize timezones (keep ET column)
tbl_et <- collapsed_tbl %>%
  mutate(
    created_utc = with_tz(as_datetime(created), tzone = "UTC"),
    created_et  = with_tz(created_utc, tzone = "America/New_York")
  ) %>%
  select(conversation_id, username, text, created_et)

# -------------------------- OpenAI helpers ----------------------

expected_schema <- tibble::tibble(
  ticker            = character(),
  conviction        = numeric(),
  direction         = character(),
  horizon           = character(),
  stop_loss_pct     = integer(),
  buy_zone          = logical(),
  sentiment_score   = numeric(),
  rationale         = character(),
  specificity_score = numeric(),
  has_quant_data    = logical(),
  evidence_score    = numeric(),
  evidence_types    = character(),
  evidence_snippet  = character()
)

ask_gpt <- function(prompt,
                    model       = OPENAI_MODEL,
                    temperature = 0,
                    max_tokens  = 1700,
                    retries     = 3) {
  system_prompt <- "You are a concise analyst. Return exactly the JSON asked—no markdown."
  for (k in seq_len(retries)) {
    resp <- tryCatch(
      RETRY(
        "POST",
        url = "https://api.openai.com/v1/chat/completions",
        times     = 3,
        pause_min = 2, pause_cap = 8,
        add_headers(Authorization = paste("Bearer", Sys.getenv("OPENAI_API_KEY"))),
        content_type_json(), encode = "json",
        body = list(
          model           = model,
          temperature     = temperature,
          max_tokens      = max_tokens,
          response_format = list(type = "json_object"),
          messages = list(
            list(role = "system", content = system_prompt),
            list(role = "user",   content = prompt)
          )
        ),
        timeout(300), config(connecttimeout = 60)
      ),
      error = identity
    )
    if (inherits(resp, "error")) {
      message("HTTP error: ", resp$message)
    } else {
      parsed <- content(resp, as = "parsed", simplifyVector = FALSE)
      if (!is.null(parsed$error)) return(paste0("[OPENAI-ERROR] ", parsed$error$message))
      if (length(parsed$choices) > 0) return(str_trim(parsed$choices[[1]]$message$content))
    }
    Sys.sleep(2^k)
  }
  NA_character_
}

make_tweet_prompt <- function(tweet_text) {
  safe_txt <- tweet_text |> str_replace_all("\\{", "{{") |> str_replace_all("\\}", "}}")
  glue("
You are a trading assistant that converts a single tweet into a *strict* JSON trade-idea list.

TWEET
-----
{safe_txt}
-----

TASK
----
1) Detect every stock / ETF / ADR ticker with enough substance for a trade idea.
   Use tidyquant/Yahoo symbols (e.g., AAPL, SHOP.TO, 7974.T, 9992.HK). Ignore $ prefixes.

2) Return EXACTLY one JSON object with a single key \"signals\" mapping to an array of idea objects.

3) Every idea inside \"signals\" MUST include ALL fields below and obey constraints:

| field              | type / range / rules                                                  |
|--------------------|-----------------------------------------------------------------------|
| ticker             | string (tidyquant symbol, no $)                                       |
| conviction         | float 0–1 (clip to [0,1])                                             |
| direction          | \"long\" | \"short\"                                                  |
| horizon            | \"short\" | \"mid\" | \"long\"                                        |
| stop_loss_pct      | integer 8–25 (clip to [8,25])                                         |
| buy_zone           | true | false                                                          |
| sentiment_score    | float −1 … 1 (clip to [−1,1])                                         |
| rationale          | string ≤ 60 words                                                     |
| specificity_score  | float 0–1  (see rubric; clip to [0,1])                                |
| has_quant_data     | boolean (true if tweet cites numbers, e.g., revenue, units, %s)       |
| evidence_score     | float 0–1  (see rubric; clip to [0,1])                                |
| evidence_types     | string of comma-separated tags from:                                  |
|                    |   financials, sales, guidance, macro, technical, news, stat,         |
|                    |   supply_chain, survey, alt_data, on_chain, insider, legal, other    |
| evidence_snippet   | ≤ 30 words quoted/paraphrased from tweet showing the evidence;        |
|                    |   empty string if none                                                |

4) Specificity rubric:
   - 0.0–0.2: vague opinion; no timeframe/catalyst
   - 0.3–0.5: directional view without quantities or timing
   - 0.6–0.8: concrete catalyst/timeframe/driver
   - 0.9–1.0: quantified forecast (numbers/dates/targets) or detailed mechanism

5) Evidence rubric:
   - 0.0: no evidence
   - 0.1–0.4: anecdote/weak reference
   - 0.5–0.7: some numbers or specific sources
   - 0.8–1.0: multiple verifiable figures/sources or high-quality data

6) If an idea is too weak (no ticker OR specificity_score < 0.30) DROP it.
   If no valid ideas remain, return {{\"signals\": []}}.

7) Enforce all ranges by scaling/rounding/clipping. DO NOT include extra keys.
   DO NOT wrap the JSON in markdown.
")
}

safe_ask <- purrr::possibly(
  function(txt, model = OPENAI_MODEL, temperature = 0, max_tokens = 1700) {
    ask_gpt(txt, model = model, temperature = temperature, max_tokens = max_tokens)
  },
  otherwise = NA_character_
)

# -------------------------- Build prompts & call GPT ------------

df <- tbl_et %>%
  select(conversation_id, created_et, text, username) %>%
  mutate(prompt = map_chr(text, make_tweet_prompt))

message(sprintf("Processing %d unseen threads (limit %d)…", nrow(df), BATCH_LIMIT))

df <- df %>%
  mutate(
    gpt_raw = map2_chr(prompt, seq_len(n()), function(pr, i) {
      if (i %% 10 == 0 || i == 1) message(sprintf("  • GPT %d / %d", i, nrow(df)))
      safe_ask(pr)
    })
  )

# -------------------------- Safe JSON parse ---------------------

# typed 0-row tibble with all expected columns
empty_signals <- tibble::as_tibble(expected_schema)[0, ]

# helper to coerce missing columns to the expected types
.fill_missing_cols <- function(out, expected_schema) {
  miss <- setdiff(names(expected_schema), names(out))
  if (!length(miss)) return(out)
  for (nm in miss) {
    tmpl <- expected_schema[[nm]]
    if ("numeric" %in% class(tmpl))   out[[nm]] <- rep(as.numeric(NA),  nrow(out))
    else if ("integer" %in% class(tmpl))  out[[nm]] <- rep(as.integer(NA),  nrow(out))
    else if ("logical" %in% class(tmpl))  out[[nm]] <- rep(as.logical(NA),  nrow(out))
    else out[[nm]] <- rep(as.character(NA), nrow(out))
  }
  out
}

parse_signals_one <- function(txt, cid) {
  if (is.na(txt) || txt == "") {
    message("🧹 ", cid, " empty GPT reply")
    return(empty_signals)
  }

  out <- tryCatch(
    jsonlite::fromJSON(txt)$signals,
    error = function(e) {
      message("🧹 ", cid, " JSON parse error")
      NULL
    }
  )

  if (is.null(out) || length(out) == 0) {
    return(empty_signals)
  }

  out <- tibble::as_tibble(out)
  out <- .fill_missing_cols(out, expected_schema)

  out %>%
    mutate(
      conviction        = pmin(pmax(as.numeric(conviction), 0), 1),
      stop_loss_pct     = as.integer(stop_loss_pct),
      sentiment_score   = as.numeric(sentiment_score),
      buy_zone          = as.logical(buy_zone),
      direction         = as.character(direction),
      horizon           = as.character(horizon),
      rationale         = map_chr(rationale, ~ paste(as.character(.x), collapse = " ")),
      specificity_score = pmin(pmax(as.numeric(specificity_score), 0), 1),
      has_quant_data    = as.logical(has_quant_data),
      evidence_score    = pmin(pmax(as.numeric(evidence_score), 0), 1),
      evidence_types    = as.character(evidence_types %||% ""),
      evidence_snippet  = as.character(evidence_snippet %||% "")
    )
}

df <- df %>%
  mutate(
    gpt_table = map2(gpt_raw, conversation_id, parse_signals_one)
  )

# Optional telemetry
ideas_per_thread <- purrr::map_int(df$gpt_table, nrow)
message(sprintf(
  "📊 threads=%d | threads_with_ideas=%d | total_ideas=%d",
  nrow(df), sum(ideas_per_thread > 0), sum(ideas_per_thread)
))

# -------------------------- Ticker normalization ----------------

.fix_one <- function(sym) {
  sym <- str_to_upper(sym)
  stock_map <- c(
    CROCS="CROX", TDMX="TMDX", ATZ="ATZ.TO", PUMA="PUM.DE",
    HUGO="BOSS.DE", PORTL="PRPL", LNMD="LNMD3.SA", DGHI="DGHI.V",
    NEWS="NWSA", ASICS="7936.T", KRKN="KRAKEN.V", GPS="GPS",
    SHARKNINJA="SN", CELSIUS="CELH", RENK="R5RK.DE",
    ARITZIA="ATZ.TO", `KRX:278470`="278470.KS", CAPCOM="9697.T",
    LVMH="MC.PA", DIAGEO="DEO", LILY="LLY", BUILD="BLDR",
    AIRLINES="JETS", TSMC="TSM", GENR="GNRC", POP="9992.HK", ZYN="PM",
    `2501.TSE`="2501.T", `8050`="8050.T", `973.XHKG`="0973.HK",
    ADS="ADS.DE", `ADS.GY`="ADS.DE", ADYRY="ASBRF", BRBY="BRBY.L",
    NTODY="NTDOY", `278470`="278470.KS", `018290`="018260.KS",
    LGHNH="003550.KS", `LGHNH.KS`="003550.KS", `EL.F`="EL.PA",
    DOCK="DOCK.L", `FOI-B`="FOI-B.ST", GAW="GAW.L", OIL="USO",
    SP500="^GSPC", NASDAQ="^IXIC", BTC.X="BTC-USD", FB="META",
    GGPI="PSNY", GIK="NVVE", LAC="LAC", HEAR="HEAR",
    MODN="MODN", MPL="MPL.AX", MTTR="MTTR", NEWR="NEWR",
    NIBE="NIBE-B.ST", NVEI="NVEI.TO", NOVO="NVO", OZON="OZON",
    OSTK="OSTK", PDYPY="PDYPY", PLNHF="PLNHF", PYCR="PYCR",
    QNT="QNT.L", SAVE="SAVE", SKL="SKIL", SMAR="SMAR",
    SNROY="SNROF", SQ="SQ", SQSP="SQSP", TPX="TPX", TRTN="TRTN",
    TTCF="TTCF", TWOU="TWOU", TWTR="TWTR", VSTO="VSTO", VVNT="VVNT",
    WIRE="WIRE", WOSG="WOSG.L", WWE="TKO", YY="YY", ZEV="ZEV",
    `Z1P.AX`="ZIP.AX"
  )
  if (sym %in% names(stock_map)) return(unname(stock_map[sym]))

  crypto_keep <- c(
    "BTC","ETH","ADA","DOGE","DOT","AVAX","LINK","MANA","1INCH","AAVE","APE","ARB","ALGO",
    "MASK","METIS","MILK","MUTE","MYRO","NEXO","NFT","NFTX","OKB","OCEAN","OXT","PEPE","PYR",
    "PYUSD","RNDR","RSR","RVN","SLERF","SNEK","LQTY","FXS","FRAX","GAS","GMX","KAS","ICP","IMX",
    "ILV","JASMY","KDA","KEEP","DOCK","MELD","MOBILE","DYM","DOLA","SAI","LRC","LUSD","RENBTC",
    "REQ","REP","RLC","RON","AUDIO","BAT","CRO","CVC","DCR","DERO","DIONE","GODS","HNT","ICX",
    "IOTX","LPT","LOOKS","LOOM","LADYS","MKR","MBL","NKN","NOIA","ONT","ORAI","PAAL","PAXG","POLY",
    "PSYOP","QUACK","RAAS","RETH","REVV","SFP","SHIB","SKL","SPELL","SSV","STARL","STEEM","STRD",
    "STRK","SUSHI","SWETH","SYS","TAO","TAROT","TFUEL","THETA","TIA","TLM","TLOS","TORN","TOSHI",
    "UMA","USDT","VEE","VELO","VRSC","WAXP","WBNB","WOO","XAUT","XCH","XCN","XDC","XDEFI","XEC",
    "XEM","XLM","XMR","XPR","XRD","XRP","XVG","XYO","ZANO","ZEC","ZEN"
  )
  if (sym %in% crypto_keep) return(paste0(sym, "-USD"))

  skip_syms <- c(
    "SOCIALARB","SOLAMA","TOKEN","TICKERPLUS","TIPRANKS","TRUMP","TTRENDS",
    "TRENDS","NETVR","PWEASE","PUBLIC","HOUSEHACK","FIGMA","FIGM","FIGR",
    "FIGUREAI","FIGURE.AI","APPTRONIK","SANCTUARY.AI","OPENSEA","MRBEAST",
    "KLAR","KLARNA","BOOPBAKERYCO","BOBO","BAYC","SHIPPING","VARDASPACE",
    "TEMU","TES","TULAV","WLD"
  )
  if (sym %in% skip_syms) return(NA_character_)

  sym
}
fix_ticker <- function(sym_vec) vapply(sym_vec, .fix_one, character(1))

signals_from_tweets <- df %>%
  select(username, conversation_id, created_et, text, gpt_table) %>%
  tidyr::unnest(gpt_table, keep_empty = TRUE) %>%   # keep typed empties safely
  filter(!is.na(ticker)) %>%
  mutate(ticker = fix_ticker(ticker)) %>%
  filter(!is.na(ticker))

if (nrow(signals_from_tweets) == 0) {
  message("No valid signals produced from GPT for unseen threads. Exiting.")
  quit(status = 0)
}

# Deduplicate & prioritize better-scored signals
signals_from_tweets <- signals_from_tweets %>%
  mutate(
    conversation_id = as.character(conversation_id),
    created_et      = as.POSIXct(created_et, tz = "America/New_York"),
    ticker          = toupper(as.character(ticker)),
    buy_zone        = as.logical(buy_zone),
    has_quant_data  = as.logical(has_quant_data),
    stop_loss_pct   = as.integer(stop_loss_pct)
  ) %>%
  arrange(
    conversation_id, ticker, created_et,
    desc(coalesce(evidence_score, -Inf)),
    desc(coalesce(specificity_score, -Inf)),
    desc(coalesce(sentiment_score, -Inf)),
    desc(coalesce(buy_zone, FALSE)),
    desc(coalesce(conviction, -Inf))
  ) %>%
  distinct(conversation_id, ticker, created_et, .keep_all = TRUE)

# -------------------------- Prepare for upsert ------------------

signals_db <- signals_from_tweets %>%
  mutate(
    tweet_key = paste0(conversation_id, "_", ticker, "_", format(created_et, "%Y%m%d%H%M%S")),
    direction = as.character(direction),
    horizon   = as.character(horizon),
    username  = as.character(username),
    text      = as.character(text)
  ) %>%
  transmute(
    tweet_key,
    username,
    conversation_id,
    created_et,
    text,
    ticker,
    conviction,
    direction,
    horizon,
    stop_loss_pct,
    buy_zone,
    sentiment_score,
    rationale,
    specificity_score,
    has_quant_data,
    evidence_score,
    evidence_types,
    evidence_snippet
  )

# -------------------------- Upsert ------------------------------

tmp_name <- "tmp_tweet_signals"
dbWriteTable(con, tmp_name, signals_db, temporary = TRUE, overwrite = TRUE)

invisible(dbExecute(con, sprintf("
  INSERT INTO tweet_signals AS t (
    tweet_key, username, conversation_id, created_et, text, ticker,
    conviction, direction, horizon, stop_loss_pct, buy_zone,
    sentiment_score, rationale, specificity_score, has_quant_data,
    evidence_score, evidence_types, evidence_snippet
  )
  SELECT
    tweet_key, username, conversation_id, created_et, text, ticker,
    conviction, direction, horizon, stop_loss_pct, buy_zone,
    sentiment_score, rationale, specificity_score, has_quant_data,
    evidence_score, evidence_types, evidence_snippet
  FROM %s
  ON CONFLICT (tweet_key) DO UPDATE SET
    username          = EXCLUDED.username,
    conversation_id   = EXCLUDED.conversation_id,
    created_et        = EXCLUDED.created_et,
    text              = EXCLUDED.text,
    ticker            = EXCLUDED.ticker,
    conviction        = EXCLUDED.conviction,
    direction         = EXCLUDED.direction,
    horizon           = EXCLUDED.horizon,
    stop_loss_pct     = EXCLUDED.stop_loss_pct,
    buy_zone          = EXCLUDED.buy_zone,
    sentiment_score   = EXCLUDED.sentiment_score,
    rationale         = EXCLUDED.rationale,
    specificity_score = EXCLUDED.specificity_score,
    has_quant_data    = EXCLUDED.has_quant_data,
    evidence_score    = EXCLUDED.evidence_score,
    evidence_types    = EXCLUDED.evidence_types,
    evidence_snippet  = EXCLUDED.evidence_snippet;
", DBI::dbQuoteIdentifier(con, tmp_name))))

invisible(dbExecute(con, sprintf("DROP TABLE IF EXISTS %s;", DBI::dbQuoteIdentifier(con, tmp_name))))

message(sprintf("✅ Upserted %d signals from %d unseen threads.", nrow(signals_db), nrow(df)))

