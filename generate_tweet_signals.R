#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
  library(dplyr)
  library(purrr)
  library(tibble)
  library(stringr)
  library(lubridate)
  library(tidyr)
  library(glue)
  library(jsonlite)
  library(httr2)
})

`%||%` <- function(x, y) if (is.null(x)) y else x

# ── 0) ENV VARS ──────────────────────────────────────────────────────────────
OPENAI_API_KEY <- Sys.getenv("OPENAI_API_KEY")
if (!nzchar(OPENAI_API_KEY)) stop("Missing env var: OPENAI_API_KEY")

SUPABASE_PORT <- as.integer(Sys.getenv("SUPABASE_PORT", "5432"))
SUPABASE_DB   <- Sys.getenv("SUPABASE_DB", "postgres")
SUPABASE_USER <- Sys.getenv("SUPABASE_USER")
SUPABASE_PWD  <- Sys.getenv("SUPABASE_PWD")

# IMPORTANT: in your X-ingest script you used a pooler host
SUPABASE_POOLER_HOST <- Sys.getenv("SUPABASE_POOLER_HOST", "aws-0-us-east-2.pooler.supabase.com")

if (!nzchar(SUPABASE_USER)) stop("Missing env var: SUPABASE_USER")
if (!nzchar(SUPABASE_PWD))  stop("Missing env var: SUPABASE_PWD")

# Optional tuning
LOCAL_TZ <- Sys.getenv("LOCAL_TZ", "America/New_York")
LOOKBACK_MINUTES <- as.integer(Sys.getenv("SIGNALS_LOOKBACK_MINUTES", "120"))
OPENAI_MODEL <- Sys.getenv("OPENAI_MODEL", "gpt-4o-mini")  # cheaper default for automation
TEMPERATURE <- as.numeric(Sys.getenv("OPENAI_TEMPERATURE", "0"))
MAX_TOKENS <- as.integer(Sys.getenv("OPENAI_MAX_TOKENS", "1700"))

message("✅ Using TZ = ", LOCAL_TZ, " | lookback = ", LOOKBACK_MINUTES, " minutes")
message("✅ OpenAI model = ", OPENAI_MODEL)

# ── 1) CONNECT SUPABASE ──────────────────────────────────────────────────────
con <- dbConnect(
  RPostgres::Postgres(),
  host     = SUPABASE_POOLER_HOST,
  port     = SUPABASE_PORT,
  dbname   = SUPABASE_DB,
  user     = SUPABASE_USER,
  password = SUPABASE_PWD,
  sslmode  = "require"
)
on.exit(dbDisconnect(con), add = TRUE)

print(dbGetQuery(con, "select '✅ connected' as status, now();"))

# ── 2) ENSURE SIGNALS TABLE EXISTS ───────────────────────────────────────────
dbExecute(con, "
  CREATE TABLE IF NOT EXISTS public.twitter_trade_signals (
    signal_id         text PRIMARY KEY,
    username          text,
    conversation_id   text,
    created_at        timestamptz,
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
")

# ── 3) PULL ONLY NEW THREADS ────────────────────────────────────────────────
# Use max(created_at) in twitter_trade_signals as watermark, minus lookback buffer
last_ts <- dbGetQuery(con, "
  SELECT COALESCE(MAX(created_at), '1970-01-01'::timestamptz) AS last_ts
  FROM public.twitter_trade_signals;
")$last_ts[[1]]

cutoff <- as.POSIXct(last_ts, tz = "UTC") - minutes(LOOKBACK_MINUTES)
message("✅ Watermark UTC: ", as.character(last_ts))
message("✅ Cutoff   UTC: ", as.character(cutoff))

# Read from collapsed threads table
collapsed_tbl3 <- dbGetQuery(
  con,
  "SELECT conversation_id, username, created_at, text, n_tweets
   FROM public.twitter_collapse_full
   WHERE created_at > $1
   ORDER BY created_at DESC;",
  params = list(format(cutoff, "%Y-%m-%d %H:%M:%S%z"))
) %>%
  as_tibble()

if (nrow(collapsed_tbl3) == 0) {
  message("✅ No new threads since cutoff. Exiting.")
  quit(status = 0)
}

message("✅ Threads to process: ", nrow(collapsed_tbl3))

# ── 4) ADD ET TIME (AUTOMATE YOUR tbl_et STEP) ───────────────────────────────
tbl_et <- collapsed_tbl3 %>%
  mutate(
    created_utc = as.POSIXct(created_at, tz = "UTC"),
    created_et  = with_tz(created_utc, tzone = LOCAL_TZ)
  )

# ── 5) PROMPT + OPENAI CALL ─────────────────────────────────────────────────
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

make_tweet_prompt <- function(tweet_text) {
  safe_txt <- tweet_text |>
    stringr::str_replace_all("\\{", "{{") |>
    stringr::str_replace_all("\\}", "}}")

  glue::glue("
You are a trading assistant that converts a single tweet into a strict JSON trade-idea list.

TWEET
-----
{safe_txt}
-----

Return EXACTLY one JSON object with a single key \"signals\" mapping to an array of idea objects.
Each idea MUST include ALL fields:

ticker, conviction (0-1), direction (long|short), horizon (short|mid|long),
stop_loss_pct (8-25), buy_zone (true|false), sentiment_score (-1..1),
rationale (<= 60 words),
specificity_score (0-1), has_quant_data (bool),
evidence_score (0-1), evidence_types (comma tags),
evidence_snippet (<=30 words, or empty if none).

Drop weak ideas: if no ticker OR specificity_score < 0.30.
If no valid ideas remain, return {{\"signals\": []}}.

Return JSON only. No markdown. No extra keys.
")
}

ask_openai <- function(prompt,
                       model = OPENAI_MODEL,
                       temperature = TEMPERATURE,
                       max_tokens = MAX_TOKENS,
                       retries = 4) {

  system_prompt <- "Return exactly valid JSON. No markdown. No extra keys."

  req <- request("https://api.openai.com/v1/chat/completions") |>
    req_headers(
      Authorization = paste("Bearer", OPENAI_API_KEY),
      `Content-Type` = "application/json"
    ) |>
    req_body_json(list(
      model = model,
      temperature = temperature,
      max_tokens = max_tokens,
      response_format = list(type = "json_object"),
      messages = list(
        list(role = "system", content = system_prompt),
        list(role = "user", content = prompt)
      )
    ))

  for (k in seq_len(retries)) {
    resp <- tryCatch(req_perform(req, timeout = 120), error = identity)

    if (inherits(resp, "error")) {
      message("⚠️ OpenAI HTTP error: ", resp$message)
    } else {
      if (resp_status(resp) == 200) {
        js <- resp_body_json(resp, simplifyVector = FALSE)
        txt <- js$choices[[1]]$message$content %||% ""
        if (nzchar(txt)) return(str_trim(txt))
      } else {
        message("⚠️ OpenAI status ", resp_status(resp), ": ", resp_body_string(resp))
      }
    }

    Sys.sleep(2^k) # exponential backoff
  }

  NA_character_
}

safe_ask <- purrr::possibly(
  function(pr) ask_openai(pr),
  otherwise = NA_character_
)

tweets_signals <- tbl_et %>%
  transmute(
    conversation_id,
    username,
    created_at,      # UTC (db)
    created_et,
    text
  ) %>%
  mutate(
    prompt = map_chr(text, make_tweet_prompt),
    gpt_raw = map_chr(prompt, safe_ask)
  )

# ── 6) PARSE JSON TO TABLES ─────────────────────────────────────────────────
parse_one <- function(txt) {
  if (is.na(txt) || !nzchar(txt)) return(tibble())
  out <- tryCatch(
    jsonlite::fromJSON(txt)$signals %>% tibble::as_tibble(),
    error = function(e) tibble()
  )

  if (nrow(out) == 0) return(tibble())

  # Fill missing fields (schema safety)
  missing <- setdiff(names(expected_schema), names(out))
  if (length(missing)) {
    for (nm in missing) {
      out[[nm]] <- NA
    }
  }

  out %>%
    transmute(
      ticker            = as.character(ticker),
      conviction        = pmin(pmax(as.numeric(conviction), 0), 1),
      direction         = as.character(direction),
      horizon           = as.character(horizon),
      stop_loss_pct     = as.integer(pmin(pmax(as.integer(stop_loss_pct), 8), 25)),
      buy_zone          = as.logical(buy_zone),
      sentiment_score   = pmin(pmax(as.numeric(sentiment_score), -1), 1),
      rationale         = as.character(rationale),
      specificity_score = pmin(pmax(as.numeric(specificity_score), 0), 1),
      has_quant_data    = as.logical(has_quant_data),
      evidence_score    = pmin(pmax(as.numeric(evidence_score), 0), 1),
      evidence_types    = as.character(evidence_types),
      evidence_snippet  = as.character(evidence_snippet)
    )
}

signals_from_tweets <- tweets_signals %>%
  mutate(gpt_table = map(gpt_raw, parse_one)) %>%
  select(username, conversation_id, created_et, text, gpt_table) %>%
  tidyr::unnest(gpt_table) %>%
  filter(!is.na(ticker), nzchar(ticker))

# If nothing parsed, exit cleanly
if (nrow(signals_from_tweets) == 0) {
  message("✅ No valid signals parsed from new threads. Exiting.")
  quit(status = 0)
}

# ── 7) TICKER FIXES (YOUR MAPPING) ──────────────────────────────────────────
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
    ADYEN="ADYEN.AS", RPI="RPI.L", PUIG="PUIG.MC", ASMDEE_B="ASMDEE-B.ST"
  )
  if (sym %in% names(stock_map)) return(unname(stock_map[sym]))

  crypto_keep <- c("BTC","ETH","DOGE","SHIB","SOL","XRP","ADA","AVAX","LINK","DOT")
  if (sym %in% crypto_keep) return(paste0(sym, "-USD"))

  skip_syms <- c("TICKERPLUS","TIPRANKS","TRUMP","TRENDS","FIGUREAI","APPTRONIK","OPENSEA","MRBEAST")
  if (sym %in% skip_syms) return(NA_character_)

  sym
}

fix_ticker <- function(sym_vec) vapply(sym_vec, .fix_one, character(1))

signals_from_tweets <- signals_from_tweets %>%
  mutate(
    ticker = fix_ticker(ticker),
    ticker = if_else(
      is.na(ticker) & str_detect(text, "\\bFIGR\\b"),
      "FIGR",
      ticker
    )
  ) %>%
  filter(!is.na(ticker), nzchar(ticker))

if (nrow(signals_from_tweets) == 0) {
  message("✅ All signals dropped after ticker normalization. Exiting.")
  quit(status = 0)
}

# ── 8) BUILD signal_id + UPSERT ─────────────────────────────────────────────
signals_db <- signals_from_tweets %>%
  transmute(
    signal_id        = paste0(username, ":", conversation_id, ":", ticker, ":", format(created_et, "%Y%m%d%H%M%S")),
    username         = as.character(username),
    conversation_id  = as.character(conversation_id),

    # created_at stored as timestamptz; convert ET instant to UTC instant
    created_at       = with_tz(as.POSIXct(created_et, tz = LOCAL_TZ), "UTC"),

    text             = as.character(text),
    ticker           = as.character(ticker),
    conviction       = as.numeric(conviction),
    direction        = as.character(direction),
    horizon          = as.character(horizon),
    stop_loss_pct    = as.integer(stop_loss_pct),
    buy_zone         = as.logical(buy_zone),
    sentiment_score  = as.numeric(sentiment_score),
    rationale        = as.character(rationale),
    specificity_score= as.numeric(specificity_score),
    has_quant_data   = as.logical(has_quant_data),
    evidence_score   = as.numeric(evidence_score),
    evidence_types   = as.character(evidence_types),
    evidence_snippet = as.character(evidence_snippet)
  ) %>%
  distinct(signal_id, .keep_all = TRUE)

# temp table + upsert
dbWriteTable(con, "tmp_twitter_trade_signals", signals_db, temporary = TRUE, overwrite = TRUE)

dbExecute(con, "
  INSERT INTO public.twitter_trade_signals AS t
    (signal_id, username, conversation_id, created_at, text,
     ticker, conviction, direction, horizon, stop_loss_pct,
     buy_zone, sentiment_score, rationale, specificity_score,
     has_quant_data, evidence_score, evidence_types, evidence_snippet)
  SELECT
    signal_id, username, conversation_id, created_at::timestamptz, text,
    ticker, conviction, direction, horizon, stop_loss_pct,
    buy_zone, sentiment_score, rationale, specificity_score,
    has_quant_data, evidence_score, evidence_types, evidence_snippet
  FROM tmp_twitter_trade_signals
  ON CONFLICT (signal_id) DO UPDATE
  SET
    username          = EXCLUDED.username,
    conversation_id   = EXCLUDED.conversation_id,
    created_at        = EXCLUDED.created_at,
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
")

dbExecute(con, "DROP TABLE IF EXISTS tmp_twitter_trade_signals;")

message("✅ Done. Upserted ", nrow(signals_db), " signals at ", Sys.time())







