"""
AURIX LABS - Solana Token Scanner v10.3 PUMPFUN-ONLY CRASH GUARD
- Pump.fun tokens ONLY (no Raydium)
- No mint/freeze check (Pump.fun handles automatically)
- Crash Guard: tokens needed to crash price back to DETECTION price
- Entry: reject if largest holder or combined top-5 >= full crash threshold
- Live: exit if largest holder or combined top-5 >= HALF crash threshold
- No stop-loss - pure crash-guard isolation
- DEXScreener primary + Solana RPC logsSubscribe backup
- v10.3 additions: buy pressure monitor, peak drawdown exit, tighter entry filters,
  stale holder re-check, mayhem floor FIXED, detection-to-entry timeout

Install: pip install requests websocket-client base58 solders flask
"""

import websocket
import json
import requests
import threading
import time
import csv
import os
import struct
import base64
import math
import subprocess
from datetime import datetime
from solders.pubkey import Pubkey
from flask import Flask

app = Flask(__name__)

@app.route('/')
def health_check():
    return "Bot Online", 200

# ============ CONFIG ============
SOLANA_RPC_HTTP = os.environ.get("SOLANA_RPC_HTTP", "")
SOLANA_RPC_WSS  = os.environ.get("SOLANA_RPC_WSS", "")

rpc_failure_state = {"count": 0, "last_reason": ""}
rpc_failure_lock  = threading.Lock()

LOG_FILE = "validated_tokens.csv"  # v10.4.2: Write to project folder for local access

PUMPFUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
PUMPFUN_INSTRUCTION = "Instruction: Create"

INSIDER_THRESHOLD_PCT  = 10.0
COMBINED_INSIDER_CAP_PCT = 35.0
SYSTEM_VAULT_THRESHOLD = 40.0

ENTRY_USD        = 20.0
TARGET_MULT      = 1.2   # v10.4.1: First TP at 20%
FINAL_TARGET_MULT = 2.0  # v10.4.1: Final TP at 100%
SELL_75_PCT      = 0.75

# GRADUATION removed in v10.4.1 — only TP1 (20%) and TP2 (100%)

CURVE_OFFSET_VIRTUAL_TOKEN_RESERVES = 8
CURVE_OFFSET_VIRTUAL_SOL_RESERVES   = 16
CURVE_OFFSET_REAL_TOKEN_RESERVES    = 24
CURVE_OFFSET_REAL_SOL_RESERVES      = 32
CURVE_OFFSET_TOKEN_TOTAL_SUPPLY     = 40
CURVE_OFFSET_COMPLETE               = 48

PYTH_SOL_USD_FEED_ID = "0xef0d8b6fda2ceba41da15d4095d1da392a0d2f8ed0c6c7bc0f4cfac8c280b56d"

# ============ CRASH GUARD CONFIG ============
CRASH_THRESHOLD_PCT_MAX_REMOVED_NOTE = True  # was 50.0, removed -- see evaluate_crash_guard comment
CRASH_THRESHOLD_PCT_MIN = 5.0
HOLDER_MONITOR_INTERVAL = 2.0  # v10.4.1: faster crash guard checks
PUMP_GENESIS_VIRTUAL_TOKEN_RESERVES = 1_073_000_000_000_000  # fixed for every pump.fun token at true creation
# INSTANT_DUMP_MAX_PCT removed in v10.3.8 — sniper dominance is often the pump

# ============ v10.3 NEW CONFIG ============
# Was a fixed 20 SOL cap -- didn't account for tokens launching at different
# baseline mcaps. Now dynamic: reject if entry mcap has moved more than this
# % above the token's own detection (launch) mcap -- matches the actual
# strategy ("enter anywhere from launch price to 50% above it").
# v13.0: MAX_ENTRY_MOVE_PCT removed entirely (see full_token_pipeline comment)
# v12.1: MAX_DETECTION_TO_ENTRY_SEC removed entirely -- a validated token is
# a token we hold, so validation should never be rushed by a clock. Entry
# now waits on wait_for_pattern_confirmation() (see below), which only
# resolves when the pattern itself resolves (confirmed / rejected /
# opportunity gone), never on elapsed time.

# FIXED BUG: was 1_000_000_000_000_000 (exactly 1B) which incorrectly flagged
# EVERY standard pump.fun token (which has exactly 1B supply) as Mayhem Mode.
# Confirmed via production logs: 100% rejection rate, all showing
# "Mayhem Mode (1.0B supply)" -- that 1.0B IS the normal supply, not the
# doubled 2B Mayhem supply. Floor now sits comfortably between the two.
MAYHEM_SUPPLY_FLOOR = 1_500_000_000_000_000  # 1.5B -- between normal (1B) and Mayhem (2B)

BUY_PRESSURE_CHECK_INTERVAL = 1.0
STALL_THRESHOLD_SECONDS = 12  # v10.3.2: bumped from 5s to reduce false stalls on early tokens

# ============ v10.3.4 NET FLOW CONFIG ============
# Option A: Track net SOL flow over a window. If net flow is negative
# (more sells than buys), sellers are overwhelming buyers — exit.
# This is SEPARATE from activity stall (Option B): a token can have
# constant activity (no activity stall) but still be bleeding SOL.
# NET_FLOW removed in v10.4.0
# NET_FLOW removed in v10.4.0


# v10.4.0: Removed net flow, holder dump, drawdown, stall exits
# Only crash guard and TP1/TP2 remain

# ============ v10.5.0 MOMENTUM GATE ============
# Stream buy/sell counts from logsSubscribe in real-time.
# Only enter tokens where buyers are winning at launch.
MOMENTUM_WINDOW_SECONDS = 3.0     # Watch for 3s after detection
MIN_BUY_SELL_RATIO = 2.0          # Need 2:1 buys vs sells
MIN_BUY_COUNT = 3                 # Need at least 3 buys in window

# Global momentum tracker: mint -> {"buys": int, "sells": int, "start_time": float}
# v10.5.1: Concurrency cap for momentum streaming (Helius Business = 250 WSS connections)
MAX_CONCURRENT_MOMENTUM_STREAMS = 5
momentum_stream_count = {"count": 0}
momentum_stream_lock = threading.Lock()


MAX_DRAWDOWN_PCT = 0.15

telegram_failure_state = {"fails": 0}
telegram_failure_lock  = threading.Lock()

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")
TELEGRAM_API_BASE  = "https://api.telegram.org"

def telegram_post_message(content):
    """
    v14.0: Replaces Discord entirely for alerts. Plain text, no parse_mode --
    Telegram's Markdown modes reject messages with unescaped special
    characters (mint addresses, exception text, etc. can trip that), and
    reliability matters more here than bold text after the Discord
    rate-limit saga. "**" from the old Discord-flavored message templates
    is just stripped rather than converted.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("  [WARN] TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID not set -- skipping Telegram post.")
        return
    text = content.replace("**", "")
    try:
        r = requests.post(
            f"{TELEGRAM_API_BASE}/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "disable_web_page_preview": True},
            timeout=10
        )
        if r.status_code != 200:
            print(f"  [WARN] Telegram post returned {r.status_code}: {r.text[:200]}")
            with telegram_failure_lock:
                telegram_failure_state["fails"] += 1
        else:
            with telegram_failure_lock:
                telegram_failure_state["fails"] = 0
    except Exception as e:
        print(f"  [WARN] Telegram post failed: {e}")
        with telegram_failure_lock:
            telegram_failure_state["fails"] += 1

def log_entry(mint, entry_mcap_sol, entry_sol, crash_th, largest_pct, combined_pct):
    """
    v10.4.2: Log when a trade OPENS.
    This creates a row with outcome = 'OPENED' so you can track
    all trades from entry to exit in one file.
    NOTE (v14.0): Render's free-tier filesystem is ephemeral -- this CSV
    does not survive a redeploy/restart/spin-down. Confirmed acceptable:
    Telegram messages are the durable trade record on this hosting tier.
    """
    fieldnames = [
        "timestamp", "mint", "outcome",
        "entry_mcap_sol", "exit_mcap_sol",
        "entry_mcap_usd", "exit_mcap_usd",
        "total_profit_sol", "total_profit_usd",
        "duration_seconds", "ticks",
        "largest_holder_pct", "combined_top5_pct"
    ]
    file_exists = os.path.exists(LOG_FILE)
    with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow({
            "timestamp": datetime.now().isoformat(),
            "mint": mint,
            "outcome": "OPENED",
            "entry_mcap_sol": round(entry_mcap_sol, 6),
            "exit_mcap_sol": "",
            "entry_mcap_usd": round(sol_to_usd(entry_mcap_sol), 2),
            "exit_mcap_usd": "",
            "total_profit_sol": "",
            "total_profit_usd": "",
            "duration_seconds": "",
            "ticks": "",
            "largest_holder_pct": round(largest_pct, 4) if largest_pct else "",
            "combined_top5_pct": round(combined_pct, 4) if combined_pct else "",
        })

# v14.0: save_position_state / clear_position_state / attempt_position_recovery
# removed entirely -- confirmed not needed (no run has ever actually recovered
# a position), and Telegram can't be read back the way the Discord channel
# was being (ab)used as a tiny database anyway.

sol_price_state = {"usd": 0.0, "last_fetch": 0}
sol_price_lock  = threading.Lock()
SOL_PRICE_CACHE_SECONDS = 10

already_seen    = set()

# v14.2: is_processing (the old single global "busy" flag) removed entirely.
# Every detected token now gets its own concurrent watch/validate thread --
# no candidate is ever skipped just because another one was mid-validation.
# The only thing still serialized is actually ENTERING a trade: only one
# active_position can be "running" at a time, claimed atomically at the
# moment a pattern confirms (see full_token_pipeline). A second candidate
# confirming while a position is already open does NOT enter -- it's logged
# to Telegram as confirmed-but-slot-taken instead of silently discarded.

# v14.2: full breakdown of what happened to every detected token. skipped_busy
# is gone -- v14.2 watches every candidate concurrently, nothing is ever
# discarded just because another token was mid-validation. confirmed_slot_taken
# is new: a real missed opportunity (the pattern genuinely confirmed) but
# another position was already open, so it wasn't entered -- logged to
# Telegram individually rather than silently dropped.
scan_stats_lock = threading.Lock()
scan_stats = {
    "raw_detections":           0,  # every pump.fun detection seen, before any gate
    "entered":                  0,  # actually opened a position
    "confirmed_slot_taken":     0,  # pattern confirmed genuinely, but another position was already open
    "rejected_basic":           0,  # not-pump-suffix / curve unreadable / too-early / mayhem
    "rejected_creator_history": 0,  # creator has prior tokens, none ever graduated
    "rejected_gmgn_risk":       0,  # GMGN bundler/rat-trader/insider-hold/overhang flag
    "rejected_pattern":         0,  # pre-entry sell-concentration or bundled-buy rejection
    "rejected_crash_guard":     0,  # holder concentration risk check failed
    "abandoned":                0,  # pattern confirmation gave up (dead/near-graduation/no interest)
}

def bump_scan_stat(key):
    with scan_stats_lock:
        scan_stats[key] = scan_stats.get(key, 0) + 1

already_seen_lock = threading.Lock()

active_position = {
    "running":               False,
    "mint":                  None,
    "curve_address":         None,
    "entry_mcap_sol":        0.0,
    "entry_price_sol":       0.0,
    "launch_price_sol":      0.0,
    "entry_sol":             0.0,
    "tp1_sol":               0.0,
    "initial_real_token_reserves": 0,
    "entry_time":            0.0,
    "phase":                 "A",
    "tp1_profit_sol":        0.0,
    "freeroll_entry_sol":    0.0,
    "lowest_real_token_reserves": None,
    "exit_triggered":        False,
    "last_tick_time":        None,
    "max_tick_gap_seconds":  0.0,
    "tick_count":            0,
    "last_value_before_exit_check": None,
    "crash_threshold_pct":   0.0,
    "largest_holder_pct":    0.0,
    "combined_top5_pct":     0.0,
    "peak_value_sol":        0.0,
    "buy_pressure_last_real_sol": 0.0,
    "buy_pressure_last_time": 0.0,
    "entry_vtr":             0,
    "launch_vtr":                0,     # v11.0 #2: virtual_token_reserves at detection
    "launch_vsr":                0,     # v11.0 #2: virtual_sol_reserves at detection
    "velocity_last_real_sol":    None,  # v11.0 #3: last real_sol_reserves sample for velocity calc
    "velocity_last_time":        None,  # v11.0 #3: timestamp of that sample
    "flow_velocity_sol_per_sec": 0.0,   # v11.0 #3: current SOL/sec inflow
    "peak_velocity_sol_per_sec": 0.0,   # v11.0 #10: peak velocity seen this trade, for reversal detection
    "creator_wallet":            None,  # v11.0 #9: creator/dev wallet address for this mint
}
position_lock = threading.Lock()

curve_ws_ref  = {"ws": None}
curve_ws_lock = threading.Lock()

def rpc_call(method, params):
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    try:
        r = requests.post(SOLANA_RPC_HTTP, json=payload, timeout=10)
        data = r.json()
        if "error" in data:
            print(f"  [WARN] RPC error on {method}: {data['error']}")
            with rpc_failure_lock:
                rpc_failure_state["count"] += 1
                rpc_failure_state["last_reason"] = f"{method}: {data['error']}"
        return data.get("result")
    except Exception as e:
        print(f"  [WARN] RPC call failed on {method}: {e}")
        with rpc_failure_lock:
            rpc_failure_state["count"] += 1
            rpc_failure_state["last_reason"] = f"{method}: {e}"
        return None

def fetch_sol_price_onchain():
    try:
        r = requests.get(
            "https://hermes.pyth.network/v2/updates/price/latest",
            params={"ids[]": PYTH_SOL_USD_FEED_ID},
            timeout=10
        )
        d = r.json()
        p = d["parsed"][0]["price"]
        price = float(p["price"]) * (10 ** int(p["expo"]))
        if 10 < price < 10000:
            return round(price, 2)
        return None
    except:
        return None

def get_sol_price_fresh():
    price = fetch_sol_price_onchain()
    if price:
        with sol_price_lock:
            sol_price_state["usd"]        = price
            sol_price_state["last_fetch"] = time.time()
        print(f"\n  [PRICE] SOL Price (fresh): ${price:.2f}")
        return price
    with sol_price_lock:
        return sol_price_state["usd"]

def get_sol_price():
    with sol_price_lock:
        if (time.time() - sol_price_state["last_fetch"] <= SOL_PRICE_CACHE_SECONDS
                and sol_price_state["usd"] > 0):
            return sol_price_state["usd"]
    return get_sol_price_fresh()

def sol_to_usd(sol_amount):
    price = get_sol_price()
    return sol_amount * price if price else 0

def usd_to_sol(usd_amount):
    price = get_sol_price_fresh()
    return usd_amount / price if price else 0

def derive_bonding_curve(mint_address):
    try:
        mint_pubkey = Pubkey.from_string(mint_address)
        program_id  = Pubkey.from_string(PUMPFUN_PROGRAM)
        pda, _bump = Pubkey.find_program_address(
            [b"bonding-curve", bytes(mint_pubkey)],
            program_id
        )
        return str(pda)
    except:
        return None

def get_curve_state(curve_address):
    result = rpc_call("getAccountInfo", [curve_address, {"encoding": "base64", "commitment": "confirmed"}])
    if not result or not result.get("value"):
        return None
    try:
        data_b64 = result["value"]["data"][0]
        return decode_curve_state_from_base64(data_b64)
    except:
        return None

def decode_curve_state_from_base64(data_b64):
    try:
        data = base64.b64decode(data_b64)
        virtual_token_reserves = struct.unpack_from("<Q", data, CURVE_OFFSET_VIRTUAL_TOKEN_RESERVES)[0]
        virtual_sol_reserves   = struct.unpack_from("<Q", data, CURVE_OFFSET_VIRTUAL_SOL_RESERVES)[0]
        real_token_reserves    = struct.unpack_from("<Q", data, CURVE_OFFSET_REAL_TOKEN_RESERVES)[0]
        real_sol_reserves      = struct.unpack_from("<Q", data, CURVE_OFFSET_REAL_SOL_RESERVES)[0]
        token_total_supply     = struct.unpack_from("<Q", data, CURVE_OFFSET_TOKEN_TOTAL_SUPPLY)[0]
        complete               = struct.unpack_from("<B", data, CURVE_OFFSET_COMPLETE)[0] != 0
        return {
            "virtual_token_reserves": virtual_token_reserves,
            "virtual_sol_reserves":   virtual_sol_reserves,
            "real_token_reserves":    real_token_reserves,
            "real_sol_reserves":      real_sol_reserves,
            "token_total_supply":     token_total_supply,
            "complete":               complete,
        }
    except:
        return None

def calc_price_from_state(curve_state):
    try:
        if not curve_state:
            return None, None
        vtr = curve_state["virtual_token_reserves"]
        vsr = curve_state["virtual_sol_reserves"]
        supply = curve_state["token_total_supply"]
        if vtr <= 0 or vsr <= 0:
            return None, None
        price_sol = (vsr / 1e9) / (vtr / 1e6)
        mcap_sol  = price_sol * (supply / 1e6)
        return price_sol, mcap_sol
    except:
        return None, None

# ============ CRASH GUARD MATH ============

def calc_crash_threshold(curve_state, detection_price_sol):
    """
    v10.5.4: Dynamic crash threshold — recalculated from CURRENT state.
    What % of supply needs to be sold to crash from CURRENT price back to DETECTION price?
    """
    try:
        vtr_current = curve_state["virtual_token_reserves"]
        vsr_current = curve_state["virtual_sol_reserves"]
        supply = curve_state["token_total_supply"]

        if vtr_current <= 0 or vsr_current <= 0 or supply <= 0:
            return None

        current_price_sol, _ = calc_price_from_state(curve_state)
        if current_price_sol is None:
            return None

        k = vtr_current * vsr_current
        P_raw_current = vsr_current / vtr_current

        # If current price hasn't moved above detection, use floor
        if current_price_sol <= detection_price_sol:
            return CRASH_THRESHOLD_PCT_MIN

        # Calculate reserves at detection price
        P_raw_detection = P_raw_current * (detection_price_sol / current_price_sol)
        vtr_detection = math.sqrt(k / P_raw_detection)
        tokens_to_sell = vtr_detection - vtr_current

        if tokens_to_sell <= 0:
            return CRASH_THRESHOLD_PCT_MIN

        calculated = (tokens_to_sell / supply) * 100
        return max(CRASH_THRESHOLD_PCT_MIN, calculated)

    except Exception as e:
        print(f"  [WARN] Crash threshold calc error: {e}")
        return None

def calc_curve_holding_pct(curve_state):
    """
    Calculate what % of supply is still held in the bonding curve.

    HIGH curve holding = TIGHT FLOAT = good for pumps
    (fewer tokens in circulation = buy pressure moves price faster)

    LOW curve holding = LOOSE FLOAT = risky
    (many tokens distributed = easy to dump)
    """
    try:
        vtr = curve_state["virtual_token_reserves"]
        supply = curve_state["token_total_supply"]
        if supply <= 0:
            return 0.0
        # Tokens still in curve = unsold supply
        tokens_in_curve = vtr
        return (tokens_in_curve / supply) * 100
    except:
        return 0.0

def calc_crash_threshold_tokens(curve_state, detection_price_sol):
    """
    v10.5.4: Dynamic crash threshold in raw tokens.
    """
    try:
        vtr_current = curve_state["virtual_token_reserves"]
        vsr_current = curve_state["virtual_sol_reserves"]
        supply = curve_state["token_total_supply"]

        if vtr_current <= 0 or vsr_current <= 0 or supply <= 0:
            return None

        current_price_sol, _ = calc_price_from_state(curve_state)
        if current_price_sol is None:
            return None

        if current_price_sol <= detection_price_sol:
            return int(supply * 0.05)

        k = vtr_current * vsr_current
        P_raw_current = vsr_current / vtr_current
        P_raw_detection = P_raw_current * (detection_price_sol / current_price_sol)
        vtr_detection = math.sqrt(k / P_raw_detection)
        tokens_to_sell = vtr_detection - vtr_current

        if tokens_to_sell <= 0:
            return int(supply * 0.05)

        return max(int(supply * 0.05), int(tokens_to_sell))

    except Exception as e:
        print(f"  [WARN] Crash threshold tokens calc error: {e}")
        return None

def calc_pct_sold_since_genesis(curve_state):
    """
    % of supply already sold, measured against the universal fixed starting
    point every pump.fun token begins from -- not relative to our entry.
    Catches tokens that were front-run/sniped before we even saw them.
    """
    vtr = curve_state["virtual_token_reserves"]
    sold = PUMP_GENESIS_VIRTUAL_TOKEN_RESERVES - vtr
    if sold <= 0:
        return 0.0
    return (sold / PUMP_GENESIS_VIRTUAL_TOKEN_RESERVES) * 100

holder_debug_last_sent = {"time": 0}
holder_debug_lock = threading.Lock()

def get_holder_snapshot(mint, max_retries=5):
    """
    Fetch the top token holders and their ownership percentages.

    v10.3.9: Also excludes the bonding curve PDA itself, which holds
    unsold tokens. The curve is NOT a dump risk — it's just supply
    that hasn't been bought yet. This lets us enter tokens where the
    curve holds 30-40% (tight float = fast pumps).

    Retry with exponential backoff when Helius hasn't indexed the mint yet.
    Fresh pump.fun tokens often fail the first few getTokenLargestAccounts
    calls because the RPC node hasn't indexed the new mint account.
    """
    # Derive bonding curve address to exclude it from holder counts
    curve_address = derive_bonding_curve(mint)

    result = rpc_call("getTokenLargestAccounts", [mint, {"commitment": "confirmed"}])
    if not result:
        for attempt in range(max_retries):
            wait = 0.5 * (2 ** attempt)
            time.sleep(wait)
            result = rpc_call("getTokenLargestAccounts", [mint, {"commitment": "confirmed"}])
            if result:
                print(f"  [HOLDER-RETRY] Success after {attempt + 2} attempts ({wait:.1f}s wait)")
                break

        if not result:
            with holder_debug_lock:
                if time.time() - holder_debug_last_sent["time"] >= 60:
                    holder_debug_last_sent["time"] = time.time()
                    with rpc_failure_lock:
                        last_reason = rpc_failure_state.get("last_reason", "unknown")
                    # v11.0: debug webhook removed -- rejections/debug no longer posted to Discord
            return None, None, 0, []
    try:
        accounts = result.get("value", [])
        if not accounts:
            return 0.0, 0.0, 0, []

        # v10.3.9: Calculate total EXCLUDING bonding curve (unsold supply)
        # The curve holds tokens that haven't been distributed yet
        curve_amount = 0
        for a in accounts:
            if a.get("address") == curve_address:
                curve_amount = int(a.get("amount", 0))
                break

        # Total circulating = total - curve holdings
        raw_total = sum(int(a.get("amount", 0)) for a in accounts)
        total = raw_total - curve_amount
        if total <= 0:
            total = raw_total  # Fallback if curve not found

        if total == 0:
            return 0.0, 0.0, 0, []

        top_n = min(10, len(accounts))
        owners = [None] * top_n

        def fetch_owner(idx, address):
            payload = {"jsonrpc": "2.0", "id": 1, "method": "getAccountInfo",
                       "params": [address, {"encoding": "jsonParsed", "commitment": "confirmed"}]}
            try:
                r = requests.post(SOLANA_RPC_HTTP, json=payload, timeout=4)
                acc_info = r.json().get("result")
            except:
                acc_info = None
            if acc_info and acc_info.get("value"):
                owners[idx] = acc_info["value"].get("owner", "")
            else:
                owners[idx] = ""

        threads = []
        for idx in range(top_n):
            address = accounts[idx].get("address", "")
            t = threading.Thread(target=fetch_owner, args=(idx, address), daemon=True)
            t.start()
            threads.append(t)
        for t in threads:
            t.join(timeout=8)

        holder_details = []
        largest_non_system_pct = 0.0
        combined_non_system_pct = 0.0
        system_total = 0.0

        for idx in range(top_n):
            amount = int(accounts[idx].get("amount", 0))
            address = accounts[idx].get("address", "")

            # v10.3.9: Skip the bonding curve itself — it's not a holder
            if address == curve_address:
                continue

            pct = (amount / total) * 100
            owner = owners[idx] or ""

            is_system = (
                owner == PUMPFUN_PROGRAM or
                owner == "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8" or
                pct >= SYSTEM_VAULT_THRESHOLD
            )

            holder_details.append((address[:12], round(pct, 2), is_system))

            if is_system:
                system_total += pct
            else:
                combined_non_system_pct += pct
                if pct > largest_non_system_pct:
                    largest_non_system_pct = pct

        return largest_non_system_pct, combined_non_system_pct, len(accounts), holder_details
    except Exception as e:
        print(f"  [WARN] Holder snapshot error: {e}")
        return None, None, 0, []

# ============ HOLDER HISTORY FOR DUMP DETECTION ============
holder_history = {}  # mint -> [{"time": t, "largest_pct": x, "combined_pct": y}, ...]
holder_history_lock = threading.Lock()
# HOLDER_HISTORY removed in v10.4.0
# SELL_ACCELERATION removed in v10.4.0

def evaluate_crash_guard(mint, curve_state, detection_price_sol, is_live_monitor=False):
    """
    v10.5.5: Fully dynamic crash guard.
    Threshold recalculated from CURRENT state back to detection price.
    Live monitor uses HALF of current threshold (dynamic, not frozen).

    ENTRY:  reject if largest holder >= FULL current crash threshold
    LIVE:   exit if largest holder >= HALF current crash threshold

    Returns:
        (passes, crash_threshold_pct, largest_pct, combined_pct, reason)
    """
    # v10.5.5: Calculate threshold from current state back to detection price
    crash_threshold_pct = calc_crash_threshold(curve_state, detection_price_sol)
    if crash_threshold_pct is None:
        return False, None, None, None, "Could not calculate crash threshold"

    largest_pct, combined_pct, holder_count, details = get_holder_snapshot(mint)
    if largest_pct is None:
        return False, crash_threshold_pct, None, None, "RPC failure: could not read holders (token too new or RPC lag)"

    # === CURVE FLOAT CHECK (v10.3.9) ===
    curve_holding_pct = calc_curve_holding_pct(curve_state)
    tight_float = curve_holding_pct >= 50.0

    if tight_float and not is_live_monitor:
        print(f"  [FLOAT] Tight float detected: {curve_holding_pct:.1f}% in curve — fast pump potential")

    # === THRESHOLD CHECKS ===
    if is_live_monitor:
        # LIVE: exit at HALF of current threshold (dynamic)
        trigger_pct = crash_threshold_pct / 2
        trigger_label = "HALF current threshold"
    else:
        # ENTRY: reject at FULL current threshold
        trigger_pct = crash_threshold_pct
        trigger_label = "FULL crash threshold"

    if largest_pct >= trigger_pct:
        return False, crash_threshold_pct, largest_pct, combined_pct,                f"Largest holder {largest_pct:.2f}% >= {trigger_label} {trigger_pct:.2f}%"

    if combined_pct >= trigger_pct:
        return False, crash_threshold_pct, largest_pct, combined_pct,                f"Combined top-5 {combined_pct:.2f}% >= {trigger_label} {trigger_pct:.2f}%"

    return True, crash_threshold_pct, largest_pct, combined_pct, "PASS"

# ============ LOGGING & POSITION MANAGEMENT ============

def log_result(data):
    """
    v10.4.2: Clean CSV logging for easy local analysis.
    Each trade has TWO rows: OPENED (entry) and CLOSED (outcome).
    Open validated_tokens.csv to see all trades.
    """
    fieldnames = [
        "timestamp", "mint", "outcome",
        "entry_mcap_sol", "exit_mcap_sol",
        "entry_mcap_usd", "exit_mcap_usd",
        "total_profit_sol", "total_profit_usd",
        "duration_seconds", "ticks",
        "largest_holder_pct", "combined_top5_pct"
    ]
    file_exists = os.path.exists(LOG_FILE)
    with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow({k: data.get(k, "") for k in fieldnames})

def release_position():
    closed_mint = None
    with position_lock:
        closed_mint = active_position.get("mint")
        active_position["running"]        = False
        active_position["exit_triggered"] = False
        active_position["mint"]           = None
    with already_seen_lock:
        if closed_mint:
            already_seen.discard(closed_mint)
    if closed_mint:
        clear_trade_flow(closed_mint)  # v12.0: drop buyer/seller tracking for the closed trade
    print(f"\n  [OK] Position closed. Scanner resuming...\n")
def exit_position(outcome, current_mcap_sol):
    with position_lock:
        if active_position["exit_triggered"]:
            return
        active_position["exit_triggered"] = True

    pos             = active_position
    entry_mcap_sol  = pos["entry_mcap_sol"]
    entry_sol       = pos["entry_sol"]
    entry_time      = pos["entry_time"]
    phase           = pos["phase"]
    tp1_profit_sol  = pos["tp1_profit_sol"]
    tick_count      = pos["tick_count"]
    max_gap         = pos["max_tick_gap_seconds"]
    crash_th        = pos.get("crash_threshold_pct", 0)
    largest_h       = pos.get("largest_holder_pct", 0)
    combined_h      = pos.get("combined_top5_pct", 0)
    duration        = time.time() - entry_time

    mcap_mult     = (current_mcap_sol / entry_mcap_sol) if entry_mcap_sol > 0 else 1
    current_value = entry_sol * mcap_mult

    # v10.4.1: Simplified PnL calculation
    total_profit = current_value - entry_sol
    tp1_final = tp1_profit_sol if phase == "B" else 0.0

    sol_price = get_sol_price_fresh()

    emoji = {
        "TP1_HIT":                 "[TP1-20%]",
        "TP2_HIT":                 "[TP2-100%]",
        "TIMEOUT":                 "[TIMEOUT]",
        "CRASH_GUARD_EXIT":        "[CRASH-GUARD]",
        "GMGN_RISK_EXIT":          "[GMGN-RISK]",
    }.get(outcome, "[EXIT]")

    print(f"\n  {emoji} {outcome}")
    print(f"  Exit MCap: {current_mcap_sol:.4f} SOL (${sol_to_usd(current_mcap_sol):,.0f})")
    print(f"  Total PnL: {total_profit:+.4f} SOL (${sol_to_usd(total_profit):+.2f})")
    if phase == "B":
        print(f"  TP1: +{tp1_final:.4f} SOL | Freeroll: +{tp1_final:.4f} SOL")
    if outcome in ("CRASH_GUARD_EXIT", "STALL_EXIT", "DRAWDOWN_EXIT"):
        print(f"  Crash Threshold: {crash_th:.2f}% | Largest: {largest_h:.2f}% | Combined: {combined_h:.2f}%")

    log_result({
        "timestamp":        datetime.now().isoformat(),
        "mint":             pos["mint"],
        "outcome":          outcome,
        "entry_mcap_sol":   round(entry_mcap_sol, 6),
        "exit_mcap_sol":    round(current_mcap_sol, 6),
        "entry_mcap_usd":   round(sol_to_usd(entry_mcap_sol), 2),
        "exit_mcap_usd":    round(sol_to_usd(current_mcap_sol), 2),
        "total_profit_sol": round(total_profit, 6),
        "total_profit_usd": round(sol_to_usd(total_profit), 2),
        "duration_seconds": round(duration, 2),
        "ticks":            pos.get("tick_count", 0),
        "largest_holder_pct": round(largest_h, 4),
        "combined_top5_pct": round(combined_h, 4),
    })

    with curve_ws_lock:
        if curve_ws_ref["ws"]:
            try:
                curve_ws_ref["ws"].close()
            except:
                pass
            curve_ws_ref["ws"] = None

    guard_info = ""
    if outcome == "CRASH_GUARD_EXIT":
        guard_info = (f"\nGuard: threshold {crash_th:.2f}% | "
                      f"largest {largest_h:.2f}% | combined {combined_h:.2f}%")
    elif outcome == "STALL_EXIT":
        guard_info = "\nNo buy pressure for 5+ seconds"
    elif outcome == "DRAWDOWN_EXIT":
        guard_info = f"\nPeak drawdown: {MAX_DRAWDOWN_PCT*100:.0f}% triggered"

    # v14.0: trade RESULTS still post (this is what you asked to keep) --
    # only entry-time REJECTIONS were cut out. Every OPENED / TP1 / TP2 / exit
    # message still fires so the channel reads as a clean trade log.
    telegram_post_message(
        f"**{outcome}** | {pos['mint']}\n"
        f"Entry MCap: {entry_mcap_sol:.3f} SOL (${sol_to_usd(entry_mcap_sol):,.0f}) -> "
        f"Exit MCap: {current_mcap_sol:.4f} SOL (${sol_to_usd(current_mcap_sol):,.0f})\n"
        f"PnL: {total_profit:+.4f} SOL (${sol_to_usd(total_profit):+.2f})\n"
        f"Duration: {duration:.0f}s | Ticks: {tick_count} | Max gap: {max_gap:.1f}s"
        f"{guard_info}"
    )
    release_position()


# ============ v13.0 TRADE FLOW ENGINE ============
# Everything here reads real, wallet-level, slot-aware trade data decoded
# from pump.fun's on-chain TradeEvent log (a base64 "Program data:" line
# emitted alongside every trade) -- never just instruction-name counting.
#
# NOTE: Anchor event field order/offsets can drift between pump.fun program
# upgrades. The offsets below match the long-standing public pump.fun IDL,
# but verify against a few live captures (print the decoded dict for your
# first several trades) before trusting this in anything but simulation.

TRADE_EVENT_DISCRIMINATOR_LEN = 8  # Anchor event discriminator prefix

def decode_trade_event(log_line):
    """
    Decode a pump.fun "Program data: <base64>" log line into a trade dict:
    {sol_amount, token_amount, is_buy, user, timestamp}. Slot is NOT in this
    event -- it comes from the outer logsNotification context and is added
    by the caller. Returns None if the line isn't a decodable trade event.
    """
    try:
        if "Program data:" not in log_line:
            return None
        b64 = log_line.split("Program data:", 1)[1].strip()
        raw = base64.b64decode(b64)
        if len(raw) < TRADE_EVENT_DISCRIMINATOR_LEN + 32 + 8 + 8 + 1 + 32 + 8:
            return None
        off = TRADE_EVENT_DISCRIMINATOR_LEN
        off += 32  # mint pubkey (skip, we already know the mint from the subscription)
        sol_amount = struct.unpack_from("<Q", raw, off)[0]; off += 8
        token_amount = struct.unpack_from("<Q", raw, off)[0]; off += 8
        is_buy = struct.unpack_from("<B", raw, off)[0] != 0; off += 1
        user_bytes = raw[off:off+32]; off += 32
        timestamp = struct.unpack_from("<q", raw, off)[0]; off += 8

        return {
            "sol_amount": sol_amount / 1e9,
            "token_amount": token_amount,
            "is_buy": is_buy,
            "user": base58_encode(user_bytes),
            "timestamp": timestamp,
        }
    except Exception:
        return None

def base58_encode(raw_bytes):
    try:
        return str(Pubkey.from_bytes(raw_bytes))
    except Exception:
        return raw_bytes.hex()

# ============ v13.0 CONFIG: TRADE FLOW THRESHOLDS (pre-entry only now) ============
MIN_UNIQUE_BUYERS_HEALTHY = 3        # fewer than this and it's still "one burst", not a broadening base
SELL_CONCENTRATION_EXIT_PCT = 55.0   # one wallet (or one slot-bundle) responsible for >= this % of sell volume = a dump
SELL_CONCENTRATION_MIN_SOL = 0.5     # ignore sell-concentration checks until there's at least this much real sell volume (avoid dust noise)

BUNDLE_CHECK_WINDOW = 5              # look at the first N buys for orchestration
BUNDLE_MIN_SAME_SLOT_FOR_REJECT = 3  # this many of those N sharing one slot = bundled/orchestrated, not organic

trade_flow_lock = threading.Lock()
trade_flow_state = {}  # mint -> {"buyers": set, "buy_events": [(t,user,sol,slot)], "sell_events": [...], "new_buyer_times": [...], "last_new_buyer_time": t}

def reset_trade_flow(mint):
    with trade_flow_lock:
        trade_flow_state[mint] = {
            "buyers": set(),
            "buy_events": [],       # (time, user, sol_amount, slot)
            "sell_events": [],      # (time, user, sol_amount, slot)
            "new_buyer_times": [],  # timestamps at which a NEW distinct buyer appeared
            "last_new_buyer_time": time.time(),
            "last_activity_time": time.time(),  # v14.1: last ANY trade (buy or sell), fixes the stall bug below
        }

def clear_trade_flow(mint):
    with trade_flow_lock:
        trade_flow_state.pop(mint, None)

def record_trade_event(mint, trade, slot):
    with trade_flow_lock:
        state = trade_flow_state.get(mint)
        if state is None:
            return
        user = trade["user"]
        now_t = time.time()
        state["last_activity_time"] = now_t  # v14.1: any trade at all resets the stale clock
        if trade["is_buy"]:
            if user not in state["buyers"]:
                state["buyers"].add(user)
                state["last_new_buyer_time"] = now_t
                state["new_buyer_times"].append(now_t)
                if len(state["new_buyer_times"]) > 300:
                    state["new_buyer_times"] = state["new_buyer_times"][-300:]
            state["buy_events"].append((now_t, user, trade["sol_amount"], slot))
            if len(state["buy_events"]) > 300:
                state["buy_events"] = state["buy_events"][-300:]
        else:
            state["sell_events"].append((now_t, user, trade["sol_amount"], slot))
            if len(state["sell_events"]) > 300:
                state["sell_events"] = state["sell_events"][-300:]

# --- Pre-entry-only checks (still used by evaluate_entry_pattern below) ---

def check_sell_signal(mint):
    """
    Sell concentration, WITH same-slot bundling folded in: sells from
    DIFFERENT wallets landing in the SAME slot are grouped together, since
    that's the structural tell of one operator splitting a dump across
    wallets to look diversified. Returns (healthy, reason).
    """
    with trade_flow_lock:
        state = trade_flow_state.get(mint)
        if state is None:
            return True, "No trade flow data yet"
        sell_events = list(state["sell_events"])

    if not sell_events:
        return True, "No sells yet"

    totals_by_user = {}
    totals_by_slot = {}
    total_sell_sol = 0.0
    for _, user, sol_amount, slot in sell_events:
        totals_by_user[user] = totals_by_user.get(user, 0.0) + sol_amount
        totals_by_slot[slot] = totals_by_slot.get(slot, 0.0) + sol_amount
        total_sell_sol += sol_amount

    if total_sell_sol < SELL_CONCENTRATION_MIN_SOL:
        return True, "Sell volume too small to judge yet"

    max_user_sol = max(totals_by_user.values())
    max_slot_sol = max(totals_by_slot.values())
    max_group_sol = max(max_user_sol, max_slot_sol)
    concentration_pct = (max_group_sol / total_sell_sol) * 100

    if concentration_pct >= SELL_CONCENTRATION_EXIT_PCT:
        via = "one wallet" if max_user_sol >= max_slot_sol else "one slot (bundled across wallets)"
        return False, (f"Sell concentration {concentration_pct:.0f}% from {via} "
                        f"({max_group_sol:.3f}/{total_sell_sol:.3f} SOL) -- looks like a dump")

    return True, "Sell flow not concentrated"

def check_bundled_buys(mint):
    """Pre-entry only: were the first several buys orchestrated in one slot?"""
    with trade_flow_lock:
        state = trade_flow_state.get(mint)
        if state is None:
            return True, "No trade flow data yet"
        buy_events = list(state["buy_events"])[:BUNDLE_CHECK_WINDOW]

    if len(buy_events) < BUNDLE_CHECK_WINDOW:
        return True, "Not enough buys yet to judge bundling"

    slot_counts = {}
    for _, _, _, slot in buy_events:
        slot_counts[slot] = slot_counts.get(slot, 0) + 1
    max_same_slot = max(slot_counts.values())

    if max_same_slot >= BUNDLE_MIN_SAME_SLOT_FOR_REJECT:
        return False, (f"{max_same_slot} of the first {BUNDLE_CHECK_WINDOW} buys landed in the same slot "
                        f"-- looks like a bundled/orchestrated buy, not organic distinct buyers")
    return True, "No buy bundling detected"

# --- Composite evaluators used by the pipeline ---

PRICE_FLATNESS_EPSILON = 1e-15  # NOT a trading threshold -- purely floating-point comparison
                                 # safety, since price_sol is a ratio of two divided integers and
                                 # can carry rounding noise far below any real price movement.

def evaluate_entry_pattern(mint, current_price_sol=None, detection_price_sol=None):
    """
    Pre-entry gate. Returns ('confirmed'|'rejected'|'pending', reason).
    Checks (in order): sell signal (concentration + same-slot bundling),
    buy bundling, buyer-broadening, then a continuous flatness gate.
    """
    healthy, reason = check_sell_signal(mint)
    if not healthy:
        return "rejected", reason

    bundle_ok, bundle_reason = check_bundled_buys(mint)
    if not bundle_ok:
        return "rejected", bundle_reason

    with trade_flow_lock:
        state = trade_flow_state.get(mint)
        unique_buyers = len(state["buyers"]) if state else 0

    if unique_buyers < MIN_UNIQUE_BUYERS_HEALTHY:
        return "pending", f"{unique_buyers}/{MIN_UNIQUE_BUYERS_HEALTHY} distinct buyers so far, still observing"

    # v14.3: FLATNESS GATE -- checked every tick, no fixed %. Enough distinct
    # buyers can be satisfied by a handful of tiny buys that don't actually
    # move price at all (confirmed cause of a bad entry: E3sLku8... confirmed
    # on buyer count alone while sitting almost exactly at genesis mcap).
    # This blocks CONFIRMATION -- not a reject/abandon -- so a token that's
    # currently flat just keeps waiting; it can still confirm later the
    # moment price genuinely moves above where we detected it.
    if (current_price_sol is None or detection_price_sol is None
            or current_price_sol <= detection_price_sol + PRICE_FLATNESS_EPSILON):
        return "pending", (f"{unique_buyers} distinct buyers, but price is still flat/at-or-below "
                            f"detection ({current_price_sol} vs {detection_price_sol}) -- waiting for real upward movement")

    return "confirmed", (f"{unique_buyers} distinct buyers, price genuinely above detection, "
                          f"no bundling or concentrated selling -- genuine broadening pattern")

# v14.5: evaluate_trend_health and its four reactive live checks (buyer
# broadening / wave rhythm / concentration trend / quiet accumulation) are
# REMOVED entirely. They were all reading trade flow AFTER something
# happened -- confirmed reactive, and confirmed to fire on stale/residual
# activity rather than real new weakness (see the 2-second, 1-tick exit on
# 5hsHcHvJU9... -- there was no time for anything new to have happened).
# Replaced by gmgn_live_risk_monitor() below: a genuinely proactive signal
# watching incentive-to-dump build up (GMGN-tagged risk wallets, unrealized
# profit overhang) rather than reacting to a dump already in progress.

pattern_ws_registry = {}   # mint -> WebSocketApp, one persistent stream per watched/held token
pattern_ws_lock = threading.Lock()

def start_trade_flow_stream(mint, curve_address):
    """
    Persistent logsSubscribe to the bonding curve PDA, decoding real
    pump.fun TradeEvents (wallet-level, slot-aware). Runs from the moment
    we start WATCHING a token (before any entry decision) straight through
    to exit. Not tied to active_position, so it works during the pre-entry
    watch phase where there's no position yet.
    """
    def on_msg(ws, message):
        try:
            data = json.loads(message)
            if data.get("method") != "logsNotification":
                return
            result = data.get("params", {}).get("result", {})
            slot = result.get("context", {}).get("slot")
            logs = result.get("value", {}).get("logs", [])
            for log in logs:
                trade = decode_trade_event(log)
                if trade:
                    record_trade_event(mint, trade, slot)
        except:
            pass

    def on_open(ws):
        ws.send(json.dumps({
            "jsonrpc": "2.0", "id": 1,
            "method": "logsSubscribe",
            "params": [
                {"mentions": [curve_address]},
                {"commitment": "processed"}
            ]
        }))

    ws = websocket.WebSocketApp(
        SOLANA_RPC_WSS,
        on_open=on_open,
        on_message=on_msg,
        on_error=lambda ws, e: None,
        on_close=lambda ws, c, m: None,
    )

    with pattern_ws_lock:
        pattern_ws_registry[mint] = ws

    ws.run_forever(ping_interval=10, ping_timeout=5)

def stop_trade_flow_stream(mint):
    """Close and drop the trade-flow stream for a mint we're done with (entered-and-exited, or rejected pre-entry)."""
    with pattern_ws_lock:
        ws = pattern_ws_registry.pop(mint, None)
    if ws:
        try:
            ws.close()
        except:
            pass

# Backward-compatible alias: exit-path code calls close_momentum_ws() with no
# args, relying on active_position["mint"] for which stream to stop.
def close_momentum_ws():
    with position_lock:
        mint = active_position.get("mint")
    if mint:
        stop_trade_flow_stream(mint)

# ============ CURVE MONITORING ============

# ============ v13.0 ENTRY-TIME PATTERN CONFIRMATION ============
# This is what actually decides whether we enter at all. A token is only
# validated -- and therefore only entered -- once its OWN trade flow has
# shown the genuine trend-to-graduation pattern (buyer broadening, no
# bundling, no concentrated dumping). There is no clock here: we wait as
# long as it takes for the pattern to resolve one way or the other. The
# only things that end the wait are the pattern itself resolving, the
# curve running out of room to profit from (near graduation already), or
# the token going dead (RPC can't read it / zero interest at all). None of
# those are "rushing" -- they're the opportunity itself closing.
PATTERN_CHECK_INTERVAL_SEC = 1.0     # how often we re-check, not a deadline
ABANDON_PUSH_PROGRESS_PCT = 92.0     # curve this close to graduating already = no room left for TP1/TP2
MAX_CONSECUTIVE_CURVE_READ_FAILURES = 6  # curve unreadable this many checks in a row = token's dead/rugged
STALE_NO_ACTIVITY_TIMEOUT_SEC = 10.0  # v14.1: no new buy OR sell at all for this long = gone quiet, abandon -- fixes a stall bug where a token that got one buyer then went dead could wait forever, since the old check only ever fired on ZERO buyers

def wait_for_pattern_confirmation(mint, curve_address, detection_price_sol):
    """
    Blocks (in the pipeline's own background thread, never the scanner's
    main threads) until the entry pattern resolves. No fixed timeout.
    Returns (result, reason, latest_curve_state) where result is
    'confirmed' / 'rejected' / 'abandoned'.
    """
    reset_trade_flow(mint)
    threading.Thread(target=start_trade_flow_stream, args=(mint, curve_address), daemon=True).start()

    consecutive_read_failures = 0
    watch_start = time.time()
    last_state = None

    while True:
        time.sleep(PATTERN_CHECK_INTERVAL_SEC)

        state = get_curve_state(curve_address)
        if not state:
            consecutive_read_failures += 1
            if consecutive_read_failures >= MAX_CONSECUTIVE_CURVE_READ_FAILURES:
                return "abandoned", "Curve unreadable for too many checks -- treating as dead/rugged", last_state
            continue
        consecutive_read_failures = 0
        last_state = state

        push_progress_pct = calc_push_progress(state)
        if push_progress_pct is not None and push_progress_pct >= ABANDON_PUSH_PROGRESS_PCT:
            return "abandoned", f"Push progress {push_progress_pct:.1f}% -- too close to graduation already, no room left to profit", state

        current_price_sol, _ = calc_price_from_state(state)
        result, reason = evaluate_entry_pattern(mint, current_price_sol, detection_price_sol)
        if result == "confirmed":
            return "confirmed", reason, state
        if result == "rejected":
            return "rejected", reason, state

        # v14.1: abandon on ANY trade flow going quiet, not just zero-ever-buyers.
        # The old check used len(buyers)==0, which permanently disarmed itself
        # the moment even one buy landed -- so a token that got 1-2 buyers
        # then went completely dead could hang here indefinitely (confirmed:
        # this was the exact cause of multiple 15-45+ min scanner stalls).
        with trade_flow_lock:
            tf_state = trade_flow_state.get(mint)
            last_activity = tf_state["last_activity_time"] if tf_state else watch_start
        if (time.time() - last_activity) > STALE_NO_ACTIVITY_TIMEOUT_SEC:
            return "abandoned", f"No trade activity at all for {STALE_NO_ACTIVITY_TIMEOUT_SEC:.0f}s -- gone quiet, dead launch", state




def process_curve_update(curve_state):
    now = time.time()
    with position_lock:
        if not active_position["running"] or active_position["exit_triggered"]:
            return

        pos            = active_position
        phase          = pos["phase"]
        entry_mcap_sol = pos["entry_mcap_sol"]
        entry_sol      = pos["entry_sol"]
        tp1_sol        = pos["tp1_sol"]
        initial_real   = pos["initial_real_token_reserves"]

        real_token_reserves = curve_state["real_token_reserves"]

        if pos["lowest_real_token_reserves"] is None or real_token_reserves < pos["lowest_real_token_reserves"]:
            pos["lowest_real_token_reserves"] = real_token_reserves

        lowest = pos["lowest_real_token_reserves"]

        if pos["last_tick_time"] is not None:
            gap = now - pos["last_tick_time"]
            if gap > pos["max_tick_gap_seconds"]:
                pos["max_tick_gap_seconds"] = gap
        pos["last_tick_time"] = now
        pos["tick_count"] += 1

    price_sol, current_mcap_sol = calc_price_from_state(curve_state)
    if current_mcap_sol is None:
        return

    # v10.5.5: Recalculate crash threshold dynamically from current state
    detection_price = pos.get("launch_price_sol", 0)
    if detection_price > 0:
        live_crash_th = calc_crash_threshold(curve_state, detection_price)
        if live_crash_th:
            with position_lock:
                active_position["crash_threshold_pct"] = live_crash_th

    curve_pct = (real_token_reserves / initial_real * 100) if initial_real > 0 else 100

    if entry_mcap_sol > 0:
        current_value_sol = entry_sol * (current_mcap_sol / entry_mcap_sol)
    else:
        current_value_sol = entry_sol

    with position_lock:
        if current_value_sol > pos.get("peak_value_sol", 0):
            pos["peak_value_sol"] = current_value_sol
        peak_value = pos.get("peak_value_sol", current_value_sol)

    drawdown = 0.0
    if peak_value > 0:
        drawdown = (peak_value - current_value_sol) / peak_value

    # === v11.0 #3/#4: FLOW VELOCITY + TIME-TO-TARGET (real SOL-committed terms) ===
    current_real_sol = curve_state["real_sol_reserves"] / 1e9
    velocity_sol_per_sec = None
    time_to_target_sec = None
    with position_lock:
        last_v_sol  = pos.get("velocity_last_real_sol")
        last_v_time = pos.get("velocity_last_time")
        if last_v_sol is not None and last_v_time is not None:
            dt = now - last_v_time
            if dt > 0:
                velocity_sol_per_sec = (current_real_sol - last_v_sol) / dt
                active_position["flow_velocity_sol_per_sec"] = velocity_sol_per_sec
                if velocity_sol_per_sec > active_position.get("peak_velocity_sol_per_sec", 0):
                    active_position["peak_velocity_sol_per_sec"] = velocity_sol_per_sec
        active_position["velocity_last_real_sol"] = current_real_sol
        active_position["velocity_last_time"]     = now
        peak_velocity = active_position.get("peak_velocity_sol_per_sec", 0)

    push_progress_pct = calc_push_progress(curve_state)
    if velocity_sol_per_sec and velocity_sol_per_sec > 0 and push_progress_pct is not None:
        remaining_sol = max(0.0, COMPLETION_SOL_TARGET - current_real_sol)
        time_to_target_sec = remaining_sol / velocity_sol_per_sec if velocity_sol_per_sec > 0 else None

    sol_price = get_sol_price()
    ttt_str = f"{time_to_target_sec:.0f}s" if time_to_target_sec is not None else "N/A"
    vel_str = f"{velocity_sol_per_sec:+.4f}" if velocity_sol_per_sec is not None else "N/A"
    print(f"  [{'A' if phase == 'A' else 'B'}] Curve: {curve_pct:.1f}% | "
          f"MCap: {current_mcap_sol:.3f} SOL (${sol_to_usd(current_mcap_sol):,.0f}) | "
          f"Value: {current_value_sol:.4f} SOL (${sol_to_usd(current_value_sol):.2f}) | "
          f"Peak: {peak_value:.4f} | Drawdown: {drawdown*100:.1f}% | "
          f"Push: {push_progress_pct:.1f}% | Vel: {vel_str} SOL/s | TTT: {ttt_str} | "
          f"Tick #{pos['tick_count']}")

    with position_lock:
        active_position["last_value_before_exit_check"] = current_value_sol

    # v10.4.0: Drawdown removed — let it run to TP1 or crash guard

    # v14.5: reactive trend-health check REMOVED from here. It was firing on
    # every single price tick, including tick #1 right after entry -- proven
    # to fire on stale/residual activity rather than real new weakness (see
    # the 2-second exit that missed a real 20%+ move). Live risk monitoring
    # is now GMGN-driven, proactive (watching incentive-to-dump build up via
    # tagged risk wallets + profit overhang), and runs on its own timer in
    # gmgn_live_risk_monitor() -- not tied to price-tick frequency at all.

    if phase == "A":
        # v14.0: TP1 -- fixed multiplier (dynamic calibration removed)
        if entry_mcap_sol > 0 and current_value_sol >= tp1_sol:
            tp1_profit = current_value_sol - entry_sol
            with position_lock:
                pos["phase"]              = "B"
                pos["tp1_profit_sol"]     = tp1_profit
            print(f"\n  [TP1] FIRST TP HIT ({int((TARGET_MULT-1)*100)}%)")
            print(f"  Value: {current_value_sol:.4f} SOL (${sol_to_usd(current_value_sol):.2f})")
            print(f"  Profit: +{tp1_profit:.4f} SOL (${sol_to_usd(tp1_profit):.2f})")
            telegram_post_message(f"**TP1 HIT** | {pos['mint']}\nProfit: +{tp1_profit:.4f} SOL (${sol_to_usd(tp1_profit):.2f})")
            # v13.0: do NOT close the trade-flow stream here -- the trend
            # health engine (sell signal / wave rhythm / concentration trend
            # / quiet accumulation) still needs live data through Phase B.
            # Continue to Phase B for TP2, don't exit yet
            return
    elif phase == "B":
        # v14.0: TP2 -- fixed multiplier (dynamic calibration removed)
        final_tp_sol = entry_sol * FINAL_TARGET_MULT
        if entry_mcap_sol > 0 and current_value_sol >= final_tp_sol:
            total_profit = current_value_sol - entry_sol
            print(f"\n  [TP2] FINAL TP HIT ({int((FINAL_TARGET_MULT-1)*100)}%)")
            print(f"  Value: {current_value_sol:.4f} SOL (${sol_to_usd(current_value_sol):.2f})")
            print(f"  Total Profit: +{total_profit:.4f} SOL (${sol_to_usd(total_profit):.2f})")
            telegram_post_message(f"**TP2 HIT** | {pos['mint']}\nTotal Profit: +{total_profit:.4f} SOL (${sol_to_usd(total_profit):.2f})")
            exit_position("TP2_HIT", current_mcap_sol)
            return

def run_curve_websocket(mint, curve_address):
    def on_open(ws):
        with curve_ws_lock:
            curve_ws_ref["ws"] = ws
        ws.send(json.dumps({
            "jsonrpc": "2.0", "id": 1,
            "method":  "accountSubscribe",
            "params":  [
                curve_address,
                {"encoding": "base64", "commitment": "processed"}
            ]
        }))
        print(f"  [WS] Curve monitor active")

    def on_message(ws, message):
        try:
            data = json.loads(message)
            if "result" in data:
                return
            if data.get("method") != "accountNotification":
                return
            with position_lock:
                if not active_position["running"] or active_position["exit_triggered"]:
                    ws.close()
                    return
            value  = data.get("params", {}).get("result", {}).get("value", {})
            raw_data = value.get("data", {})
            if isinstance(raw_data, list) and len(raw_data) >= 1:
                data_b64 = raw_data[0]
                curve_state = decode_curve_state_from_base64(data_b64)
                if curve_state:
                    process_curve_update(curve_state)
        except:
            pass

    def on_error(ws, error): pass

    def on_close(ws, code, msg):
        with position_lock:
            still_running = (
                active_position["running"] and
                active_position["mint"] == mint and
                not active_position["exit_triggered"]
            )
        if still_running:
            time.sleep(1)
            threading.Thread(
                target=run_curve_websocket,
                args=(mint, curve_address),
                daemon=True
            ).start()

    ws = websocket.WebSocketApp(
        SOLANA_RPC_WSS,
        on_open    = on_open,
        on_message = on_message,
        on_error   = on_error,
        on_close   = on_close,
    )
    ws.run_forever(ping_interval=15, ping_timeout=8)

# ============ LIVE HOLDER MONITOR ============

def holder_monitor(mint):
    """
    v10.5.4: CRASH GUARD with live threshold from active_position.
    Uses the dynamically updated threshold — no redundant recalculation.
    """
    while True:
        time.sleep(HOLDER_MONITOR_INTERVAL)

        with position_lock:
            if not active_position["running"] or active_position["exit_triggered"]:
                return
            if active_position["mint"] != mint:
                return
            curve_addr = active_position["curve_address"]
            detection_price = active_position.get("launch_price_sol", 0)
            # entry_crash_threshold_pct removed in v10.5.5
            live_threshold = active_position.get("crash_threshold_pct", 0)

        if detection_price <= 0:
            continue

        state = get_curve_state(curve_addr)
        if not state:
            continue

        # v10.5.4: Use live-updated threshold from process_curve_update
        # Only recalculate if we don't have a live threshold yet
        if live_threshold <= 0:
            live_threshold = calc_crash_threshold(state, detection_price)
            if live_threshold:
            # min with entry threshold removed in v10.5.5
                with position_lock:
                    active_position["crash_threshold_pct"] = live_threshold

        trigger_pct = live_threshold / 2

        largest_pct, combined_pct, holder_count, details = get_holder_snapshot(mint)
        if largest_pct is None:
            continue

        # Check against half of entry threshold (one-way ratchet)
        if largest_pct >= trigger_pct or combined_pct >= trigger_pct:
            reason = f"Largest {largest_pct:.2f}% or Combined {combined_pct:.2f}% >= HALF entry threshold {trigger_pct:.2f}%"
            print(f"\n  [CRASH-GUARD] HALF-TRIGGER (live monitor)")
            print(f"  Reason: {reason}")

            with position_lock:
                active_position["crash_threshold_pct"] = live_threshold
                active_position["largest_holder_pct"] = largest_pct or 0
                active_position["combined_top5_pct"] = combined_pct or 0

            _, current_mcap = calc_price_from_state(state)
            exit_position("CRASH_GUARD_EXIT", current_mcap or active_position["entry_mcap_sol"])
            return

# ============ v13.0 #1: PRE-ENTRY CREATOR WALLET HISTORY ============
# Proactive, not reactive: checked ONCE at detection, before a single trade
# on THIS token happens. Looks at the creator wallet's past pump.fun token
# creations and whether any of them ever graduated. A wallet with a track
# record of nothing but rugs is a red flag before the pattern ever starts
# forming on the new token.
#
# NOTE (flagged honestly): this needs getSignaturesForAddress + a
# getTransaction per signature, which is meaningfully heavier/slower than
# anything else in the pipeline, and pump.fun launches fast. This can
# become the throughput bottleneck. Caps below (signature limit, sample
# cap) bound the cost, but if the bot feels sluggish in sim, look here first.
CREATOR_HISTORY_SIGNATURE_LIMIT = 25     # how far back into the creator's history we look
CREATOR_HISTORY_SAMPLE_CAP = 6           # stop once we've found this many prior pump.fun creations -- enough sample to judge
CREATOR_HISTORY_MIN_TOKENS_TO_JUDGE = 3  # need at least this many prior tokens found before we'll reject on "never graduated"

def get_creator_prior_mints(creator_wallet):
    """
    Returns a set of prior pump.fun mint addresses this wallet created,
    or None if the RPC call itself failed (distinct from "found zero").
    """
    sigs = rpc_call("getSignaturesForAddress", [creator_wallet, {"limit": CREATOR_HISTORY_SIGNATURE_LIMIT}])
    if sigs is None:
        return None
    prior_mints = set()
    for sig_info in sigs:
        if len(prior_mints) >= CREATOR_HISTORY_SAMPLE_CAP:
            break
        sig = sig_info.get("signature")
        if not sig:
            continue
        tx = rpc_call("getTransaction", [sig, {"encoding": "jsonParsed",
                      "maxSupportedTransactionVersion": 0, "commitment": "confirmed"}])
        if not tx:
            continue
        logs = tx.get("meta", {}).get("logMessages", []) or []
        if not any(PUMPFUN_INSTRUCTION in log for log in logs):
            continue
        mint = extract_mint_from_tx(tx, "pump.fun")
        if mint:
            prior_mints.add(mint)
    return prior_mints

def evaluate_creator_history(creator_wallet):
    """
    Returns (passes, reason). New creators (no history, or RPC couldn't
    read it) pass by default -- no history isn't the same as bad history,
    and an RPC hiccup shouldn't block an otherwise-good token.
    """
    if not creator_wallet:
        return True, "No creator wallet identified -- passing by default"

    prior_mints = get_creator_prior_mints(creator_wallet)
    if prior_mints is None:
        return True, "Could not read creator history (RPC) -- passing by default"

    if len(prior_mints) < CREATOR_HISTORY_MIN_TOKENS_TO_JUDGE:
        return True, f"Only {len(prior_mints)} prior token(s) found -- not enough history to judge, passing by default"

    graduated = 0
    for m in prior_mints:
        curve_addr = derive_bonding_curve(m)
        if not curve_addr:
            continue
        state = get_curve_state(curve_addr)
        if state and state.get("complete"):
            graduated += 1

    if graduated == 0:
        return False, f"Creator has {len(prior_mints)} prior tokens, none ever graduated -- rug pattern"
    return True, f"Creator has {len(prior_mints)} prior tokens, {graduated} graduated -- acceptable track record"

# ============ v14.4: GMGN VALIDATION LAYER ============
# GMGN's own docs are explicit: "Do not call gmgn.ai web endpoints directly
# -- use gmgn-cli commands." There is no separate plain-HTTP contract to
# call with `requests` -- gmgn-cli (a Node/TypeScript tool) IS the API
# surface. Our bot shells out to it as a subprocess and parses its JSON
# output. This supplements our existing checks (crash guard, bundled-buy
# detection) with data our own RPC-based checks can't see directly --
# it does not replace any of them.
#
# NOTE (flagged honestly, same as the trade-event byte offsets earlier):
# the exact field names below (rug_ratio, bundler_trader_amount_rate,
# rat_trader_amount_rate, sniper_count, suspected_insider_hold_rate) come
# from GMGN's own published README, but the exact JSON shape `gmgn-cli
# token security --raw` actually returns hasn't been visually confirmed
# against live output yet. Print the raw dict for the first several calls
# and sanity-check field names/nesting before trusting the thresholds below
# in anything but simulation.
GMGN_API_KEY = os.environ.get("GMGN_API_KEY", "")
GMGN_MIN_CALL_INTERVAL_SEC = 1.05  # a hair over GMGN's documented 1 req/sec limit, bot-wide across ALL concurrently-watched candidates
GMGN_CALL_TIMEOUT_SEC = 10          # each call spins up a fresh Node process -- bound how long we'll wait for one

gmgn_rate_limit_lock = threading.Lock()
gmgn_last_call_time = {"t": 0.0}

def gmgn_cli_call(args):
    """
    Runs a gmgn-cli command as a subprocess, throttled to ~1 call/sec
    bot-wide (shared across every concurrently-watched candidate, not
    per-token), and returns parsed JSON or None on any failure.
    Never raises -- a GMGN hiccup should never crash a candidate's pipeline.
    """
    if not GMGN_API_KEY:
        return None

    with gmgn_rate_limit_lock:
        wait = GMGN_MIN_CALL_INTERVAL_SEC - (time.time() - gmgn_last_call_time["t"])
        if wait > 0:
            time.sleep(wait)
        gmgn_last_call_time["t"] = time.time()

    env = os.environ.copy()
    env["GMGN_API_KEY"] = GMGN_API_KEY
    cmd = ["gmgn-cli"] + args + ["--raw"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=GMGN_CALL_TIMEOUT_SEC, env=env)
        if result.returncode != 0:
            print(f"  [WARN] gmgn-cli failed ({' '.join(args)}): {result.stderr[:200]}")
            return None
        return json.loads(result.stdout.strip())
    except subprocess.TimeoutExpired:
        print(f"  [WARN] gmgn-cli timed out after {GMGN_CALL_TIMEOUT_SEC}s: {' '.join(args)}")
        return None
    except json.JSONDecodeError as e:
        print(f"  [WARN] gmgn-cli returned unparsable output for {' '.join(args)}: {e}")
        return None
    except Exception as e:
        print(f"  [WARN] gmgn-cli call error ({' '.join(args)}): {e}")
        return None

# ============ v14.5 CONFIG: GMGN HOLDER-RISK THRESHOLDS ============
# v14.5: rebuilt entirely on `gmgn-cli token security` -- confirmed via live
# output that endpoint has NO rug_ratio/bundler/rat-trader/insider fields at
# all (it's a contract-level check: honeypot, tax, mint/freeze authority --
# pump.fun tokens all look identical there since the bonding curve already
# forces those to be safe). The REAL data lives in `token holders`: each
# holder carries maker_token_tags (bundler/rat_trader/dev_team/sniper/
# creator), is_suspicious, amount_percentage, and unrealized_profit/
# total_cost -- verified against live output on a real mint.
GMGN_BUNDLER_PCT_REJECT = 15.0     # % of supply held by wallets tagged "bundler"
GMGN_RAT_TRADER_PCT_REJECT = 30.0  # % of supply held by wallets tagged "rat_trader"
GMGN_INSIDER_PCT_REJECT = 30.0     # % of supply held by wallets tagged "dev_team" or "creator"
GMGN_SUSPICIOUS_COUNT_REJECT = 3   # this many top holders flagged is_suspicious by GMGN

# Live-only, on top of the same absolute thresholds above:
GMGN_LIVE_CHECK_INTERVAL_SEC = 12.0   # while holding: how often we re-pull holder data (not tied to price-tick frequency)
GMGN_LIVE_RISK_RISE_PCT = 10.0        # combined bundler+rat+insider share rising this many points since entry = incentive to dump is building
GMGN_OVERHANG_GAIN_RATIO = 0.5        # a holder sitting on >= 50% unrealized gain counts as "deep profit"
GMGN_OVERHANG_PCT_REJECT = 25.0       # % of supply sitting on that kind of deep unrealized gain
# NOTE (flagged honestly): the one live sample we've seen showed unrealized_profit/
# unrealized_pnl as null/0 for every on-curve (pre-graduation) holder -- GMGN may
# not compute this reliably for tokens still on the bonding curve. This check
# is written to no-op gracefully (contributes 0%) when the data isn't populated,
# rather than assume it's working. Watch a few real GMGN-RISK-EXIT logs to see
# whether overhang_pct ever comes back non-zero before trusting it.

def get_gmgn_holder_risk(mint):
    """
    Pulls token holders from GMGN and reduces it to the numbers we act on.
    Returns None on failure (never raises).
    """
    data = gmgn_cli_call(["token", "holders", "--chain", "sol", "--address", mint, "--limit", "20"])
    if not data or "list" not in data:
        return None

    holders = data["list"]
    bundler_pct = 0.0
    rat_pct = 0.0
    insider_pct = 0.0
    overhang_pct = 0.0
    suspicious_count = 0
    sniper_count = 0

    for h in holders:
        pct = (h.get("amount_percentage") or 0) * 100
        tags = h.get("maker_token_tags") or []

        if "bundler" in tags:
            bundler_pct += pct
        if "rat_trader" in tags:
            rat_pct += pct
        if "dev_team" in tags or "creator" in tags:
            insider_pct += pct
        if "sniper" in tags:
            sniper_count += 1
        if h.get("is_suspicious"):
            suspicious_count += 1

        unrealized = h.get("unrealized_profit")
        cost = h.get("total_cost")
        if unrealized and cost and cost > 0 and (unrealized / cost) >= GMGN_OVERHANG_GAIN_RATIO:
            overhang_pct += pct

    return {
        "bundler_pct": bundler_pct,
        "rat_pct": rat_pct,
        "insider_pct": insider_pct,
        "overhang_pct": overhang_pct,
        "suspicious_count": suspicious_count,
        "sniper_count": sniper_count,
    }

def evaluate_gmgn_holder_risk(mint):
    """
    Used both pre-entry (Checkpoints A/B) and live. Returns (passes, reason, snapshot).
    snapshot is the raw dict from get_gmgn_holder_risk (or None) -- callers
    that need a baseline for later drift comparison (live monitoring) use this.
    If GMGN is unreachable/unconfigured, passes by default -- GMGN supplements
    our own checks, it's never a single point of failure that blocks an
    otherwise-good token.
    """
    snapshot = get_gmgn_holder_risk(mint)
    if snapshot is None:
        return True, "GMGN data unavailable -- passing by default", None

    reasons = []
    if snapshot["bundler_pct"] >= GMGN_BUNDLER_PCT_REJECT:
        reasons.append(f"bundler wallets hold {snapshot['bundler_pct']:.1f}% of supply")
    if snapshot["rat_pct"] >= GMGN_RAT_TRADER_PCT_REJECT:
        reasons.append(f"rat-trader wallets hold {snapshot['rat_pct']:.1f}% of supply")
    if snapshot["insider_pct"] >= GMGN_INSIDER_PCT_REJECT:
        reasons.append(f"dev/insider wallets hold {snapshot['insider_pct']:.1f}% of supply")
    if snapshot["suspicious_count"] >= GMGN_SUSPICIOUS_COUNT_REJECT:
        reasons.append(f"{snapshot['suspicious_count']} top holders flagged suspicious by GMGN")
    if snapshot["overhang_pct"] >= GMGN_OVERHANG_PCT_REJECT:
        reasons.append(f"{snapshot['overhang_pct']:.1f}% of supply sitting on {GMGN_OVERHANG_GAIN_RATIO*100:.0f}%+ unrealized gain")

    if reasons:
        return False, "GMGN holder risk -- " + "; ".join(reasons), snapshot
    return True, (f"GMGN OK (bundler={snapshot['bundler_pct']:.1f}%, rat={snapshot['rat_pct']:.1f}%, "
                  f"insider={snapshot['insider_pct']:.1f}%, suspicious={snapshot['suspicious_count']}, "
                  f"sniper_count={snapshot['sniper_count']})"), snapshot

def gmgn_live_risk_monitor(mint, baseline_snapshot):
    """
    v14.5: The proactive replacement for the old reactive trend-health engine.
    Instead of reacting to a dump already happening (trade flow AFTER the
    fact), this watches whether the INCENTIVE to dump is building --
    GMGN-tagged risk-wallet share climbing, or supply concentrating in
    wallets sitting on deep unrealized gains -- checked on its own timer,
    independent of price-tick frequency (confirmed cause of a bad early
    exit: the old engine fired on tick #1, 2 seconds after entry).
    """
    while True:
        time.sleep(GMGN_LIVE_CHECK_INTERVAL_SEC)

        with position_lock:
            if not active_position["running"] or active_position["exit_triggered"] or active_position.get("mint") != mint:
                return

        passes, reason, snapshot = evaluate_gmgn_holder_risk(mint)
        if snapshot is None:
            continue  # GMGN hiccup -- never force an exit purely because a call failed

        reasons = []
        if not passes:
            reasons.append(reason)

        if baseline_snapshot:
            combined_now = snapshot["bundler_pct"] + snapshot["rat_pct"] + snapshot["insider_pct"]
            combined_base = baseline_snapshot["bundler_pct"] + baseline_snapshot["rat_pct"] + baseline_snapshot["insider_pct"]
            if (combined_now - combined_base) >= GMGN_LIVE_RISK_RISE_PCT:
                reasons.append(f"combined bundler/rat/insider share rose {combined_now - combined_base:.1f} "
                                f"points since entry ({combined_base:.1f}% -> {combined_now:.1f}%)")

        if reasons:
            with position_lock:
                if not active_position["running"] or active_position["exit_triggered"] or active_position.get("mint") != mint:
                    return
                curve_address = active_position.get("curve_address")
                fallback_mcap = active_position.get("entry_mcap_sol")
            print(f"\n  [GMGN-RISK-EXIT] " + "; ".join(reasons))
            state = get_curve_state(curve_address)
            _, current_mcap = calc_price_from_state(state) if state else (None, None)
            exit_position("GMGN_RISK_EXIT", current_mcap or fallback_mcap)
            return

def timeout_watcher(mint):
    time.sleep(1800)
    with position_lock:
        should_exit = (
            active_position["running"] and
            active_position["mint"] == mint and
            not active_position["exit_triggered"]
        )
        curve_address = active_position["curve_address"]
        fallback_mcap = active_position["entry_mcap_sol"]
    if should_exit:
        state = get_curve_state(curve_address)
        _, mcap = calc_price_from_state(state) if state else (None, None)
        exit_position("TIMEOUT", mcap or fallback_mcap)


# ============ v11.0 #1/#2/#3/#4: PUSH PROGRESS, VELOCITY, TIME-TO-TARGET ============
# Pump.fun bonding curves complete once ~85 real SOL has been raised into the
# curve. This lets us talk about "how close to graduation" in real dollars
# raised, rather than an abstract curve percentage.
COMPLETION_SOL_TARGET = 85.0

def calc_push_progress(curve_state):
    """
    v11.0 #1: SOL-committed vs SOL-needed, in real-money terms.
    Returns % of the way from 0 SOL raised to COMPLETION_SOL_TARGET SOL raised.
    """
    try:
        real_sol = curve_state["real_sol_reserves"] / 1e9
        pct = (real_sol / COMPLETION_SOL_TARGET) * 100
        return max(0.0, min(100.0, pct))
    except:
        return None

# v14.0: dynamic TP calibration removed entirely (confirmed) -- it depended
# on validated_tokens.csv surviving across restarts, which it can't on
# Render's free ephemeral filesystem, so it would have silently never
# activated. Back to fixed TARGET_MULT / FINAL_TARGET_MULT.

# ============ MAIN PIPELINE ============


# ============ v14.6: BOUNDED VALIDATION CONCURRENCY ============
# v14.2 removed the single global "busy" flag entirely so every candidate
# could be watched concurrently. Confirmed via real heartbeat data: fully
# unbounded concurrency overwhelms the free Helius RPC plan (repeated
# 'rate limited' warnings, and SSL connection failures) and GMGN's shared
# 1/sec throttle -- leaving a large, growing gap between total detections
# and anything actually resolving into a bucket (entered/rejected/abandoned).
# This caps how many candidates can be ACTIVELY validating (the RPC+GMGN
# heavy part) at once. Anything over the cap waits its turn -- honestly
# tracked as a live queue depth, not silently dropped or left unaccounted.
MAX_CONCURRENT_VALIDATIONS = 10  # starting point -- tune based on how RPC failures/queue depth trend

validation_semaphore = threading.Semaphore(MAX_CONCURRENT_VALIDATIONS)
validation_gauge_lock = threading.Lock()
validation_gauges = {"active": 0, "queued": 0}  # live snapshots, not per-interval tallies -- read as "right now"

def safe_full_token_pipeline(mint, source, creator_wallet=None, tx_fetch_elapsed=0.0):
    """
    Safety wrapper: catches any unhandled exception in full_token_pipeline so
    one bad token can't crash its own watcher thread silently. v14.2: no
    longer needs to reset a shared "busy" flag -- each token's pipeline run
    is independent now, so a crash here only affects this one candidate.
    v14.6: now waits for a bounded validation slot before doing any real work.
    """
    with validation_gauge_lock:
        validation_gauges["queued"] += 1
    wait_start = time.time()
    validation_semaphore.acquire()
    wait_time = time.time() - wait_start
    with validation_gauge_lock:
        validation_gauges["queued"] -= 1
        validation_gauges["active"] += 1
    if wait_time > 2.0:
        print(f"  [QUEUE] {mint} waited {wait_time:.1f}s for a validation slot ({MAX_CONCURRENT_VALIDATIONS} concurrent cap)")

    try:
        full_token_pipeline(mint, source, creator_wallet, tx_fetch_elapsed)
    except Exception as e:
        import traceback
        print(f"  [CRITICAL] Unhandled exception in full_token_pipeline for {mint}: {e}")
        traceback.print_exc()
        # If this candidate had already claimed the position slot before
        # crashing, release it -- otherwise a mid-pipeline crash after entry
        # would leave active_position stuck "running" forever with no
        # monitor threads ever started to close it out.
        with position_lock:
            stuck = active_position.get("running") and active_position.get("mint") == mint
        if stuck:
            print(f"  [CRITICAL] {mint} had claimed the position slot before crashing -- releasing it")
            release_position()
    finally:
        with validation_gauge_lock:
            validation_gauges["active"] -= 1
        validation_semaphore.release()

def get_real_token_supply(mint):
    """
    Reads the mint's actual on-chain token supply directly via getTokenSupply
    -- independent of the bonding curve struct's internal token_total_supply
    field. Added as a second Mayhem signal after a genuine Mayhem token
    (confirmed via dexscreener: 2.0B supply, holder tagged "Pump.fun Mayhem")
    slipped through the curve-struct-only check. The two sources may not
    always agree if the agent's extra minted supply isn't reflected in the
    curve struct's own field.
    """
    result = rpc_call("getTokenSupply", [mint, {"commitment": "confirmed"}])
    if not result:
        return None
    try:
        return int(result["value"]["amount"])
    except:
        return None

def full_token_pipeline(mint, source, creator_wallet=None, tx_fetch_elapsed=0.0):
    pipeline_start = time.time()

    print(f"\n{'='*52}")
    print(f"  VALIDATING: {mint}")
    print(f"  {datetime.now().strftime('%H:%M:%S')} | {source}")
    print(f"  https://solscan.io/token/{mint}")
    print(f"{'='*52}")

    if not mint.endswith("pump"):
        print(f"  [FAIL] Not a pump.fun token")
        bump_scan_stat("rejected_basic")
        return

    curve_address = derive_bonding_curve(mint)
    if not curve_address:
        print(f"  [FAIL] Could not derive curve address")
        bump_scan_stat("rejected_basic")
        return

    detection_state = get_curve_state(curve_address)
    if not detection_state:
        print(f"  [FAIL] Could not read curve state")
        bump_scan_stat("rejected_basic")
        return

    detection_price_sol, detection_mcap_sol = calc_price_from_state(detection_state)
    # v11.0 #2: snapshot launch reserves -- needed by calc_push_progress/velocity math
    launch_vtr = detection_state["virtual_token_reserves"]
    launch_vsr = detection_state["virtual_sol_reserves"]

    real_sol = detection_state["real_sol_reserves"] / 1e9
    if real_sol < 0.3:
        # One quick retry before giving up permanently -- a fresh curve read
        # of exactly 0 SOL just means nobody's bought yet at this instant,
        # not that the token is dead. Waiting ~1s and checking once more
        # catches tokens whose first buyer landed just after our first look,
        # without slowing down every token with a blanket delay.
        time.sleep(1.0)
        retry_state = get_curve_state(curve_address)
        if retry_state:
            retry_real_sol = retry_state["real_sol_reserves"] / 1e9
            if retry_real_sol >= 0.3:
                print(f"  [RETRY-OK] Was too early ({real_sol:.3f} SOL), now {retry_real_sol:.3f} SOL after 1s retry")
                detection_state = retry_state
                detection_price_sol, detection_mcap_sol = calc_price_from_state(detection_state)
                launch_vtr = detection_state["virtual_token_reserves"]
                launch_vsr = detection_state["virtual_sol_reserves"]
                real_sol = retry_real_sol
            else:
                print(f"  [FAIL] Too early: {retry_real_sol:.3f} SOL in curve (after retry)")
                bump_scan_stat("rejected_basic")
                return
        else:
            print(f"  [FAIL] Too early: {real_sol:.3f} SOL in curve (retry read failed)")
            bump_scan_stat("rejected_basic")
            return

    raw_supply = detection_state["token_total_supply"]
    real_supply = get_real_token_supply(mint)  # second, independent source
    is_mayhem_by_curve = raw_supply >= MAYHEM_SUPPLY_FLOOR
    is_mayhem_by_mint  = real_supply is not None and real_supply >= MAYHEM_SUPPLY_FLOOR
    if is_mayhem_by_curve or is_mayhem_by_mint:
        print(f"  [FAIL] Mayhem Mode -- curve_supply={raw_supply/1e15:.2f}B "
              f"mint_supply={(real_supply/1e15) if real_supply else 'N/A'}B")
        bump_scan_stat("rejected_basic")
        return

    # v10.3.8: REMOVED sniper dominance check
    # Sniper dominance is often the pump itself. Crash guard handles
    # concentration risk. Let aggressive early buyers through.

    # === v13.0 #1: PRE-ENTRY CREATOR WALLET HISTORY ===
    # Cheap checks (mayhem, curve validity) already ran above, so we don't
    # waste this RPC-heavy check on tokens that would've failed anyway.
    print(f"\n  [CREATOR-CHECK] Reading creator wallet history...")
    creator_ok, creator_reason = evaluate_creator_history(creator_wallet)
    print(f"  [CREATOR-CHECK] {'PASS' if creator_ok else 'FAIL'} -- {creator_reason}")
    if not creator_ok:
        bump_scan_stat("rejected_creator_history")
        return

    # === v14.5 CHECKPOINT A: GMGN HOLDER-RISK CHECK (early) ===
    # Supplements creator-history and the (later) crash guard with data our
    # own RPC-based checks can't see directly: GMGN-tagged bundler/rat-trader/
    # insider wallet share, suspicious-wallet count, profit overhang.
    print(f"\n  [GMGN-CHECK-A] Reading GMGN holder-risk data...")
    gmgn_ok_a, gmgn_reason_a, _gmgn_snap_a = evaluate_gmgn_holder_risk(mint)
    print(f"  [GMGN-CHECK-A] {'PASS' if gmgn_ok_a else 'FAIL'} -- {gmgn_reason_a}")
    if not gmgn_ok_a:
        bump_scan_stat("rejected_gmgn_risk")
        return

    # === v13.0: DYNAMIC PATTERN CONFIRMATION -- no rush, no fixed timeout ===
    # This IS the entry gate now. We watch this specific token's own trade
    # flow (real wallets buying/selling) until it either shows the genuine
    # broadening pattern (confirmed -> proceed to risk checks below), shows
    # the pump-and-dump pattern (rejected -> walk away), or the opportunity
    # itself closes (abandoned -- curve too near graduation, or dead with
    # zero interest). A validated token is a token we hold, so there is no
    # clock pushing this decision.
    print(f"\n  [WATCHING] {mint} -- observing trade flow for genuine broadening vs. pump-and-dump...")
    result, reason, entry_state = wait_for_pattern_confirmation(mint, curve_address, detection_price_sol)
    print(f"  [PATTERN] {result.upper()} -- {reason}")

    if result != "confirmed":
        bump_scan_stat("rejected_pattern" if result == "rejected" else "abandoned")
        stop_trade_flow_stream(mint)
        clear_trade_flow(mint)
        return

    if not entry_state:
        entry_state = get_curve_state(curve_address)
    if not entry_state:
        print(f"  [FAIL] Could not read entry state after pattern confirmation")
        bump_scan_stat("rejected_basic")
        stop_trade_flow_stream(mint)
        clear_trade_flow(mint)
        return

    entry_price_sol, entry_mcap_sol = calc_price_from_state(entry_state)
    if entry_mcap_sol is None:
        print(f"  [FAIL] Could not price curve")
        bump_scan_stat("rejected_basic")
        stop_trade_flow_stream(mint)
        clear_trade_flow(mint)
        return

    elapsed = time.time() - pipeline_start

    # === RISK AXIS: crash guard (holder concentration) -- unchanged ===
    # The pattern confirmation above answered "is this genuinely trending
    # toward graduation" (the opportunity question). This still separately
    # answers "how exposed are we to one wallet crashing it" (the risk
    # question). Both have to pass.
    push_progress_pct = calc_push_progress(entry_state)

    passes_guard, crash_th, largest_pct, combined_pct, guard_reason = evaluate_crash_guard(
        mint, entry_state, detection_price_sol, is_live_monitor=False
    )

    print(f"\n  [ENTRY-GATE] RISK CHECK (pattern already confirmed above)")
    print(f"  Push progress: {push_progress_pct:.1f}% toward {COMPLETION_SOL_TARGET:.0f} SOL completion" if push_progress_pct is not None else "  Push progress: N/A")
    print(f"  Risk (crash guard): {'PASS' if passes_guard else 'FAIL'} -- {guard_reason}")
    print(f"  Full Threshold:  {crash_th:.4f}% of supply" if crash_th is not None else "  Full Threshold:  N/A")
    print(f"  Half Threshold:  {crash_th/2:.4f}% of supply" if crash_th is not None else "  Half Threshold:  N/A")
    print(f"  [ENTRY CHECK] Largest Holder:  {largest_pct:.2f}%" if largest_pct is not None else "  [ENTRY CHECK] Largest Holder:  N/A")
    print(f"  [ENTRY CHECK] Combined Top-5:  {combined_pct:.2f}%" if combined_pct is not None else "  [ENTRY CHECK] Combined Top-5:  N/A")

    if not passes_guard:
        # v11.0 #11: rejections no longer post to alerts -- console/log only
        bump_scan_stat("rejected_crash_guard")
        stop_trade_flow_stream(mint)
        clear_trade_flow(mint)
        return

    # v13.0: MAX_ENTRY_MOVE_PCT removed -- it directly fought the pattern
    # wait (confirming genuine broadening requires real buys, which move
    # price up while we wait; a fixed % cap would reject good setups purely
    # for taking longer to confirm).

    print(f"\n  [RECHECK] Final holder re-check before entry (FULL threshold)...")
    passes_guard2, crash_th2, largest_pct2, combined_pct2, guard_reason2 = evaluate_crash_guard(
        mint, entry_state, detection_price_sol, is_live_monitor=False
    )
    if not passes_guard2:
        print(f"  [FAIL] Holder changed during validation: {guard_reason2}")
        bump_scan_stat("rejected_crash_guard")
        stop_trade_flow_stream(mint)
        clear_trade_flow(mint)
        return
    crash_th = crash_th2
    largest_pct = largest_pct2
    combined_pct = combined_pct2

    # === v14.5 CHECKPOINT B: GMGN HOLDER-RISK RECHECK (final, right before entry) ===
    # The pattern-confirmation wait can take anywhere from seconds to much
    # longer -- catch anything that changed on GMGN's side during that
    # window. This snapshot also becomes the ENTRY BASELINE that
    # gmgn_live_risk_monitor() compares against while the position is held.
    print(f"\n  [GMGN-CHECK-B] Final GMGN holder-risk recheck before entry...")
    gmgn_ok_b, gmgn_reason_b, gmgn_baseline_snapshot = evaluate_gmgn_holder_risk(mint)
    print(f"  [GMGN-CHECK-B] {'PASS' if gmgn_ok_b else 'FAIL'} -- {gmgn_reason_b}")
    if not gmgn_ok_b:
        bump_scan_stat("rejected_gmgn_risk")
        stop_trade_flow_stream(mint)
        clear_trade_flow(mint)
        return

    entry_sol = usd_to_sol(ENTRY_USD)
    tp1_sol   = entry_sol * TARGET_MULT

    print(f"\n  [PASS] ALL CHECKS PASSED")
    print(f"  Entry MCap: {entry_mcap_sol:.3f} SOL")
    print(f"  Entry: {entry_sol:.4f} SOL | TP1: {tp1_sol:.4f} SOL ({TARGET_MULT}x)")
    print(f"  Detection-to-entry: {elapsed:.2f}s")
    print(f"  Full Threshold: {crash_th:.4f}% | Half: {crash_th/2:.4f}%")

    entry_vtr = entry_state["virtual_token_reserves"]
    entry_vsr = entry_state["virtual_sol_reserves"]
    entry_real_sol = entry_state["real_sol_reserves"]

    with position_lock:
        slot_already_taken = active_position.get("running", False)
        if not slot_already_taken:
            active_position.update({
                "running":                     True,
                "mint":                        mint,
                "curve_address":               curve_address,
                "entry_mcap_sol":              entry_mcap_sol,
                "entry_price_sol":             entry_price_sol,
                "launch_price_sol":            detection_price_sol,
                "launch_vtr":                  launch_vtr,
                "launch_vsr":                  launch_vsr,
                "entry_sol":                   entry_sol,
                "tp1_sol":                     tp1_sol,
                "initial_real_token_reserves": entry_state["real_token_reserves"],
                "entry_vtr":                   entry_vtr,
                "entry_time":                  time.time(),
                "phase":                       "A",
                "tp1_profit_sol":              0.0,
                "freeroll_entry_sol":          0.0,
                "lowest_real_token_reserves":  entry_state["real_token_reserves"],
                "exit_triggered":              False,
                "last_tick_time":              None,
                "max_tick_gap_seconds":        0.0,
                "tick_count":                  0,
                "last_value_before_exit_check": None,
                "crash_threshold_pct":         crash_th,
                "largest_holder_pct":          largest_pct,
                "combined_top5_pct":           combined_pct,
                "peak_value_sol":              entry_sol,
                "buy_pressure_last_real_sol":  entry_real_sol / 1e9,
                "buy_pressure_last_time":      time.time(),
                "velocity_last_real_sol":      entry_real_sol / 1e9,
                "velocity_last_time":          time.time(),
                "flow_velocity_sol_per_sec":   0.0,
                "peak_velocity_sol_per_sec":   0.0,
                "creator_wallet":              creator_wallet,
                "tokens_sold_since_entry":     0,
            })

    if slot_already_taken:
        # v14.2: this candidate genuinely confirmed (real pattern, passed
        # crash guard) -- it's a real missed opportunity, not a discard.
        # Watching every candidate concurrently means this WILL happen
        # sometimes; logging it individually is the point, not a silent drop.
        print(f"\n  [SLOT-TAKEN] {mint} confirmed genuine pattern and passed risk checks, "
              f"but another position is already open -- not entered")
        bump_scan_stat("confirmed_slot_taken")
        telegram_post_message(
            f"**CONFIRMED (slot already taken)** | {mint}\n"
            f"Pattern confirmed -- genuine broadening, passed crash guard -- but another "
            f"position was already open, so this one was not entered.\n"
            f"Push progress: {push_progress_pct:.1f}% toward completion\n"
            f"https://solscan.io/token/{mint}\n"
            f"https://gmgn.ai/sol/token/{mint}"
        )
        stop_trade_flow_stream(mint)
        clear_trade_flow(mint)
        return

    bump_scan_stat("entered")
    telegram_post_message(
        f"**OPENED POSITION** | {mint}\n"
        f"Entry: {entry_sol:.4f} SOL | MCap: {entry_mcap_sol:.3f} SOL\n"
        f"TP1: {TARGET_MULT}x | TP2: {FINAL_TARGET_MULT}x | Crash Guard Only\n"
        f"Entry Guard (FULL): {crash_th:.4f}% | Live Guard (HALF): {crash_th/2:.4f}%\n"
        f"Largest: {largest_pct:.2f}% | Combined: {combined_pct:.2f}%\n"
        f"Push progress: {push_progress_pct:.1f}% toward completion\n"
        f"Detection-to-entry: {elapsed:.2f}s\n"
        f"https://solscan.io/token/{mint}\n"
        f"https://gmgn.ai/sol/token/{mint}"
    )

    # v10.4.2: Log entry to CSV
    log_entry(mint, entry_mcap_sol, entry_sol, crash_th, largest_pct, combined_pct)

    threading.Thread(target=timeout_watcher, args=(mint,), daemon=True).start()
    threading.Thread(target=run_curve_websocket, args=(mint, curve_address), daemon=True).start()
    threading.Thread(target=holder_monitor, args=(mint,), daemon=True).start()
    threading.Thread(target=gmgn_live_risk_monitor, args=(mint, gmgn_baseline_snapshot), daemon=True).start()
    # v12.1: trade-flow stream is already running from the pattern-confirmation
    # watch phase above -- do NOT reset it or start a second one here, that
    # would throw away the exact buyer/seller history that just confirmed
    # this was a genuine trend, right as we enter the trade.
    # v13.0: creator wallet is checked ONCE pre-entry (evaluate_creator_history) --
    # no live monitor thread needed anymore.
    # v14.5: live risk monitoring is now GMGN-driven and proactive (see
    # gmgn_live_risk_monitor above), replacing the old reactive trade-flow
    # based trend-health engine entirely.

# v14.4: DexScreener detection removed entirely -- confirmed not needed,
# RPC logsSubscribe already handles detection alone. GMGN now fills the
# "matters in validation" role DexScreener never actually served (it was
# only ever a second detection source, never a data source).

# ============ SOLANA RPC BACKUP DETECTION ============

def extract_mint_from_tx(tx_data, source):
    try:
        if not tx_data:
            return None
        def is_plausible_mint(addr):
            return bool(addr) and isinstance(addr, str) and addr.endswith("pump")
        for inner in tx_data.get("meta", {}).get("innerInstructions", []):
            for instruction in inner.get("instructions", []):
                parsed = instruction.get("parsed", {})
                if isinstance(parsed, dict):
                    if parsed.get("type") in ["initializeMint", "initializeMint2"]:
                        mint = parsed.get("info", {}).get("mint")
                        if is_plausible_mint(mint):
                            return mint
        post_balances = tx_data.get("meta", {}).get("postTokenBalances", [])
        pre_balances  = tx_data.get("meta", {}).get("preTokenBalances",  [])
        pre_mints     = {b.get("mint") for b in pre_balances}
        for balance in post_balances:
            mint = balance.get("mint")
            if is_plausible_mint(mint) and mint not in pre_mints:
                return mint
        accounts = tx_data.get("transaction", {}).get("message", {}).get("accountKeys", [])
        if source == "pump.fun" and len(accounts) >= 2:
            for idx in [0, 1, 2]:
                if idx < len(accounts):
                    account = accounts[idx]
                    key = account.get("pubkey", account) if isinstance(account, dict) else str(account)
                    if is_plausible_mint(key) and key not in pre_mints:
                        return key
        return None
    except:
        return None

def extract_creator_from_tx(tx_data):
    """
    v11.0 #9: Best-effort extraction of the token creator/dev wallet --
    the fee payer (accountKeys[0]) on a pump.fun "Instruction: Create" tx.
    Returns None if it can't be determined; creator monitoring is simply
    skipped for that token in that case rather than blocking the trade.
    """
    try:
        accounts = tx_data.get("transaction", {}).get("message", {}).get("accountKeys", [])
        if not accounts:
            return None
        first = accounts[0]
        key = first.get("pubkey", first) if isinstance(first, dict) else str(first)
        return key or None
    except:
        return None

def heartbeat():
    dots = 0
    while True:
        time.sleep(10)
        dots   = (dots % 3) + 1
        with position_lock:
            holding = active_position.get("running", False)
        status = "HOLDING POSITION" if holding else "WATCHING CANDIDATES"
        sol_p  = f"SOL=${sol_price_state['usd']:.0f}" if sol_price_state["usd"] else "SOL=?"
        print(f"\r  [{datetime.now().strftime('%H:%M:%S')}] {status}{'.' * dots} | {sol_p}   ", end="", flush=True)

HEARTBEAT_INTERVAL_SECONDS = 900
POSITION_STALE_WARNING_SECONDS = 20 * 60

def telegram_heartbeat():
    while True:
        time.sleep(HEARTBEAT_INTERVAL_SECONDS)
        try:
            with position_lock:
                pos_snapshot = dict(active_position)
            with telegram_failure_lock:
                fails = dict(telegram_failure_state)
            with scan_stats_lock:
                stats_snapshot = dict(scan_stats)
                for k in scan_stats:
                    scan_stats[k] = 0
            with rpc_failure_lock:
                rpc_fails = dict(rpc_failure_state)
                rpc_failure_state["count"] = 0

            # v14.0: unconditional -- always post every 15 min so you always
            # know the bot's alive, not just when something's already wrong.
            status = "PROCESSING (position open)" if pos_snapshot["running"] else "SCANNING"
            lines = [f"[HEARTBEAT] Status: {status}"]
            sol_p_val = get_sol_price()
            lines.append(f"SOL: ${sol_p_val:.2f}" if sol_p_val else "SOL: unknown")

            # v14.2: every candidate is now watched concurrently -- no more
            # "skipped busy". Instead: entered vs. confirmed_slot_taken tells
            # you exactly how often the pattern engine found something real
            # but a position was already open to take the trade.
            lines.append(
                f"Detections: {stats_snapshot['raw_detections']} | "
                f"Entered: {stats_snapshot['entered']} | "
                f"Confirmed (slot taken): {stats_snapshot['confirmed_slot_taken']}"
            )
            # v14.6: live snapshot (right now, not since-last-heartbeat) of
            # the bounded validation concurrency cap -- if "queued" stays
            # persistently high, the cap is too tight for how much pump.fun
            # is launching; if it's usually 0, we have headroom to raise it.
            with validation_gauge_lock:
                active_now = validation_gauges["active"]
                queued_now = validation_gauges["queued"]
            lines.append(f"Validation slots: {active_now}/{MAX_CONCURRENT_VALIDATIONS} active | {queued_now} queued")
            lines.append(
                f"Rejected -- basic: {stats_snapshot['rejected_basic']} | "
                f"creator history: {stats_snapshot['rejected_creator_history']} | "
                f"GMGN risk: {stats_snapshot['rejected_gmgn_risk']} | "
                f"pattern: {stats_snapshot['rejected_pattern']} | "
                f"crash guard: {stats_snapshot['rejected_crash_guard']} | "
                f"abandoned: {stats_snapshot['abandoned']}"
            )
            if stats_snapshot['raw_detections'] == 0:
                lines.append("[NOTE] Zero detections since last heartbeat -- either pump.fun was quiet, "
                              "or the main WS connection may be down (check for repeated "
                              "'Main WS closed -- reconnecting...' in the console logs).")

            if pos_snapshot["running"]:
                age_sec = time.time() - pos_snapshot.get("entry_time", time.time())
                lines.append(f"Open: {pos_snapshot['mint']} | Phase {pos_snapshot['phase']} | Age: {age_sec/60:.1f}m")
                if age_sec >= POSITION_STALE_WARNING_SECONDS:
                    lines.append(f"[WARN] Position open {age_sec/60:.1f}m -- approaching timeout.")

            if fails["fails"] >= 2:
                lines.append(f"[WARN] Telegram post failures: {fails['fails']}")

            if rpc_fails["count"] > 0:
                lines.append(f"[WARN] RPC failures: {rpc_fails['count']} | Last: {rpc_fails['last_reason'][:200]}")

            telegram_post_message("\n".join(lines))
        except Exception as e:
            print(f"  [WARN] telegram_heartbeat error: {e}")

sub_map = {}

def on_message(ws, message):
    try:
        data = json.loads(message)
        if "result" in data and isinstance(data["result"], int):
            sub_id = data["result"]
            req_id = data.get("id")
            if req_id == 1:
                sub_map[sub_id] = "pump.fun"
            return
        if "error" in data or data.get("method") != "logsNotification":
            return
        params    = data.get("params", {})
        result    = params.get("result", {})
        value     = result.get("value", {})
        logs      = value.get("logs", [])
        signature = value.get("signature", "")
        sub_id    = params.get("subscription")
        source    = sub_map.get(sub_id, "unknown")
        if value.get("err") or not signature:
            return
        is_new_token = False
        if source == "pump.fun" and any(PUMPFUN_INSTRUCTION in log for log in logs):
            is_new_token = True
        if not is_new_token:
            return

        def handle():
            # v14.2: is_processing gate removed entirely -- every detection
            # now spawns its own concurrent watcher thread. raw_detections
            # is still bumped unconditionally, first thing, so the heartbeat
            # count reflects everything pump.fun actually did.
            bump_scan_stat("raw_detections")

            tx_fetch_start = time.time()
            tx_data = None
            for attempt in range(6):
                time.sleep(0.4 + (attempt * 0.4))
                tx_data = rpc_call("getTransaction", [
                    signature,
                    {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0,
                     "commitment": "confirmed"}
                ])
                if tx_data:
                    break

            mint = extract_mint_from_tx(tx_data, source)
            if not mint:
                return

            # v10.5.5-hotfix: Reject stale tokens older than 60 seconds
            if tx_data:
                block_time = tx_data.get("blockTime")
                if block_time:
                    token_age = time.time() - block_time
                    if token_age > 60:
                        print(f"  [STALE] Token {mint} is {token_age:.0f}s old — rejecting")
                        with already_seen_lock:
                            already_seen.add(mint)
                        return

            creator_wallet = extract_creator_from_tx(tx_data) if tx_data else None

            # Derive curve address up front (used for the organic price observation below)
            curve_address = derive_bonding_curve(mint)
            if not curve_address:
                return

            with already_seen_lock:
                if mint in already_seen:
                    return
                already_seen.add(mint)

            # === v11.0 #6/#7: RETIRED buy/sell-count momentum gate ===
            # No more fixed observation window + hardcoded buy-count/ratio
            # thresholds. Instead we let the token trade organically for a
            # short beat, then gate purely on whether real progress has
            # happened: entry_price > detection_price (push_progress > 0).
            # The actual pass/fail call still happens inside
            # full_token_pipeline's two-axis entry gate (#5), which re-reads
            # live curve state itself -- this is just the initial detection
            # hand-off, so we no longer stream/count buys and sells here at all.
            print(f"\n  [DETECTED] {mint} -- handing off to entry pipeline")

            tx_fetch_elapsed = time.time() - tx_fetch_start
            threading.Thread(
                target=safe_full_token_pipeline,
                args=(mint, source, creator_wallet, tx_fetch_elapsed),
                daemon=True
            ).start()

        threading.Thread(target=handle, daemon=True).start()

    except Exception as e:
        pass


def on_open(ws):
    print("[OK] Main WebSocket connected\n")
    ws.send(json.dumps({
        "jsonrpc": "2.0", "id": 1,
        "method":  "logsSubscribe",
        "params":  [{"mentions": [PUMPFUN_PROGRAM]}, {"commitment": "confirmed"}]
    }))
    print("[OK] Subscribed to Pump.fun\n")
    print("  Watching for new tokens...\n")

def on_error(ws, error): pass

def on_close(ws, code, msg):
    print(f"\n  Main WS closed -- reconnecting...")
    time.sleep(3)
    start_main_ws()

def start_main_ws():
    ws = websocket.WebSocketApp(
        SOLANA_RPC_WSS,
        on_open    = on_open,
        on_message = on_message,
        on_error   = on_error,
        on_close   = on_close,
    )
    threading.Thread(
        target=ws.run_forever,
        kwargs={"ping_interval": 20, "ping_timeout": 10},
        daemon=True
    ).start()

# v14.0: position recovery removed entirely (confirmed) -- no run has ever
# actually recovered a position, and Telegram can't be read back the way
# Discord was being used as a makeshift database anyway. If the process
# restarts mid-trade, that one open position is simply lost -- acceptable
# tradeoff per explicit confirmation.

def run_bot():
    print("=" * 52)
    print("  AURIX LABS -- Solana Token Scanner v14.5 GMGN PROACTIVE RISK ENGINE")
    print("=" * 52)
    print(f"  Entry:    ${ENTRY_USD} USD")
    print(f"  TP1:      {TARGET_MULT}x | TP2: {FINAL_TARGET_MULT}x (fixed -- dynamic calibration removed)")
    print(f"  --- PRE-ENTRY (before we ever start watching trades) ---")
    print(f"  1) Creator wallet history: reject if {CREATOR_HISTORY_MIN_TOKENS_TO_JUDGE}+ prior tokens, 0 ever graduated")
    print(f"  2) GMGN holder-risk (Checkpoint A): bundler>={GMGN_BUNDLER_PCT_REJECT}% / rat_trader>={GMGN_RAT_TRADER_PCT_REJECT}% / insider>={GMGN_INSIDER_PCT_REJECT}% / suspicious>={GMGN_SUSPICIOUS_COUNT_REJECT}")
    print(f"  3) Bundled-buy detection: reject if {BUNDLE_MIN_SAME_SLOT_FOR_REJECT}+ of first {BUNDLE_CHECK_WINDOW} buys share one slot")
    print(f"  --- ENTRY GATE (dynamic, no fixed validation timeout) ---")
    print(f"  Confirms on {MIN_UNIQUE_BUYERS_HEALTHY}+ distinct buyers, price above detection, no sell concentration/bundling")
    print(f"  Also gated by: crash guard (holder %) + GMGN holder-risk recheck (Checkpoint B)")
    print(f"  --- LIVE / POST-ENTRY (GMGN_RISK_EXIT -- proactive, not reactive) ---")
    print(f"  Watches incentive-to-dump BUILDING (GMGN risk-wallet share + profit overhang), not a dump already happening")
    print(f"  Re-checks every {GMGN_LIVE_CHECK_INTERVAL_SEC:.0f}s | exits if risk share rises {GMGN_LIVE_RISK_RISE_PCT:.0f}+ points since entry, or crosses absolute thresholds")
    print(f"  Crash Guard: standalone, HALF-threshold live exit, every {HOLDER_MONITOR_INTERVAL}s")
    print(f"  TIMEOUT: 30 min max hold (safety net, unrelated to pattern reading)")
    print(f"  Completion target: {COMPLETION_SOL_TARGET:.0f} SOL raised (push progress / time-to-target)")
    print(f"  Mayhem floor: {MAYHEM_SUPPLY_FLOOR/1e15:.1f}B supply (FIXED)")
    print(f"  Alerts: Telegram -- OPENED/TP1/TP2/exit results only, rejections NOT posted")
    print(f"  Heartbeat: unconditional, every {HEARTBEAT_INTERVAL_SECONDS//60} min, full scan-stats breakdown")
    print(f"  Position recovery: REMOVED (confirmed not needed) -- a restart mid-trade loses that position")
    print(f"  Persistence: CSV/local files do NOT survive Render free-tier restarts -- Telegram is the durable log")
    print(f"  Detect:   DEXScreener WS primary + Solana RPC backup")
    print("=" * 52 + "\n")

    print("  Fetching SOL price from Pyth...")
    price = fetch_sol_price_onchain()
    if price:
        sol_price_state["usd"]        = price
        sol_price_state["last_fetch"] = time.time()
        print(f"  SOL Price: ${price:.2f}\n")
    else:
        sol_price_state["usd"]        = 180.0
        sol_price_state["last_fetch"] = time.time()
        print(f"  SOL Price: $180.00 (fallback)\n")

    threading.Thread(target=heartbeat, daemon=True).start()
    threading.Thread(target=telegram_heartbeat, daemon=True).start()

    start_main_ws()

    while True:
        time.sleep(60)

# ============ ENTRYPOINT ============
bot_thread = threading.Thread(target=run_bot, daemon=True)
bot_thread.start()

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
