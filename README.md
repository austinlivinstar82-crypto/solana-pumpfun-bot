# Solana Pump.fun Token Scanner

A Python bot that watches new token launches on pump.fun (Solana) in
real time, applies a multi-stage risk and pattern-confirmation pipeline,
and simulates entering/exiting trades based on that analysis.

> **Disclaimer:** Built for research and educational purposes. This is
> not financial advice. Cryptocurrency trading carries substantial risk,
> and automated trading strategies can lose money. Use at your own risk.

## Features
- **Real-Time Detection:** Subscribes to Solana RPC logs to catch new
  pump.fun token launches as they happen.
- **Crash Guard:** Calculates, from live bonding-curve state, how much
  selling would be needed to crash a token back to its detection price,
  and rejects or exits positions that cross that threshold.
- **Pattern Confirmation:** Watches each token's own real trade flow
  (distinct buyers, sell concentration, buy bundling) before entering,
  rather than acting on a fixed timer.
- **Creator & Holder Risk Checks:** Screens the token creator's past
  launches and GMGN-tagged holder risk (bundlers, rat traders, insiders)
  before and during a trade.
- **Defensive Design:** Every RPC call, holder snapshot, and price
  calculation is wrapped in error handling so one bad token can't crash
  the scanner.
- **Telegram Alerts & Heartbeat:** Posts trade opens/closes and a
  periodic status summary to a Telegram chat.

## Tech Stack
- Python 3.x
- `websocket-client`, `requests`, `solders`, `flask`
- Solana RPC (Helius), Telegram Bot API, GMGN CLI

## Project Structure
```text
solana-pumpfun-bot/
├── bot.py          # Main scanner, pipeline, and monitoring logic
└── .gitignore
```

## Getting Started

### Prerequisites
Python 3.8 or higher

### Installation
```bash
pip install requests websocket-client base58 solders flask
```

### Configuration
This bot reads its secrets from environment variables, never from the
code. Set the following before running:
```bash
SOLANA_RPC_HTTP=your_helius_http_url
SOLANA_RPC_WSS=your_helius_wss_url
TELEGRAM_BOT_TOKEN=your_telegram_bot_token
TELEGRAM_CHAT_ID=your_telegram_chat_id
GMGN_API_KEY=your_gmgn_api_key
```

### Usage
```bash
python bot.py
```
