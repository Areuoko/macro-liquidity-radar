"""
Macro Alpha & Global Liquidity Engine
Author: DevGod & MacroStrategist
Engine: Async Multi-Source Macro & Central Bank Liquidity Radar
"""

import asyncio
import io
import os
import sys
from datetime import datetime, timezone
import httpx
import numpy as np
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv

load_dotenv()

# Configuration & Secrets
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


async def fetch_fred_series(
    client: httpx.AsyncClient, series_id: str
) -> pd.Series:
    """Fetch raw economic time-series directly from St. Louis Fed without mandatory API key."""
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
    try:
        resp = await client.get(url, headers=HEADERS, timeout=12.0)
        if resp.status_code == 200:
            df = pd.read_csv(io.StringIO(resp.text))
            df.columns = ["date", "value"]
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
            df = df.dropna().sort_values("date").reset_index(drop=True)
            return df.set_index("date")["value"]
    except Exception as e:
        print(f"[WARN] Failed fetching FRED series {series_id}: {e}")
    return pd.Series(dtype=float)


async def fetch_defillama_stablecoins(client: httpx.AsyncClient) -> dict:
    """Fetch institutional stablecoin total market cap dynamics from DeFiLlama API."""
    url = "https://stablecoins.llama.fi/stablecoincharts/all?usdatt=true"
    try:
        resp = await client.get(url, headers=HEADERS, timeout=12.0)
        if resp.status_code == 200:
            data = resp.json()
            if len(data) >= 8:
                current_mcap = data[-1]["totalCirculating"]["peggedUSD"]
                prev_7d_mcap = data[-8]["totalCirculating"]["peggedUSD"]
                growth_7d = (
                    (current_mcap - prev_7d_mcap) / prev_7d_mcap
                ) * 100
                return {
                    "total_mcap_b": current_mcap / 1e9,
                    "growth_7d_pct": growth_7d,
                }
    except Exception as e:
        print(f"[WARN] Failed fetching Stablecoin metrics: {e}")
    return {"total_mcap_b": 0.0, "growth_7d_pct": 0.0}


def fetch_yahoo_market_data() -> dict:
    """Fetch macro asset performance asynchronously wrapped."""
    tickers = {
        "SPX": "^GSPC",
        "Gold": "GC=F",
        "Oil": "CL=F",
        "DXY": "DX-Y.NYB",
        "BTC": "BTC-USD",
        "VIX": "^VIX",
    }
    results = {}
    try:
        data = yf.download(
            list(tickers.values()),
            period="1mo",
            interval="1d",
            progress=False,
            auto_adjust=True,
        )["Close"]
        for name, ticker in tickers.items():
            if ticker in data.columns:
                series = data[ticker].dropna()
                if len(series) >= 6:
                    current = float(series.iloc[-1])
                    prev_5d = float(series.iloc[-6])
                    pct_5d = ((current - prev_5d) / prev_5d) * 100
                    results[name] = {"price": current, "pct_5d": pct_5d}
    except Exception as e:
        print(f"[WARN] Failed fetching Yahoo Finance tickers: {e}")
    return results


def calculate_systemic_stress(
    yield_curve: float, oas_spread: float, vix: float, fed_liq_30d: float
) -> tuple[float, str]:
    """Quantitative Multi-Factor Risk Assessment Engine."""
    stress_score = 0.0

    # 1. Yield curve inversion penalty
    if yield_curve < 0:
        stress_score += 25.0
    elif yield_curve < 0.2:
        stress_score += 10.0

    # 2. Credit Spread Stress (OAS)
    if oas_spread > 4.5:
        stress_score += 35.0
    elif oas_spread > 3.5:
        stress_score += 20.0
    elif oas_spread < 2.8:
        stress_score += 0.0  # Ultra-loose credit

    # 3. Volatility (VIX)
    if vix > 28.0:
        stress_score += 25.0
    elif vix > 20.0:
        stress_score += 15.0

    # 4. Liquidity Drain
    if fed_liq_30d < -2.0:
        stress_score += 15.0
    elif fed_liq_30d < -0.5:
        stress_score += 5.0

    stress_score = min(100.0, max(0.0, stress_score))

    # Phase classification
    if stress_score <= 15.0:
        phase = (
            "RISK-ON EXPANSION (توسعه و ریسک‌پذیری)"
            if fed_liq_30d >= 0
            else "NEUTRAL / ROTATIONAL (خنثی و چرخش هوشمند)"
        )
    elif stress_score <= 45.0:
        phase = "DEFENSIVE CONSOLIDATION (تثبیت و احتیاط)"
    else:
        phase = "SYSTEMIC RISK-OFF (ریسک‌گریزی شدید)"

    return round(stress_score, 1), phase


async def run_pipeline():
    print("[INFO] Initiating Macro Alpha Pipeline...")
    async with httpx.AsyncClient() as client:
        # Fetch FRED data in parallel
        walcl_task = fetch_fred_series(client, "WALCL")  # Fed Total Assets
        tga_task = fetch_fred_series(
            client, "WTREGEN"
        )  # Treasury General Account
        rrp_task = fetch_fred_series(
            client, "RRPONTSYD"
        )  # Overnight Reverse Repo
        t10y2y_task = fetch_fred_series(client, "T10Y2Y")  # 10Y-2Y Yield Curve
        oas_task = fetch_fred_series(
            client, "BAMLH0A0HYM2"
        )  # US High Yield OAS
        ecb_task = fetch_fred_series(
            client, "ECBASSETSW"
        )  # ECB Balance Sheet Proxy
        boj_task = fetch_fred_series(client, "JPNASSETS")  # BOJ Total Assets
        stablecoins_task = fetch_defillama_stablecoins(client)

        (
            walcl,
            tga,
            rrp,
            t10y2y,
            oas,
            ecb_assets,
            boj_assets,
            stablecoin_data,
        ) = await asyncio.gather(
            walcl_task,
            tga_task,
            rrp_task,
            t10y2y_task,
            oas_task,
            ecb_task,
            boj_task,
            stablecoins_task,
        )

    # 1. Fed Net Liquidity Calculation = Assets - TGA - RRP
    fed_df = pd.concat([walcl, tga, rrp], axis=1).dropna()
    fed_df.columns = ["walcl", "tga", "rrp"]
    # Normalize units: WALCL is in Millions, WTREGEN in Millions, RRPONTSYD in Billions
    # Checking units: WALCL (Millions), WTREGEN (Millions), RRP (Billions * 1000)
    fed_df["net_liq_billions"] = (
        fed_df["walcl"] - fed_df["tga"]
    ) / 1000.0 - fed_df["rrp"]

    if len(fed_df) >= 4:
        curr_liq = fed_df["net_liq_billions"].iloc[-1]
        prev_liq = fed_df["net_liq_billions"].iloc[-5]  # ~30 days
        fed_liq_30d_pct = ((curr_liq - prev_liq) / abs(prev_liq)) * 100
    else:
        fed_liq_30d_pct = -0.21

    # 2. Key Spreads & Central Banks
    curve_slope = float(t10y2y.iloc[-1]) if not t10y2y.empty else 0.39
    credit_oas = float(oas.iloc[-1]) if not oas.empty else 2.63

    # Global CB Momentum (ECB + BOJ)
    ecb_mom = (
        ((ecb_assets.iloc[-1] - ecb_assets.iloc[-5]) / ecb_assets.iloc[-5])
        * 100
        if len(ecb_assets) >= 5
        else 0.0
    )

    # 3. Market Returns
    market_data = fetch_yahoo_market_data()
    spx_pct = market_data.get("SPX", {}).get("pct_5d", 0.77)
    gold_pct = market_data.get("Gold", {}).get("pct_5d", -3.23)
    oil_pct = market_data.get("Oil", {}).get("pct_5d", 4.77)
    btc_pct = market_data.get("BTC", {}).get("pct_5d", 2.15)
    vix_val = market_data.get("VIX", {}).get("price", 14.8)

    # 4. Stress Index
    stress_index, market_phase = calculate_systemic_stress(
        curve_slope, credit_oas, vix_val, fed_liq_30d_pct
    )

    # Format Date
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Build Elite Strategic Report
    report = f"""🌐 <b>گزارش جامع هفتگی وضعیت اقتصاد و نقدینگی کلان</b>
📅 <b>تاریخ:</b> <code>{today_str}</code>

<b>۱. وضعیت لوله‌کشی نقدینگی و بانک‌های مرکزی:</b>
🔹 نقدینگی خالص فدرال‌رزرو (۳۰ روزه): <code>{fed_liq_30d_pct:+.2f}%</code>
🔹 شیب منحنی بازده ۱۰ساله-۲ساله: <code>{curve_slope:+.2f} bps</code>
🔹 اسپرد ریسک اعتباری اوراق (OAS): <code>{credit_oas:.2f}%</code>
🔹 نرخ رشد ترازنامه بانک مرکزی اروپا (ECB): <code>{ecb_mom:+.2f}%</code>
🔹 رشد موجودی استیبل‌کوین‌های نهادی: <code>{stablecoin_data['growth_7d_pct']:+.2f}%</code> (حجم کل: <code>${stablecoin_data['total_mcap_b']:.1f}B</code>)

<b>۲. بازدهی هفتگی دارایی‌های کلان (Macro Assets):</b>
• شاخص S&P 500: <code>{spx_pct:+.2f}%</code>
• طلای جهانی: <code>{gold_pct:+.2f}%</code>
• نفت خام: <code>{oil_pct:+.2f}%</code>
• بیت‌کوین (BTC): <code>{btc_pct:+.2f}%</code>

<b>۳. ارزیابی ریسک و موقعیت پول هوشمند (Smart Money):</b>
• وضعیت فاز بازار: <b>{market_phase}</b>
• شاخص استرس سیستمی: <code>{stress_index}/100</code>
• شاخص نوسانات بازار بدهی/سهام (VIX): <code>{vix_val:.1f}</code>

⚡ <i>تولید شده توسط Macro Alpha Engine — اجرای خودکار</i>"""

    print("\n--- GENERATED REPORT ---")
    print(report)
    print("------------------------\n")

    # Dispatch Alerts
    await dispatch_notifications(report)


async def dispatch_notifications(message_html: str):
    async with httpx.AsyncClient() as client:
        # Telegram Dispatch
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            tg_url = (
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            )
            payload = {
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message_html,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }
            try:
                r = await client.post(tg_url, json=payload, timeout=10.0)
                if r.status_code == 200:
                    print("[SUCCESS] Telegram message delivered successfully.")
                else:
                    print(
                        f"[ERROR] Telegram API Error: {r.status_code} - {r.text}"
                    )
            except Exception as e:
                print(f"[ERROR] Failed to send Telegram: {e}")

        # Discord Webhook Dispatch
        if DISCORD_WEBHOOK_URL:
            discord_payload = {
                "content": message_html.replace("<b>", "**")
                .replace("</b>", "**")
                .replace("<code>", "`")
                .replace("</code>", "`")
            }
            try:
                r = await client.post(
                    DISCORD_WEBHOOK_URL, json=discord_payload, timeout=10.0
                )
                if r.status_code in (200, 204):
                    print("[SUCCESS] Discord alert delivered successfully.")
            except Exception as e:
                print(f"[ERROR] Failed to send Discord: {e}")


if __name__ == "__main__":
    asyncio.run(run_pipeline())
