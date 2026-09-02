"""
Macro Alpha & Global Liquidity Engine (fixed)
Author: DevGod & MacroStrategist
Engine: Async Multi-Source Macro & Central Bank Liquidity Radar

اصلاحات نسبت به نسخه‌ی قبلی:
  1) دیگر هیچ عددِ ثابتِ باورپذیر به‌عنوان «داده‌ی زنده» نمایش داده نمی‌شود.
     اگر منبع اصلی شکست بخورد، به آخرین مقدارِ *واقعیِ* ذخیره‌شده در کش
     برمی‌گردیم و آن را با برچسبِ صریحِ «کش <تاریخ>» نشان می‌دهیم؛ اگر کشی هم
     نبود، مقدار به‌صورت N/A نمایش داده می‌شود — هیچ‌وقت جعلی به‌جای واقعی نه.
  2) هر سری، پیش از استفاده، بررسیِ «تازگی» می‌شود (STALE_AFTER_DAYS)؛ اگر
     آخرین نقطه‌ی داده خیلی قدیمی باشد (باگ قبلی VIX که یک هفته کهنه بود)،
     به‌عنوان شکست در نظر گرفته می‌شود، نه به‌عنوان مقدار «جاری».
  3) شیب منحنی ۱۰-۲ ساله دیگر با واحد غلط «bps» نمایش داده نمی‌شود
     (T10Y2Y از قبل به درصد است، نه بیسیس‌پوینت).
  4) هر نماد یاهو جداگانه دانلود می‌شود (نه یک‌جا/batch) تا عدم تطابقِ
     تقویم معاملاتی بین دارایی‌های ۵روزه (سهام/طلا/نفت) و ۷روزه (BTC) باعث
     جابه‌جاییِ ایندکسِ «آخرین روز» نشود.
  5) اگر هر یک از متریک‌های حیاتی گزارش N/A یا کش باشد، این موضوع صریحاً
     در پاورقیِ گزارش اعلام می‌شود.
  6) fetch از FRED دیگر از اسکرپِ fredgraph.csv انجام نمی‌شود (آن endpoint
     از IPهای دیتاسنتری/گیت‌هاب‌اکشنز به‌طور مکرر با تایم‌اوتِ بی‌پیام شکست
     می‌خورد) و به‌جایش از API رسمی FRED با کلید (متغیر محیطی FRED_API_KEY)
     استفاده می‌شود.
  7) WALCL و ECBASSETSW سری‌هایی هفتگی‌اند (به‌ترتیب چهارشنبه‌ها و جمعه‌ها
     منتشر می‌شوند)، پس آستانه‌ی تازگی جداگانه‌ای (WEEKLY_STALE_AFTER_DAYS)
     برایشان در نظر گرفته شده تا با آستانه‌ی سری‌های روزانه اشتباه گرفته
     نشوند و به‌غلط «کهنه» علامت نخورند.

فاز ۱ — تکمیل پوشش بانک‌های مرکزی (PBoC / BoJ / DXY):
  8) BoJ: از سری رسمی و زنده‌ی FRED به‌نام JPNASSETS استفاده می‌شود
     (ماهانه، واحد ۱۰۰ میلیون ین). چون ماهانه است، آستانه‌ی تازگی جداگانه‌ای
     (MONTHLY_STALE_AFTER_DAYS) دارد؛ نباید با آستانه‌ی هفتگی/روزانه قاطی شود.
  9) PBoC: هیچ سری معتبر و پرفرکانسی برای «کل دارایی‌های ترازنامه»ی PBoC
     روی FRED پیدا نشد (برخلاف Fed/ECB/BoJ، چین در این سطح از جزئیات
     پوشش داده نمی‌شود). گزینه‌ی اول (M2 چین، MYAGM2CNM189N) در عمل روی
     GitHub Actions با «empty observations array» شکست خورد — یعنی این سری
     IMF-محور اصلاً در پنجره‌ی ۱۲۰روزه‌ی fetch داده‌ای نداشت (تأخیر انتشار
     چند ماهه). به‌جایش از ذخایر ارزی چین (TRESEGCNM052N، ماهانه، IMF)
     استفاده می‌شود که در عمل تأییدشده و تازه است (تأخیر ~۶۳ روزه، مشابه
     BoJ). این هم صراحتاً در نام متریک و گزارش به‌عنوان «پراکسی» برچسب
     می‌خورد، نه «ترازنامه‌ی PBoC».
 10) DXY: از تیکر یاهو DX-Y.NYB استفاده می‌شود، دقیقاً هم‌الگو با
     SPX/Gold/Oil/BTC (fetch جداگانه، بدون منبع جدید).
"""

import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv

load_dotenv()

# --------------------------------------------------------------------------- #
# پیکربندی و کلیدها
# --------------------------------------------------------------------------- #
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/csv,application/csv,text/plain,*/*",
}

CACHE_PATH = Path(__file__).resolve().parent / "data_cache.json"
STALE_AFTER_DAYS = 4        # اگر آخرین نقطه‌ی داده کهنه‌تر از این بود => شکست فرض شود
WEEKLY_STALE_AFTER_DAYS = 10 # برای سری‌های هفتگی (WALCL چهارشنبه‌ها، ECBASSETSW جمعه‌ها)
MONTHLY_STALE_AFTER_DAYS = 75 # برای سری‌های ماهانه با تأخیر انتشار (JPNASSETS، TRESEGCNM052N)
                               # این عدد در برابر اجرای واقعی GitHub Actions صحت‌سنجی شد:
                               # آخرین نقطه‌ی JPNASSETS معمولاً ~۶۳ روز از تاریخ اجرا عقب‌تره
                               # (۴۵ روز اولیه بیش‌ازحد سخت‌گیرانه بود و به‌غلط «کهنه» می‌زد)
FRED_MAX_RETRIES = 2
FRED_RETRY_BACKOFF_S = 1.5


# --------------------------------------------------------------------------- #
# کش پایدار: به‌جای «مقدار پیشِ‌فرضِ هاردکد»، آخرین مقدار واقعیِ موفق را با
# تاریخش نگه می‌داریم. توجه: روی GitHub Actions این فایل باید بین اجراها
# persist شود (نگاه کنید به قدم commit-back در ورک‌فلو).
# --------------------------------------------------------------------------- #
def load_cache() -> dict:
    if CACHE_PATH.exists():
        try:
            return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"[WARN] Cache file unreadable, starting empty: {e}")
    return {}


def save_cache(cache: dict) -> None:
    try:
        CACHE_PATH.write_text(
            json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8"
        )
    except Exception as e:
        print(f"[WARN] Failed writing cache file: {e}")


class Metric:
    """
    یک عدد به‌همراه منشاش:
      status = "live"    -> همین اجرا از منبع اصلی خوانده شد
      status = "cached"   -> منبع اصلی شکست خورد؛ آخرین مقدار موفقِ قبلی
      status = "missing"  -> نه داده‌ی زنده و نه کش قبلی موجود بود
    """

    __slots__ = ("value", "status", "as_of")

    def __init__(self, value: Optional[float], status: str, as_of: Optional[str] = None):
        self.value = value
        self.status = status
        self.as_of = as_of

    @property
    def ok(self) -> bool:
        return self.value is not None

    def fmt(self, spec: str = "+.2f", unit: str = "%") -> str:
        if self.value is None:
            return "N/A ⚠️"
        text = format(self.value, spec) + unit
        if self.status == "cached":
            text += f" (کش {self.as_of})"
        return text


def to_metric(cache: dict, key: str, result: Optional[tuple]) -> Metric:
    """result باید (value, as_of_iso) یا None باشد."""
    if result is not None:
        value, as_of = result
        cache[key] = {"value": value, "as_of": as_of}
        return Metric(value, "live", as_of)

    cached = cache.get(key)
    if cached:
        return Metric(cached["value"], "cached", cached["as_of"])

    return Metric(None, "missing")


def _is_stale(last_date, stale_after_days: int = STALE_AFTER_DAYS) -> bool:
    age_days = (datetime.now(timezone.utc).date() - pd.Timestamp(last_date).date()).days
    return age_days > stale_after_days


# --------------------------------------------------------------------------- #
# FRED — API رسمی (نه اسکرپ fredgraph.csv که از IPهای دیتاسنتری/گیت‌هاب‌اکشنز
# اغلب تایم‌اوت بی‌پیام می‌داد؛ ریشه‌ی مشکل قبلی همین بود)
# --------------------------------------------------------------------------- #
FRED_API_KEY = os.getenv("FRED_API_KEY")
FRED_API_URL = "https://api.stlouisfed.org/fred/series/observations"


async def fetch_fred_series(client: httpx.AsyncClient, series_id: str) -> pd.Series:
    """سری از API رسمی FRED (JSON)؛ در صورت شکست نهایی سری خالی برمی‌گرداند."""
    if not FRED_API_KEY:
        print(f"[WARN] FRED_API_KEY تنظیم نشده؛ fetch برای {series_id} رد شد.")
        return pd.Series(dtype=float)

    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "asc",
        "observation_start": (datetime.now(timezone.utc) - pd.Timedelta(days=120)).strftime("%Y-%m-%d"),
    }
    last_err = "unknown"
    for attempt in range(1, FRED_MAX_RETRIES + 1):
        try:
            resp = await client.get(FRED_API_URL, params=params, timeout=15.0)
            if resp.status_code == 200:
                payload = resp.json()
                obs = payload.get("observations", [])
                if obs:
                    df = pd.DataFrame(obs)[["date", "value"]]
                    df["date"] = pd.to_datetime(df["date"], errors="coerce")
                    # FRED مقادیر گم‌شده را به‌صورت رشته‌ی "." برمی‌گرداند؛
                    # to_numeric با errors="coerce" این‌ها را خودکار NaN می‌کند
                    df["value"] = pd.to_numeric(df["value"], errors="coerce")
                    df = df.dropna().sort_values("date").reset_index(drop=True)
                    if not df.empty:
                        return df.set_index("date")["value"]
                    last_err = "parsed JSON but no valid numeric observations"
                else:
                    last_err = "empty observations array"
            elif resp.status_code in (400, 403):
                # معمولاً یعنی خودِ کلید API نامعتبر/غیرفعال است — تلاش دوباره فایده ندارد
                last_err = f"HTTP {resp.status_code} (احتمالاً کلید API نامعتبر): {resp.text[:150]!r}"
                break
            else:
                last_err = f"HTTP {resp.status_code}: {resp.text[:150]!r}"
        except Exception as e:
            last_err = f"{type(e).__name__}" + (f": {e}" if str(e) else "")
        if attempt < FRED_MAX_RETRIES:
            await asyncio.sleep(FRED_RETRY_BACKOFF_S * attempt)
    print(f"[WARN] FRED fetch failed for {series_id} after {FRED_MAX_RETRIES} tries: {last_err}")
    return pd.Series(dtype=float)


def compute_fed_net_liquidity(walcl: pd.Series, tga: pd.Series, rrp: pd.Series):
    """نقدینگی خالص فدرال‌رزرو = WALCL - TGA - RRP، تغییر ۳۰روزه (٪)."""
    if walcl.empty or tga.empty or rrp.empty:
        return None
    fed_df = pd.concat([walcl, tga, rrp], axis=1).dropna()
    if len(fed_df) < 5:
        return None
    fed_df.columns = ["walcl", "tga", "rrp"]
    # واحدها: WALCL و WTREGEN میلیون دلار، RRPONTSYD میلیارد دلار
    net = (fed_df["walcl"] - fed_df["tga"]) / 1000.0 - fed_df["rrp"]
    last_date = net.index[-1]
    if _is_stale(last_date, WEEKLY_STALE_AFTER_DAYS):
        print(f"[WARN] Fed liquidity series stale (last point {last_date.date()})")
        return None
    curr, prev = net.iloc[-1], net.iloc[-5]
    if prev == 0:
        return None
    pct = ((curr - prev) / abs(prev)) * 100
    return pct, last_date.strftime("%Y-%m-%d")


def compute_last_point(series: pd.Series):
    """آخرین مقدار یک سری تک‌متغیره (برای شیب منحنی و OAS) + بررسی تازگی."""
    if series.empty:
        return None
    last_date = series.index[-1]
    if _is_stale(last_date):
        print(f"[WARN] Series stale (last point {last_date.date()})")
        return None
    return float(series.iloc[-1]), last_date.strftime("%Y-%m-%d")


def compute_fred_period_change(series: pd.Series, lookback: int, stale_after_days: int):
    """
    درصد تغییر بین آخرین نقطه و `lookback` نقطه‌ی قبل، برای سری‌های FRED با
    فرکانس غیرروزانه (هفتگی/ماهانه). آستانه‌ی تازگی جداگانه می‌گیرد چون این
    سری‌ها با تأخیرهای متفاوتی منتشر می‌شوند.
    مثال: ECBASSETSW هفتگی است => lookback=4 (۴ گام به عقب از iloc[-1] یعنی iloc[-5]؛
          ≈۱ماه، دقیقاً همان چیزی که کد قبلی fed_liq/ECB با iloc[-5] محاسبه می‌کرد)
          با WEEKLY_STALE_AFTER_DAYS
          JPNASSETS/TRESEGCNM052N ماهانه‌اند => lookback=1 (ماه به ماه) با MONTHLY_STALE_AFTER_DAYS
    """
    if len(series) <= lookback:
        return None
    last_date = series.index[-1]
    if _is_stale(last_date, stale_after_days):
        print(f"[WARN] Series stale (last point {last_date.date()})")
        return None
    curr, prev = float(series.iloc[-1]), float(series.iloc[-1 - lookback])
    if prev == 0:
        return None
    pct = ((curr - prev) / abs(prev)) * 100
    return pct, last_date.strftime("%Y-%m-%d")


def compute_pct_change_5(series: pd.Series):
    """درصد تغییر بین آخرین نقطه و ۵ نقطه‌ی قبل (≈ هفتگی برای بازارهای ۵روزه) + بررسی تازگی."""
    if len(series) <= 5:
        return None
    last_date = series.index[-1]
    if _is_stale(last_date):
        print(f"[WARN] Series stale (last point {last_date.date()})")
        return None
    curr, prev = float(series.iloc[-1]), float(series.iloc[-6])
    if prev == 0:
        return None
    pct = ((curr - prev) / abs(prev)) * 100
    return pct, last_date.strftime("%Y-%m-%d")


# --------------------------------------------------------------------------- #
# استیبل‌کوین‌ها (DeFiLlama)
# --------------------------------------------------------------------------- #
async def fetch_defillama_stablecoins(client: httpx.AsyncClient):
    """رشد ۷روزه‌ی کل بازار استیبل‌کوین + حجم فعلی. خروجی: (growth_pct, total_b, as_of) یا None."""
    url = "https://stablecoins.llama.fi/stablecoincharts/all?usdatt=true"
    try:
        resp = await client.get(url, headers=HEADERS, timeout=15.0)
        if resp.status_code != 200:
            print(f"[WARN] DeFiLlama HTTP {resp.status_code}")
            return None
        data = resp.json()
        if len(data) < 8:
            return None
        current_mcap = data[-1]["totalCirculating"]["peggedUSD"]
        prev_7d_mcap = data[-8]["totalCirculating"]["peggedUSD"]
        if prev_7d_mcap == 0:
            return None
        growth_7d = ((current_mcap - prev_7d_mcap) / prev_7d_mcap) * 100
        as_of = datetime.fromtimestamp(int(data[-1]["date"]), tz=timezone.utc).strftime("%Y-%m-%d")
        if _is_stale(as_of):
            print(f"[WARN] Stablecoin data stale (last point {as_of})")
            return None
        return growth_7d, current_mcap / 1e9, as_of
    except Exception as e:
        print(f"[WARN] Failed fetching stablecoin metrics: {e}")
        return None


# --------------------------------------------------------------------------- #
# یاهو فایننس — هر نماد جداگانه (نه batch) تا تقویم‌های معاملاتی قاطی نشوند
# --------------------------------------------------------------------------- #
def _fetch_yahoo_ticker_sync(ticker: str) -> pd.Series:
    try:
        hist = yf.Ticker(ticker).history(period="2mo", interval="1d", auto_adjust=True)
        if hist.empty:
            return pd.Series(dtype=float)
        s = hist["Close"].dropna()
        s.index = pd.to_datetime(s.index).tz_localize(None)
        return s
    except Exception as e:
        print(f"[WARN] yfinance fetch failed for {ticker}: {e}")
        return pd.Series(dtype=float)


async def fetch_all_yahoo_series(tickers: dict) -> dict:
    """دانلود موازیِ هر نماد در یک ترد جدا (yfinance sync است)."""

    async def _one(name, tk):
        series = await asyncio.to_thread(_fetch_yahoo_ticker_sync, tk)
        return name, series

    pairs = await asyncio.gather(*[_one(n, t) for n, t in tickers.items()])
    return dict(pairs)


# --------------------------------------------------------------------------- #
# شاخص استرس سیستمی — فقط وقتی همه‌ی ورودی‌های حیاتی موجودند محاسبه می‌شود
# --------------------------------------------------------------------------- #
def calculate_systemic_stress(
    yield_curve_pct: float, oas_spread: float, vix: float, fed_liq_30d_pct: float
) -> tuple[float, str]:
    stress_score = 0.0

    if yield_curve_pct < 0:
        stress_score += 25.0
    elif yield_curve_pct < 0.2:
        stress_score += 10.0

    if oas_spread > 4.5:
        stress_score += 35.0
    elif oas_spread > 3.5:
        stress_score += 20.0

    if vix > 28.0:
        stress_score += 25.0
    elif vix > 20.0:
        stress_score += 15.0

    if fed_liq_30d_pct < -2.0:
        stress_score += 15.0
    elif fed_liq_30d_pct < -0.5:
        stress_score += 5.0

    stress_score = min(100.0, max(0.0, stress_score))

    if stress_score <= 15.0:
        phase = (
            "RISK-ON EXPANSION (توسعه و ریسک‌پذیری)"
            if fed_liq_30d_pct >= 0
            else "NEUTRAL / ROTATIONAL (خنثی و چرخش هوشمند)"
        )
    elif stress_score <= 45.0:
        phase = "DEFENSIVE CONSOLIDATION (تثبیت و احتیاط)"
    else:
        phase = "SYSTEMIC RISK-OFF (ریسک‌گریزی شدید)"

    return round(stress_score, 1), phase


# --------------------------------------------------------------------------- #
# پایپ‌لاین اصلی
# --------------------------------------------------------------------------- #
async def run_pipeline():
    print("[INFO] Initiating Macro Alpha Pipeline...")
    cache = load_cache()

    async with httpx.AsyncClient() as client:
        walcl_task = fetch_fred_series(client, "WALCL")
        tga_task = fetch_fred_series(client, "WTREGEN")
        rrp_task = fetch_fred_series(client, "RRPONTSYD")
        t10y2y_task = fetch_fred_series(client, "T10Y2Y")
        oas_task = fetch_fred_series(client, "BAMLH0A0HYM2")
        ecb_task = fetch_fred_series(client, "ECBASSETSW")
        boj_task = fetch_fred_series(client, "JPNASSETS")
        china_fx_task = fetch_fred_series(client, "TRESEGCNM052N")
        stablecoins_task = fetch_defillama_stablecoins(client)

        (
            walcl, tga, rrp, t10y2y, oas, ecb_assets, boj_assets, china_fx,
            stablecoin_result,
        ) = await asyncio.gather(
            walcl_task, tga_task, rrp_task, t10y2y_task, oas_task, ecb_task,
            boj_task, china_fx_task,
            stablecoins_task,
        )

    yahoo_tickers = {
        "SPX": "^GSPC", "Gold": "GC=F", "Oil": "CL=F", "BTC": "BTC-USD", "VIX": "^VIX",
        "DXY": "DX-Y.NYB",
    }
    yahoo_series = await fetch_all_yahoo_series(yahoo_tickers)

    # ---- ساخت متریک‌ها (هر کدام: زنده / کش-با-تاریخ / N/A) ------------------
    fed_liq = to_metric(cache, "fed_liq_30d_pct", compute_fed_net_liquidity(walcl, tga, rrp))
    curve = to_metric(cache, "curve_slope_pct", compute_last_point(t10y2y))
    oas_m = to_metric(cache, "credit_oas_pct", compute_last_point(oas))

    # ECB: تغییر ۵نقطه‌ای مشابه fed_liq اما تک‌سری است (ECBASSETSW هم هفتگی است: جمعه‌ها)
    if not ecb_assets.empty and len(ecb_assets) < 5:
        print(f"[WARN] ECB series too short ({len(ecb_assets)} points)")
    ecb_m = to_metric(
        cache, "ecb_mom_pct",
        compute_fred_period_change(ecb_assets, lookback=4, stale_after_days=WEEKLY_STALE_AFTER_DAYS),
    )

    # BoJ: ماهانه (JPNASSETS)؛ تغییر ماه‌به‌ماه (lookback=1) با آستانه‌ی تازگی ماهانه
    boj_m = to_metric(
        cache, "boj_mom_pct",
        compute_fred_period_change(boj_assets, lookback=1, stale_after_days=MONTHLY_STALE_AFTER_DAYS),
    )

    # PBoC: پراکسی — رشد ذخایر ارزی چین (نه ترازنامه‌ی واقعی PBoC؛ نگاه کنید به یادداشت فاز ۱ در هدر فایل)
    china_fx_m = to_metric(
        cache, "china_fx_reserves_mom_pct",
        compute_fred_period_change(china_fx, lookback=1, stale_after_days=MONTHLY_STALE_AFTER_DAYS),
    )

    stable_growth = to_metric(
        cache, "stablecoin_growth_pct",
        (stablecoin_result[0], stablecoin_result[2]) if stablecoin_result else None,
    )
    stable_total = to_metric(
        cache, "stablecoin_total_b",
        (stablecoin_result[1], stablecoin_result[2]) if stablecoin_result else None,
    )

    spx_m = to_metric(cache, "spx_pct", compute_pct_change_5(yahoo_series.get("SPX", pd.Series(dtype=float))))
    gold_m = to_metric(cache, "gold_pct", compute_pct_change_5(yahoo_series.get("Gold", pd.Series(dtype=float))))
    oil_m = to_metric(cache, "oil_pct", compute_pct_change_5(yahoo_series.get("Oil", pd.Series(dtype=float))))
    btc_m = to_metric(cache, "btc_pct", compute_pct_change_5(yahoo_series.get("BTC", pd.Series(dtype=float))))
    vix_m = to_metric(cache, "vix_level", compute_last_point(yahoo_series.get("VIX", pd.Series(dtype=float))))
    dxy_m = to_metric(cache, "dxy_pct", compute_pct_change_5(yahoo_series.get("DXY", pd.Series(dtype=float))))

    save_cache(cache)

    # ---- استرس سیستمی: فقط اگر هر ۴ ورودی حیاتی موجودند ----------------------
    critical = [curve, oas_m, vix_m, fed_liq]
    if all(m.ok for m in critical):
        stress_index, market_phase = calculate_systemic_stress(
            curve.value, oas_m.value, vix_m.value, fed_liq.value
        )
        stress_str = f"{stress_index}/100"
    else:
        stress_str = "N/A ⚠️"
        market_phase = "قابل محاسبه نیست (داده‌ی حیاتی ناقص)"

    # ---- پاورقیِ کیفیتِ داده -------------------------------------------------
    all_metrics = {
        "نقدینگی فدرال‌رزرو": fed_liq, "شیب منحنی": curve, "OAS": oas_m,
        "ترازنامه ECB": ecb_m, "ترازنامه BoJ": boj_m, "ذخایر ارزی چین (پراکسی PBoC)": china_fx_m,
        "استیبل‌کوین (رشد)": stable_growth, "استیبل‌کوین (حجم)": stable_total,
        "S&P 500": spx_m, "طلا": gold_m, "نفت": oil_m, "BTC": btc_m, "VIX": vix_m,
        "DXY": dxy_m,
    }
    issues = [f"{name} ({m.status})" for name, m in all_metrics.items() if m.status != "live"]
    quality_note = ""
    if issues:
        quality_note = "\n\n⚠️ <b>هشدار کیفیت داده:</b> " + "، ".join(issues)

    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    curve_bps = curve.value * 100 if curve.ok else None
    curve_display = (
        f"{curve.fmt('+.2f', '%')}" + (f" (≈{curve_bps:+.0f} bps)" if curve_bps is not None else "")
        if curve.ok else curve.fmt()
    )

    report = f"""🌐 <b>گزارش جامع هفتگی وضعیت اقتصاد و نقدینگی کلان</b>
📅 <b>تاریخ:</b> <code>{today_str}</code>

<b>۱. وضعیت لوله‌کشی نقدینگی و بانک‌های مرکزی:</b>
🔹 نقدینگی خالص فدرال‌رزرو (۳۰ روزه): <code>{fed_liq.fmt()}</code>
🔹 شیب منحنی بازده ۱۰ساله-۲ساله: <code>{curve_display}</code>
🔹 اسپرد ریسک اعتباری اوراق (OAS): <code>{oas_m.fmt()}</code>
🔹 نرخ رشد ترازنامه بانک مرکزی اروپا (ECB): <code>{ecb_m.fmt()}</code>
🔹 نرخ رشد ترازنامه بانک مرکزی ژاپن (BoJ): <code>{boj_m.fmt()}</code>
🔹 رشد ذخایر ارزی چین (پراکسی نقدینگی PBoC): <code>{china_fx_m.fmt()}</code>
🔹 رشد موجودی استیبل‌کوین‌های نهادی: <code>{stable_growth.fmt()}</code> (حجم کل: <code>${stable_total.fmt('.1f', 'B')}</code>)

<b>۲. بازدهی هفتگی دارایی‌های کلان (Macro Assets):</b>
• شاخص S&P 500: <code>{spx_m.fmt()}</code>
• طلای جهانی: <code>{gold_m.fmt()}</code>
• نفت خام: <code>{oil_m.fmt()}</code>
• بیت‌کوین (BTC): <code>{btc_m.fmt()}</code>
• شاخص دلار (DXY): <code>{dxy_m.fmt()}</code>

<b>۳. ارزیابی ریسک و موقعیت پول هوشمند (Smart Money):</b>
• وضعیت فاز بازار: <b>{market_phase}</b>
• شاخص استرس سیستمی: <code>{stress_str}</code>
• شاخص نوسانات بازار بدهی/سهام (VIX): <code>{vix_m.fmt('.1f', '')}</code>

⚡ <i>تولید شده توسط Macro Alpha Engine — اجرای خودکار</i>{quality_note}"""

    print("\n--- GENERATED REPORT ---")
    print(report)
    print("------------------------\n")

    await dispatch_notifications(report)


async def dispatch_notifications(message_html: str):
    async with httpx.AsyncClient() as client:
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            tg_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
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
                    print(f"[ERROR] Telegram API Error: {r.status_code} - {r.text}")
            except Exception as e:
                print(f"[ERROR] Failed to send Telegram: {e}")

        if DISCORD_WEBHOOK_URL:
            discord_payload = {
                "content": message_html.replace("<b>", "**")
                .replace("</b>", "**")
                .replace("<code>", "`")
                .replace("</code>", "`")
            }
            try:
                r = await client.post(DISCORD_WEBHOOK_URL, json=discord_payload, timeout=10.0)
                if r.status_code in (200, 204):
                    print("[SUCCESS] Discord alert delivered successfully.")
                else:
                    print(f"[ERROR] Discord webhook error: {r.status_code} - {r.text}")
            except Exception as e:
                print(f"[ERROR] Failed to send Discord: {e}")


if __name__ == "__main__":
    asyncio.run(run_pipeline())
