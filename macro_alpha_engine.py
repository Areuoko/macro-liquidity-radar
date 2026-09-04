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
     استفاده می‌شود که fetch آن موفق است، اما تأخیر انتشارش از BoJ هم
     بیشتر است (در عمل ۹۳ روز دیده شد)، پس آستانه‌ی تازگی جداگانه‌ای
     (CHINA_FX_STALE_AFTER_DAYS) دارد و نباید با آستانه‌ی BoJ قاطی شود.
     این هم صراحتاً در نام متریک و گزارش به‌عنوان «پراکسی» برچسب می‌خورد،
     نه «ترازنامه‌ی PBoC».
 10) DXY: از تیکر یاهو DX-Y.NYB استفاده می‌شود، دقیقاً هم‌الگو با
     SPX/Gold/Oil/BTC (fetch جداگانه، بدون منبع جدید).

فاز ۲ — استرس اعتباری عمیق‌تر (IG OAS / SOFR-EFFR):
 11) IG OAS: سری BAMLC0A0CM (ICE BofA US Corporate Index OAS) از FRED،
     دقیقاً هم‌الگو با HY OAS موجود (compute_last_point، آستانه‌ی روزانه).
 12) SOFR-EFFR: تابع جدید compute_spread_last_point_bps دو سری روزانه‌ی
     SOFR و EFFR را روی تاریخ‌های مشترک تراز می‌کند و اسپرد را به bps
     تبدیل می‌کند (هر دو سری از FRED به‌صورت درصد منتشر می‌شوند).
 13) هر دو متریک فعلاً فقط نمایشی‌اند — طبق تصمیم صریح، وارد فرمول
     calculate_systemic_stress نمی‌شوند؛ بازطراحی آن فرمول برای فاز ۵
     نگه داشته شده تا با rotation score ترکیب شود.

فاز ۳ — سیگنال‌های rotation واقعی (Growth/Value، Cyclical/Defensive، Small/Large):
 14) هر سه جفت با ETF واقعی مقایسه می‌شوند نه ایندکس (IWM÷SPY، نه IWM÷^GSPC)
     چون نسبت باید بین دو دارایی معامله‌پذیرِ هم‌ساختار باشد.
 15) تابع عمومی compute_ratio_series سری نسبت a/b را روی تاریخ‌های مشترک
     می‌سازد؛ سپس با همان compute_last_point (سطح فعلی) و compute_pct_change_5
     (مومنتوم ۵روزه) که برای SPX/Gold/... استفاده می‌شوند دوباره‌استفاده
     می‌شود — بدون منطق تازگی/محاسبه‌ی جدید.
 16) طبق تصمیم، هم سطح فعلی و هم درصد تغییر ۵روزه نمایش داده می‌شوند؛
     این‌ها هم فعلاً فقط نمایشی‌اند (نه ورودی stress_score) — همان اصل فاز ۲.

فاز ۴ — جریان پول هوشمند (CFTC COT؛ ETF flows موکول به بعد):
 17) طبق تصمیم صریح، فقط CFTC COT پیاده‌سازی شد. ETF flows چون منبع
     رایگان و دقیقی ندارد (طبق یادداشتِ خودِ روندمپ)، عمداً کنار گذاشته
     شد تا در یک تصمیم/چت جداگانه دوباره بررسی شود.
 18) منبع: API رسمی و رایگان Socrata دولت آمریکا در publicreporting.cftc.gov
     (دیتاست TFF Futures Only، شناسه gpe5-46if) — نیازی به کلید API ندارد.
 19) دسته‌ی «Leveraged Funds» (هج‌فاندها/سفته‌بازهای حرفه‌ای) به‌عنوان
     پراکسیِ «پول هوشمند» انتخاب شد (نه Asset Manager/Dealer) چون این دسته
     رایج‌ترین معیار «پوزیشن‌گیری سفته‌بازی» در تفسیر macro است.
 20) دو بازار پوشش داده شد: S&P 500 Consolidated (کد ۱۳۸۷۴+، سهام) و
     USD Index (کد ۰۹۸۶۶۲، دلار) — هر دو دارایی‌ای که این موتور از قبل
     بازدهی قیمتی‌شان را نمایش می‌دهد، پس این‌جا cross-check «پوزیشن در
     برابر قیمت» فراهم می‌شود.
 21) چون COT هفته‌ای فقط یک نقطه منتشر می‌شود (نه روزانه)، از تابع جدید
     compute_period_change_raw استفاده شد (تغییرِ خامِ کانترکت بین آخرین
     دو هفته)، نه compute_pct_change_5 یا compute_fred_period_change —
     چون خالص پوزیشن می‌تواند از مثبت به منفی رد شود و آن‌جا درصد بی‌معنی
     می‌شود؛ تغییرِ خام همان قراردادِ استاندارد گزارش‌های COT بازار است.
 22) آستانه‌ی تازگیِ جداگانه (COT_STALE_AFTER_DAYS=10) — همان اصل فاز ۱:
     یک آستانه‌ی سراسری برای فرکانس‌های مختلف باعث data gap کاذب می‌شود.
 23) این دو متریک هم فعلاً فقط نمایشی‌اند (نه ورودی stress_score) —
     بازطراحی نهایی با rotation score برای فاز ۵ نگه داشته شده.

فاز ۵ — بازطراحیِ نهاییِ امتیازدهی (دو امتیازِ جدا: stress + rotation):
 24) طبق تصمیم صریح، stress_score همه‌ی متریک‌های جدید (IG OAS، SOFR-EFFR،
     COT) را وزن می‌کند. ۴ ورودیِ اصلیِ قدیمی هم‌چنان «حیاتی»‌اند (غیبتِ
     هرکدام = کل امتیاز N/A، دقیقاً مثل قبل)؛ ۳ ورودیِ جدید «اختیاری»‌اند —
     غیبتشان فقط سهمِ خودشان را صفر می‌کند، نه کلِ امتیاز، تا تاب‌آوریِ
     امتیازِ اصلی به هفت منبع به‌جای چهار منبع وابسته نشود.
 25) وزن‌های ۴ ورودیِ اصلی متناسب کم شدند (حداکثر همچنان ۱۰۰) تا جا برای
     IG OAS/SOFR-EFFR/COT باز شود؛ جزئیاتِ کاملِ وزن‌ها در docstring خودِ
     calculate_systemic_stress است.
 26) COT فقط از طریق تغییرِ هفتگیِ Leveraged Funds در S&P 500 وارد شد (نه
     USD Index) — بودجه‌ی وزنِ COT کم است و دی‌ریسکینگِ سهام سیگنالِ
     مستقیم‌تری برای «استرس» است؛ پوزیشنِ دلار بیشتر ماهیتِ rotation دارد.
 27) rotation_score طبق رودمپ کاملاً جدا از stress_score است — تابعِ جدیدِ
     calculate_rotation_score، میانگینِ سه مومنتومِ ۵روزه‌ی فاز ۳ (از قبل
     هم‌مقیاس، پس نیازی به آستانه/وزنِ دستی مثلِ stress_score نیست).
 28) هر دو امتیاز اکنون در بخشِ «۵. ارزیابی ریسک» گزارش نمایش داده می‌شوند.
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
MONTHLY_STALE_AFTER_DAYS = 75 # برای سری‌های ماهانه با تأخیر انتشار معمول (JPNASSETS)
                               # این عدد در برابر اجرای واقعی GitHub Actions صحت‌سنجی شد:
                               # آخرین نقطه‌ی JPNASSETS معمولاً ~۶۳ روز از تاریخ اجرا عقب‌تره
                               # (۴۵ روز اولیه بیش‌ازحد سخت‌گیرانه بود و به‌غلط «کهنه» می‌زد)
CHINA_FX_STALE_AFTER_DAYS = 120 # TRESEGCNM052N (ذخایر ارزی چین) از BoJ هم عقب‌تر منتشر می‌شود؛
                                 # در عمل روی Actions آخرین نقطه ۹۳ روز عقب‌تر دیده شد، پس آستانه‌ی
                                 # جدا و شل‌تری لازم داشت — طبق همان اصل «یک آستانه‌ی سراسری باعث
                                 # data gap می‌شود» که برای WALCL/ECBASSETSW هم رعایت شده بود
COT_STALE_AFTER_DAYS = 10       # CFTC COT هفته‌ای یک‌بار جمعه‌ها منتشر می‌شود (داده‌ی سه‌شنبه‌ی قبل)؛
                                 # اجرای دوشنبه‌صبح یعنی حداکثر ~۳ روز عقب‌تر در حالت عادی، اما با
                                 # تعطیلات/تأخیر انتشار به همان الگوی WEEKLY_STALE_AFTER_DAYS=10 نیاز دارد
FRED_MAX_RETRIES = 2
FRED_RETRY_BACKOFF_S = 1.5

CFTC_TFF_API_URL = "https://publicreporting.cftc.gov/resource/gpe5-46if.json"  # TFF Futures Only (رسمی، Socrata)
CFTC_SP500_CODE = "13874+"      # CME S&P 500 Consolidated (استاندارد + E-mini + Micro، تجمیع‌شده)
CFTC_USD_INDEX_CODE = "098662"  # ICE U.S. Dollar Index (USDX)


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


def compute_last_point(series: pd.Series, stale_after_days: int = STALE_AFTER_DAYS):
    """آخرین مقدار یک سری تک‌متغیره (برای شیب منحنی و OAS) + بررسی تازگی."""
    if series.empty:
        return None
    last_date = series.index[-1]
    if _is_stale(last_date, stale_after_days):
        print(f"[WARN] Series stale (last point {last_date.date()})")
        return None
    return float(series.iloc[-1]), last_date.strftime("%Y-%m-%d")


def compute_period_change_raw(series: pd.Series, lookback: int = 1, stale_after_days: int = STALE_AFTER_DAYS):
    """
    تغییرِ خام (نه ٪) بین آخرین نقطه و `lookback` نقطه‌ی قبل. برخلاف
    compute_fred_period_change که درصد برمی‌گرداند و prev==0 را رد می‌کند،
    این تابع برای سری‌هایی مثل خالص پوزیشن COT است که می‌توانند از مثبت
    به منفی رد شوند — آنجا درصدِ تغییر بی‌معنی/گمراه‌کننده است، اما تغییرِ
    خامِ کانترکت (مثلاً «+۵۲۰۰ کانترکت») همان قراردادِ استاندارد بازار است.
    """
    if len(series) <= lookback:
        return None
    last_date = series.index[-1]
    if _is_stale(last_date, stale_after_days):
        print(f"[WARN] Series stale (last point {last_date.date()})")
        return None
    curr = float(series.iloc[-1])
    prev = float(series.iloc[-1 - lookback])
    return curr - prev, last_date.strftime("%Y-%m-%d")


def compute_spread_last_point_bps(series_a: pd.Series, series_b: pd.Series):
    """
    آخرین اسپرد (a - b) بین دو سری هم‌فرکانس روزانه، بر حسب بیسیس‌پوینت،
    با تراز کردن روی تاریخ‌های مشترک (برای SOFR - EFFR).
    هر دو سری FRED از قبل به‌صورت درصد منتشر می‌شوند، پس ضرب در ۱۰۰ => bps.
    """
    if series_a.empty or series_b.empty:
        return None
    df = pd.concat([series_a, series_b], axis=1).dropna()
    if df.empty:
        return None
    df.columns = ["a", "b"]
    last_date = df.index[-1]
    if _is_stale(last_date):
        print(f"[WARN] Spread series stale (last point {last_date.date()})")
        return None
    spread_bps = (float(df["a"].iloc[-1]) - float(df["b"].iloc[-1])) * 100.0
    return spread_bps, last_date.strftime("%Y-%m-%d")


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


def compute_ratio_series(series_a: pd.Series, series_b: pd.Series) -> pd.Series:
    """
    سری نسبت a/b روی تاریخ‌های مشترک دو سری (برای سیگنال‌های rotation:
    Growth/Value، Cyclical/Defensive، Small/Large). خروجی یک سری معمولی
    است که با همان compute_last_point (سطح فعلی) و compute_pct_change_5
    (مومنتوم ۵روزه) که برای بقیه‌ی دارایی‌ها استفاده می‌شوند، سازگار است.
    """
    if series_a.empty or series_b.empty:
        return pd.Series(dtype=float)
    df = pd.concat([series_a, series_b], axis=1).dropna()
    if df.empty:
        return pd.Series(dtype=float)
    df.columns = ["a", "b"]
    return df["a"] / df["b"]


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
# فاز ۴ — CFTC COT (Traders in Financial Futures): موقعیت‌گیری صندوق‌های
# اهرمی (Leveraged Funds ≈ هج‌فاندها/سفته‌بازهای حرفه‌ای) به‌عنوان پراکسیِ
# «پول هوشمند». منبع رسمی و رایگان (Socrata API دولتی)، هفته‌ای یک‌بار
# جمعه‌ها منتشر می‌شود؛ ETF flows (بخش دوم فاز ۴) چون منبع رایگان و دقیقی
# ندارد، فعلاً عمداً اضافه نشده — طبق تصمیم صریح، به بعد موکول شده.
# --------------------------------------------------------------------------- #
async def fetch_cftc_tff_net_leveraged(client: httpx.AsyncClient, contract_code: str) -> pd.Series:
    """
    خالص پوزیشنِ Leveraged Funds (long - short) برای یک قرارداد مشخص، از
    گزارش هفتگی TFF-Futures-Only (دیتاست رسمی gpe5-46if). چند هفته‌ی اخیر
    گرفته می‌شود (نه فقط آخرین) تا محاسبه‌ی تغییر هفته‌به‌هفته هم ممکن باشد.
    """
    params = {
        "$where": f"cftc_contract_market_code='{contract_code}'",
        "$select": "report_date_as_yyyy_mm_dd,lev_money_positions_long,lev_money_positions_short",
        "$order": "report_date_as_yyyy_mm_dd DESC",
        "$limit": "10",
    }
    last_err = "unknown"
    for attempt in range(1, FRED_MAX_RETRIES + 1):
        try:
            resp = await client.get(CFTC_TFF_API_URL, params=params, timeout=15.0)
            if resp.status_code == 200:
                rows = resp.json()
                if not rows:
                    last_err = "empty result set"
                else:
                    df = pd.DataFrame(rows)
                    df["report_date_as_yyyy_mm_dd"] = pd.to_datetime(
                        df["report_date_as_yyyy_mm_dd"], errors="coerce"
                    )
                    df["lev_money_positions_long"] = pd.to_numeric(
                        df["lev_money_positions_long"], errors="coerce"
                    )
                    df["lev_money_positions_short"] = pd.to_numeric(
                        df["lev_money_positions_short"], errors="coerce"
                    )
                    df = df.dropna().sort_values("report_date_as_yyyy_mm_dd").reset_index(drop=True)
                    if not df.empty:
                        net = df["lev_money_positions_long"] - df["lev_money_positions_short"]
                        net.index = df["report_date_as_yyyy_mm_dd"]
                        return net
                    last_err = "parsed JSON but no valid numeric rows"
            else:
                last_err = f"HTTP {resp.status_code}: {resp.text[:150]!r}"
        except Exception as e:
            last_err = f"{type(e).__name__}" + (f": {e}" if str(e) else "")
        if attempt < FRED_MAX_RETRIES:
            await asyncio.sleep(FRED_RETRY_BACKOFF_S * attempt)
    print(f"[WARN] CFTC TFF fetch failed for contract {contract_code} after {FRED_MAX_RETRIES} tries: {last_err}")
    return pd.Series(dtype=float)


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
    """
    دانلود ترتیبیِ هر نماد در یک ترد جدا (yfinance sync است).
    عمداً موازی نیست: yfinance داخلی‌اش یک کش sqlite مشترک (برای timezone) دارد
    و وقتی چند ترد هم‌زمان بهش می‌نویسند، خطای «database is locked» می‌دهد —
    این باگ واقعی روی GitHub Actions دیده شد (بعد از اضافه‌شدن DXY به ۶ تیکر
    احتمالش بیشتر شد). چون این پایپ‌لاین هفتگی است و به سرعت حساس نیست،
    ترتیبی‌بودن (چند ثانیه کندتر) کاملاً قابل‌قبول است و ریسک قفل را از ریشه
    حذف می‌کند.
    """
    result = {}
    for name, tk in tickers.items():
        result[name] = await asyncio.to_thread(_fetch_yahoo_ticker_sync, tk)
    return result


# --------------------------------------------------------------------------- #
# فاز ۵ — دو امتیاز جدا: استرس سیستمی (۴ ورودی حیاتی + ۳ ورودی اختیاری) و
# rotation (جدا، از سه نسبت فاز ۳). جزئیات هرکدام در docstring خودشان.
# --------------------------------------------------------------------------- #
def calculate_systemic_stress(
    yield_curve_pct: float,
    oas_spread: float,
    vix: float,
    fed_liq_30d_pct: float,
    ig_oas_pct: Optional[float] = None,
    sofr_effr_bps: Optional[float] = None,
    cot_spx_wow_change: Optional[float] = None,
) -> tuple[float, str]:
    """
    فاز ۵ — بازطراحی: طبق تصمیم صریح، سه ورودیِ جدیدِ فاز ۲/۴ هم وزن می‌شوند:
    IG OAS، اسپرد SOFR-EFFR، و تغییرِ هفتگیِ خالص پوزیشنِ Leveraged Funds در
    S&P 500 (از CFTC COT). USD Index COT عمداً وارد نشد — بودجه‌ی وزنِ COT کم
    است (۵ امتیاز) و دی‌ریسکینگِ سهام سیگنالِ مستقیم‌تری برای «استرس» است؛
    پوزیشنِ دلار بیشتر ماهیتِ rotation دارد (در rotation_score دیده می‌شود).

    ۴ ورودیِ اصلیِ قبلی هم‌چنان "حیاتی"‌اند — در سطحِ فراخوان، اگر هرکدام
    غایب باشد کلِ امتیاز N/A می‌شود (دقیقاً مثل قبل). اما ۳ ورودیِ جدید
    "اختیاری"‌اند (پیش‌فرض None): اگر غایب باشند فقط سهمِ خودشان صفر می‌شود،
    نه کلِ امتیاز — چون این‌ها تصفیه‌کننده‌ی سیگنالِ اصلی‌اند نه پایه‌ی آن؛
    وابسته‌کردنِ کلِ امتیاز به هر ۷ منبع، تاب‌آوری را بی‌دلیل کم می‌کرد.
    غیبتِ هرکدام هم‌چنان جداگانه در پاورقیِ کیفیتِ داده گزارش می‌شود.

    وزن‌های جدید (حداکثرِ جمع = ۱۰۰، دقیقاً مثل قبل):
      شیب منحنی: ۲۰ (بود ۲۵) | HY OAS: ۲۵ (بود ۳۵) | IG OAS: ۱۰ (جدید)
      SOFR-EFFR: ۱۰ (جدید) | VIX: ۲۰ (بود ۲۵) | نقدینگی فد: ۱۰ (بود ۱۵)
      COT (دی‌ریسکینگِ هفتگیِ S&P): ۵ (جدید)

    آستانه‌های IG OAS/SOFR-EFFR/COT اولیه و تجربی‌اند (اولین‌بار در این پروژه
    استفاده می‌شوند) — طبق همان اصلِ «آستانه‌ی اولیه، بعد اصلاح روی داده‌ی
    واقعی» که برای MONTHLY_STALE_AFTER_DAYS در فاز ۱ هم به کار رفت. به‌ویژه
    آستانه‌ی خامِ COT (کانترکت) چون با رشدِ اندازه‌ی بازار در سال‌ها جابه‌جا
    می‌شود، محتمل‌ترین کاندیدِ بازبینیِ آینده است.
    """
    stress_score = 0.0

    if yield_curve_pct < 0:
        stress_score += 20.0
    elif yield_curve_pct < 0.2:
        stress_score += 8.0

    if oas_spread > 4.5:
        stress_score += 25.0
    elif oas_spread > 3.5:
        stress_score += 14.0

    if ig_oas_pct is not None:
        if ig_oas_pct > 2.0:
            stress_score += 10.0
        elif ig_oas_pct > 1.5:
            stress_score += 5.0

    if sofr_effr_bps is not None:
        if sofr_effr_bps > 15.0:
            stress_score += 10.0
        elif sofr_effr_bps > 7.0:
            stress_score += 5.0

    if vix > 28.0:
        stress_score += 20.0
    elif vix > 20.0:
        stress_score += 12.0

    if fed_liq_30d_pct < -2.0:
        stress_score += 10.0
    elif fed_liq_30d_pct < -0.5:
        stress_score += 4.0

    if cot_spx_wow_change is not None:
        if cot_spx_wow_change < -60000.0:
            stress_score += 5.0
        elif cot_spx_wow_change < -30000.0:
            stress_score += 2.5

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


def calculate_rotation_score(
    growth_value_chg_pct: float, cyclical_defensive_chg_pct: float, small_large_chg_pct: float
) -> tuple[float, str]:
    """
    فاز ۵ — امتیازِ جدیدِ rotation، جدا از stress_score (طبق رودمپ). ورودی‌ها
    مومنتومِ ۵روزه‌ی همان سه نسبتِ فاز ۳ هستند (Growth/Value، Cyclical/
    Defensive، Small/Large) — همه از قبل هم‌مقیاس‌اند (٪ تغییر ۵روزه)، پس
    برخلاف stress_score (که متریک‌های ناهم‌مقیاس را با آستانه/امتیاز جمع
    می‌کند)، اینجا میانگینِ ساده کافی است؛ نیازی به وزن‌دهیِ دستی نیست.
    مثبت = چرخشِ ریسک‌پذیر (رشد/چرخه‌ای/کوچک در حال پیشی‌گرفتن)،
    منفی = چرخشِ ریسک‌گریز (ارزش/تدافعی/بزرگ در حال پیشی‌گرفتن — پناه به کیفیت).
    آستانه‌ی ±۱.۵٪ (برای برچسبِ «چرخشِ واضح») تجربی و اولیه است، هم‌راستا با
    دامنه‌ی واقعیِ دیده‌شده در اجراهای اولیه‌ی این پروژه (معمولاً زیرِ ~۲.۵٪).
    """
    avg = (growth_value_chg_pct + cyclical_defensive_chg_pct + small_large_chg_pct) / 3.0

    if avg > 1.5:
        label = "RISK-ON ROTATION (چرخش به‌سمت رشد/چرخه‌ای/کوچک)"
    elif avg < -1.5:
        label = "RISK-OFF ROTATION (چرخش به‌سمت ارزش/تدافعی/بزرگ)"
    else:
        label = "NEUTRAL / NO CLEAR ROTATION (بدون چرخش واضح)"

    return round(avg, 2), label


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
        ig_oas_task = fetch_fred_series(client, "BAMLC0A0CM")
        sofr_task = fetch_fred_series(client, "SOFR")
        effr_task = fetch_fred_series(client, "EFFR")
        ecb_task = fetch_fred_series(client, "ECBASSETSW")
        boj_task = fetch_fred_series(client, "JPNASSETS")
        china_fx_task = fetch_fred_series(client, "TRESEGCNM052N")
        stablecoins_task = fetch_defillama_stablecoins(client)
        cot_spx_task = fetch_cftc_tff_net_leveraged(client, CFTC_SP500_CODE)
        cot_dxy_task = fetch_cftc_tff_net_leveraged(client, CFTC_USD_INDEX_CODE)

        (
            walcl, tga, rrp, t10y2y, oas, ig_oas, sofr, effr, ecb_assets, boj_assets, china_fx,
            stablecoin_result, cot_spx_net, cot_dxy_net,
        ) = await asyncio.gather(
            walcl_task, tga_task, rrp_task, t10y2y_task, oas_task, ig_oas_task,
            sofr_task, effr_task, ecb_task,
            boj_task, china_fx_task,
            stablecoins_task, cot_spx_task, cot_dxy_task,
        )

    yahoo_tickers = {
        "SPX": "^GSPC", "Gold": "GC=F", "Oil": "CL=F", "BTC": "BTC-USD", "VIX": "^VIX",
        "DXY": "DX-Y.NYB",
        # فاز ۳ — سیگنال‌های rotation: هر پا به‌صورت ETF جداگانه (نه ایندکس)
        # چون نسبت باید بین دو دارایی معامله‌پذیرِ هم‌نوع باشد (IWM/SPY، نه IWM/^GSPC)
        "GrowthETF": "IWF", "ValueETF": "IWD",
        "CyclicalETF": "XLY", "DefensiveETF": "XLP",
        "SmallCapETF": "IWM", "LargeCapETF": "SPY",
    }
    yahoo_series = await fetch_all_yahoo_series(yahoo_tickers)

    # ---- ساخت متریک‌ها (هر کدام: زنده / کش-با-تاریخ / N/A) ------------------
    fed_liq = to_metric(cache, "fed_liq_30d_pct", compute_fed_net_liquidity(walcl, tga, rrp))
    curve = to_metric(cache, "curve_slope_pct", compute_last_point(t10y2y))
    oas_m = to_metric(cache, "credit_oas_pct", compute_last_point(oas))

    # فاز ۲ — استرس اعتباری عمیق‌تر (فقط نمایشی؛ طبق تصمیم، وارد فرمول
    # stress_score نمی‌شوند — آن بازطراحی برای فاز ۵ نگه داشته شده)
    ig_oas_m = to_metric(cache, "credit_ig_oas_pct", compute_last_point(ig_oas))
    sofr_effr_m = to_metric(
        cache, "sofr_effr_spread_bps", compute_spread_last_point_bps(sofr, effr)
    )

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
        compute_fred_period_change(china_fx, lookback=1, stale_after_days=CHINA_FX_STALE_AFTER_DAYS),
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

    # فاز ۳ — سیگنال‌های rotation: سطح فعلیِ نسبت + مومنتوم ۵روزه، برای هر جفت
    growth_value_ratio = compute_ratio_series(
        yahoo_series.get("GrowthETF", pd.Series(dtype=float)),
        yahoo_series.get("ValueETF", pd.Series(dtype=float)),
    )
    cyclical_defensive_ratio = compute_ratio_series(
        yahoo_series.get("CyclicalETF", pd.Series(dtype=float)),
        yahoo_series.get("DefensiveETF", pd.Series(dtype=float)),
    )
    small_large_ratio = compute_ratio_series(
        yahoo_series.get("SmallCapETF", pd.Series(dtype=float)),
        yahoo_series.get("LargeCapETF", pd.Series(dtype=float)),
    )

    gv_level_m = to_metric(cache, "growth_value_ratio_level", compute_last_point(growth_value_ratio))
    gv_chg_m = to_metric(cache, "growth_value_ratio_5d_pct", compute_pct_change_5(growth_value_ratio))
    cd_level_m = to_metric(cache, "cyclical_defensive_ratio_level", compute_last_point(cyclical_defensive_ratio))
    cd_chg_m = to_metric(cache, "cyclical_defensive_ratio_5d_pct", compute_pct_change_5(cyclical_defensive_ratio))
    sl_level_m = to_metric(cache, "small_large_ratio_level", compute_last_point(small_large_ratio))
    sl_chg_m = to_metric(cache, "small_large_ratio_5d_pct", compute_pct_change_5(small_large_ratio))

    # فاز ۴ — CFTC COT: خالص پوزیشن Leveraged Funds + تغییر هفته‌به‌هفته
    cot_spx_level_m = to_metric(
        cache, "cot_spx_lev_net_contracts",
        compute_last_point(cot_spx_net, stale_after_days=COT_STALE_AFTER_DAYS),
    )
    cot_spx_chg_m = to_metric(
        cache, "cot_spx_lev_net_wow_change",
        compute_period_change_raw(cot_spx_net, lookback=1, stale_after_days=COT_STALE_AFTER_DAYS),
    )
    cot_dxy_level_m = to_metric(
        cache, "cot_dxy_lev_net_contracts",
        compute_last_point(cot_dxy_net, stale_after_days=COT_STALE_AFTER_DAYS),
    )
    cot_dxy_chg_m = to_metric(
        cache, "cot_dxy_lev_net_wow_change",
        compute_period_change_raw(cot_dxy_net, lookback=1, stale_after_days=COT_STALE_AFTER_DAYS),
    )

    save_cache(cache)

    # ---- استرس سیستمی: ۴ ورودی اصلی هم‌چنان حیاتی؛ ۳ ورودی جدید اختیاری ----
    critical = [curve, oas_m, vix_m, fed_liq]
    if all(m.ok for m in critical):
        stress_index, market_phase = calculate_systemic_stress(
            curve.value, oas_m.value, vix_m.value, fed_liq.value,
            ig_oas_pct=ig_oas_m.value,
            sofr_effr_bps=sofr_effr_m.value,
            cot_spx_wow_change=cot_spx_chg_m.value,
        )
        stress_str = f"{stress_index}/100"
    else:
        stress_str = "N/A ⚠️"
        market_phase = "قابل محاسبه نیست (داده‌ی حیاتی ناقص)"

    # ---- امتیاز rotation: جدا از استرس سیستمی، طبق رودمپ فاز ۵ ----------------
    rotation_critical = [gv_chg_m, cd_chg_m, sl_chg_m]
    if all(m.ok for m in rotation_critical):
        rotation_index, rotation_phase = calculate_rotation_score(
            gv_chg_m.value, cd_chg_m.value, sl_chg_m.value
        )
        rotation_str = f"{rotation_index:+.2f}%"
    else:
        rotation_str = "N/A ⚠️"
        rotation_phase = "قابل محاسبه نیست (داده‌ی حیاتی ناقص)"

    # ---- پاورقیِ کیفیتِ داده -------------------------------------------------
    all_metrics = {
        "نقدینگی فدرال‌رزرو": fed_liq, "شیب منحنی": curve, "OAS (HY)": oas_m,
        "OAS (IG)": ig_oas_m, "اسپرد SOFR-EFFR": sofr_effr_m,
        "ترازنامه ECB": ecb_m, "ترازنامه BoJ": boj_m, "ذخایر ارزی چین (پراکسی PBoC)": china_fx_m,
        "استیبل‌کوین (رشد)": stable_growth, "استیبل‌کوین (حجم)": stable_total,
        "S&P 500": spx_m, "طلا": gold_m, "نفت": oil_m, "BTC": btc_m, "VIX": vix_m,
        "DXY": dxy_m,
        "Growth/Value (سطح)": gv_level_m, "Growth/Value (Δ۵روزه)": gv_chg_m,
        "Cyclical/Defensive (سطح)": cd_level_m, "Cyclical/Defensive (Δ۵روزه)": cd_chg_m,
        "Small/Large (سطح)": sl_level_m, "Small/Large (Δ۵روزه)": sl_chg_m,
        "COT S&P500 (سطح)": cot_spx_level_m, "COT S&P500 (Δهفتگی)": cot_spx_chg_m,
        "COT USD Index (سطح)": cot_dxy_level_m, "COT USD Index (Δهفتگی)": cot_dxy_chg_m,
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
🔹 اسپرد ریسک اعتباری اوراق پرریسک (HY OAS): <code>{oas_m.fmt()}</code>
🔹 اسپرد ریسک اعتباری اوراق سرمایه‌گذاری (IG OAS): <code>{ig_oas_m.fmt()}</code>
🔹 اسپرد استرس ریپو (SOFR-EFFR): <code>{sofr_effr_m.fmt('+.0f', ' bps')}</code>
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

<b>۳. سیگنال‌های چرخش هوشمند (Rotation Signals):</b>
🔄 رشد/ارزش (Growth/Value، IWF÷IWD): <code>{gv_level_m.fmt('.3f', '')}</code> | تغییر ۵روزه: <code>{gv_chg_m.fmt()}</code>
🔄 چرخه‌ای/تدافعی (Cyclical/Defensive، XLY÷XLP): <code>{cd_level_m.fmt('.3f', '')}</code> | تغییر ۵روزه: <code>{cd_chg_m.fmt()}</code>
🔄 کوچک/بزرگ (Small/Large Cap، IWM÷SPY): <code>{sl_level_m.fmt('.3f', '')}</code> | تغییر ۵روزه: <code>{sl_chg_m.fmt()}</code>

<b>۴. موقعیت‌گیری صندوق‌های اهرمی (CFTC COT، Leveraged Funds):</b>
🎯 خالص پوزیشن در S&P 500 (Consolidated): <code>{cot_spx_level_m.fmt('+,.0f', ' قرارداد')}</code> | تغییر هفتگی: <code>{cot_spx_chg_m.fmt('+,.0f', ' قرارداد')}</code>
🎯 خالص پوزیشن در USD Index: <code>{cot_dxy_level_m.fmt('+,.0f', ' قرارداد')}</code> | تغییر هفتگی: <code>{cot_dxy_chg_m.fmt('+,.0f', ' قرارداد')}</code>

<b>۵. ارزیابی ریسک و موقعیت پول هوشمند (Smart Money):</b>
• وضعیت فاز بازار: <b>{market_phase}</b>
• شاخص استرس سیستمی: <code>{stress_str}</code>
• امتیاز rotation: <code>{rotation_str}</code> — <b>{rotation_phase}</b>
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
