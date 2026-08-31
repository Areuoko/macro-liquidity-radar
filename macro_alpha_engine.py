import os
import smtplib
import asyncio
import logging
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dataclasses import dataclass
from typing import Dict, Optional

import httpx
import yfinance as yf
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")
logger = logging.getLogger("MacroEngine")

@dataclass
class MacroSignal:
    timestamp: datetime
    net_liquidity_roc_30d: float
    yield_curve_slope: float
    credit_spread_oas: float
    stablecoin_supply_change_30d: float
    cross_asset_stress_index: float
    anomaly_detected: bool
    direction: str
    confidence_score: float
    description: str

class FreeDataHarvester:
    """جمع‌آوری داده‌های کلان مالی از منابع ۱۰۰٪ رایگان و آزاد"""

    def __init__(self, fred_api_key: Optional[str] = None):
        self.fred_api_key = fred_api_key or os.getenv("FRED_API_KEY")

    async def fetch_fred_series(self, series_id: str, limit: int = 60) -> pd.Series:
        """واکشی سری زمانی از پایگاه داده فدرال‌رزرو سنت لوئیس (FRED)"""
        if not self.fred_api_key:
            logger.warning(f"کلید FRED_API_KEY یافت نشد. سری {series_id} رد شد.")
            return pd.Series(dtype=float)

        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": series_id,
            "api_key": self.fred_api_key,
            "file_type": "json",
            "sort_order": "desc",
            "limit": limit
        }
        
        async with httpx.AsyncClient(timeout=15.0) as client:
            try:
                res = await client.get(url, params=params)
                if res.status_code == 200:
                    data = res.json().get("observations", [])
                    df = pd.DataFrame(data)
                    df["date"] = pd.to_datetime(df["date"])
                    df["value"] = pd.to_numeric(df["value"], errors="coerce")
                    return df.dropna().sort_values("date").set_index("date")["value"]
                else:
                    logger.error(f"خطای دریافت FRED ({series_id}): {res.status_code}")
            except Exception as e:
                logger.error(f"خطا در ارتباط با FRED برای {series_id}: {e}")
        return pd.Series(dtype=float)

    async def fetch_defillama_stablecoins(self) -> float:
        """رهگیری ضرب و سوزاندن استیبل‌کوین‌های نهادی از DefiLlama"""
        url = "https://stablecoins.llama.fi/stablecoincharts/all"
        async with httpx.AsyncClient(timeout=15.0) as client:
            try:
                res = await client.get(url)
                if res.status_code == 200:
                    raw_data = res.json()
                    if isinstance(raw_data, list) and len(raw_data) >= 30:
                        def get_usd(entry):
                            val = entry.get("totalCirculatingUSD") or entry.get("totalUSD") or {}
                            if isinstance(val, dict):
                                return float(val.get("peggedUSD", 0))
                            return float(val) if isinstance(val, (int, float)) else 0.0

                        current_val = get_usd(raw_data[-1])
                        past_30d_val = get_usd(raw_data[-30])
                        
                        if past_30d_val > 0:
                            return round(((current_val - past_30d_val) / past_30d_val) * 100.0, 2)
            except Exception as e:
                logger.error(f"خطا در پردازش دیتای DefiLlama: {e}")
        return 0.0

    def fetch_market_matrix(self) -> Dict[str, pd.DataFrame]:
        """دریافت دیتای زنده دارایی‌های جهانی از Yahoo Finance"""
        tickers = {
            "SPX": "^GSPC",
            "Gold": "GC=F",
            "CrudeOil": "CL=F",
            "US10Y": "^TNX",
            "DXY": "DX-Y.NYB"
        }
        data_matrix = {}
        for name, ticker in tickers.items():
            try:
                hist = yf.Ticker(ticker).history(period="1mo", interval="1d")
                if not hist.empty:
                    data_matrix[name] = hist
            except Exception as e:
                logger.error(f"خطا در دانلود دیتای نماد {name}: {e}")
        return data_matrix


class MacroPredictiveEngine:
    """موتور تحلیل و کشف ردپای پول هوشمند در افق ۳۰ روزه"""

    def __init__(self, harvester: FreeDataHarvester):
        self.harvester = harvester

    async def analyze(self) -> MacroSignal:
        # دریافت داده‌های لوله‌کشی نقدینگی فدرال‌رزرو
        walcl = await self.harvester.fetch_fred_series("WALCL")
        tga = await self.harvester.fetch_fred_series("WTREGEN")
        rrp = await self.harvester.fetch_fred_series("RRPONTSYD")
        t10y2y = await self.harvester.fetch_fred_series("T10Y2Y")
        hy_oas = await self.harvester.fetch_fred_series("BAMLH0A0HYM2")
        stablecoin_roc = await self.harvester.fetch_defillama_stablecoins()

        # محاسبه نقدینگی خالص: ترازنامه - حساب خزانه‌داری - ریورس ریپو
        liquidity_roc = 0.0
        if not walcl.empty and not tga.empty and not rrp.empty:
            combined = pd.DataFrame({"walcl": walcl, "tga": tga, "rrp": rrp}).ffill().dropna()
            combined["net_liquidity"] = combined["walcl"] - combined["tga"] - combined["rrp"]
            if len(combined) >= 4:
                liquidity_roc = ((combined["net_liquidity"].iloc[-1] - combined["net_liquidity"].iloc[-4]) / abs(combined["net_liquidity"].iloc[-4])) * 100.0

        curve_slope = float(t10y2y.iloc[-1]) if not t10y2y.empty else 0.0
        credit_oas = float(hy_oas.iloc[-1]) if not hy_oas.empty else 3.5

        # نمره‌دهی ناهنجاری کلان
        stress_points = 0.0
        direction = "NEUTRAL"
        anomaly = False

        if liquidity_roc > 2.0 and stablecoin_roc > 1.0:
            direction = "BULLISH_EXPANSION"
            stress_points += 40
            anomaly = True
        elif liquidity_roc < -2.0:
            direction = "BEARISH_CONTRACTION"
            stress_points += 40
            anomaly = True

        if curve_slope < 0:
            stress_points += 25
        if credit_oas > 4.5:
            stress_points += 20

        confidence = min(stress_points + 25.0, 96.0)

        desc = (
            f"🔹 تغییرات نقدینگی خالص فدرال‌رزرو (۳۰ روزه): <b>{liquidity_roc:+.2f}%</b>\n"
            f"🔹 شیب منحنی بازده ۱۰ساله-۲ساله: <b>{curve_slope:+.2f} bps</b>\n"
            f"🔹 اسپرد ریسک اعتباری اوراق (OAS): <b>{credit_oas:.2f}%</b>\n"
            f"🔹 رشد موجودی استیبل‌کوین‌های نهادی: <b>{stablecoin_roc:+.2f}%</b>\n"
        )

        return MacroSignal(
            timestamp=datetime.now(),
            net_liquidity_roc_30d=round(liquidity_roc, 2),
            yield_curve_slope=round(curve_slope, 2),
            credit_spread_oas=round(credit_oas, 2),
            stablecoin_supply_change_30d=stablecoin_roc,
            cross_asset_stress_index=stress_points,
            anomaly_detected=anomaly,
            direction=direction,
            confidence_score=confidence,
            description=desc
        )


class NotificationDispatcher:
    """سیستم توزیع هشدارها به تلگرام و ایمیل با قابلیت Fallback"""

    def __init__(self):
        self.tg_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.tg_chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
        self.smtp_port = int(os.getenv("SMTP_PORT", 587))
        self.smtp_user = os.getenv("SMTP_USER")
        self.smtp_pass = os.getenv("SMTP_PASS")
        self.email_receiver = os.getenv("EMAIL_RECEIVER")

    async def send_telegram(self, message: str):
        if not self.tg_token or not self.tg_chat_id:
            logger.warning("اطلاعات تلگرام در سکرت‌ها ناقص است.")
            return

        chat_id_clean = str(self.tg_chat_id).strip()
        url = f"https://api.telegram.org/bot{self.tg_token.strip()}/sendMessage"
        
        payload = {
            "chat_id": chat_id_clean,
            "text": message,
            "parse_mode": "HTML"
        }
        
        async with httpx.AsyncClient(timeout=15.0) as client:
            try:
                res = await client.post(url, json=payload)
                if res.status_code == 200:
                    logger.info("پیام تلگرام با موفقیت ارسال شد.")
                elif res.status_code == 400:
                    # ارسال متن ساده در صورت خطای تگ‌های HTML
                    payload.pop("parse_mode", None)
                    clean_text = (
                        message.replace("<b>", "")
                        .replace("</b>", "")
                        .replace("<code>", "")
                        .replace("</code>", "")
                        .replace("<i>", "")
                        .replace("</i>", "")
                    )
                    payload["text"] = clean_text
                    retry_res = await client.post(url, json=payload)
                    if retry_res.status_code == 200:
                        logger.info("پیام تلگرام به صورت متن ساده ارسال شد.")
                    else:
                        logger.error(f"خطای مجدد ارسال تلگرام: {retry_res.text}")
                else:
                    logger.error(f"خطای تلگرام: {res.text}")
            except Exception as e:
                logger.error(f"خطای اتصال به شبکه تلگرام: {e}")

    def send_email(self, subject: str, body_html: str):
        if not self.smtp_user or not self.smtp_pass or not self.email_receiver:
            logger.warning("اطلاعات SMTP ناقص است.")
            return
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = self.smtp_user
            msg["To"] = self.email_receiver
            msg.attach(MIMEText(body_html, "html"))
            
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_pass)
                server.sendmail(self.smtp_user, self.email_receiver, msg.as_string())
            logger.info("ایمیل با موفقیت ارسال شد.")
        except Exception as e:
            logger.error(f"خطای ارسال ایمیل: {e}")


class MacroOrchestrator:
    """مدیریت اجرای اسکن‌ها و تولید گزارش جامع"""

    def __init__(self):
        self.harvester = FreeDataHarvester()
        self.engine = MacroPredictiveEngine(self.harvester)
        self.dispatcher = NotificationDispatcher()

    async def run_daily_scan(self):
        signal = await self.engine.analyze()
        if signal.anomaly_detected:
            status_fa = "تزریق سنگین نقدینگی نهادی (Bullish Flow)" if signal.direction == "BULLISH_EXPANSION" else "تخلیه شدید نقدینگی (Bearish Flow)"
            msg = (
                f"🚨 <b>هشدار ناهنجاری جریان پول هوشمند (افق ۳۰ روزه)</b>\n\n"
                f"<b>جهت پیش‌بینی‌شده:</b> {status_fa}\n"
                f"<b>ضریب اطمینان:</b> {signal.confidence_score}%\n\n"
                f"📊 <b>متریک‌های کلیدی:</b>\n"
                f"{signal.description}\n"
                f"💡 <i>تاثیر این جابه‌جایی طی ۳ الی ۴ هفته آینده روی کامودیتی‌ها و بازارها تخلیه خواهد شد.</i>"
            )
            await self.dispatcher.send_telegram(msg)
            self.dispatcher.send_email("🚨 هشدار تحرک پول هوشمند جهانی", msg.replace("\n", "<br>"))
        else:
            logger.info("ناهنجاری ماکرو مشاهده نشد.")

    async def run_weekly_macro_report(self):
        signal = await self.engine.analyze()
        markets = self.harvester.fetch_market_matrix()
        
        spx = f"{((markets['SPX']['Close'].iloc[-1] - markets['SPX']['Close'].iloc[-5]) / markets['SPX']['Close'].iloc[-5]) * 100:+.2f}%" if "SPX" in markets and len(markets['SPX']) >= 5 else "N/A"
        gold = f"{((markets['Gold']['Close'].iloc[-1] - markets['Gold']['Close'].iloc[-5]) / markets['Gold']['Close'].iloc[-5]) * 100:+.2f}%" if "Gold" in markets and len(markets['Gold']) >= 5 else "N/A"
        oil = f"{((markets['CrudeOil']['Close'].iloc[-1] - markets['CrudeOil']['Close'].iloc[-5]) / markets['CrudeOil']['Close'].iloc[-5]) * 100:+.2f}%" if "CrudeOil" in markets and len(markets['CrudeOil']) >= 5 else "N/A"

        report = (
            f"🌐 <b>گزارش جامع هفتگی وضعیت اقتصاد و نقدینگی کلان</b>\n"
            f"📅 تاریخ: {datetime.now().strftime('%Y-%m-%d')}\n\n"
            f"<b>۱. وضعیت لوله‌کشی نقدینگی جهانی:</b>\n"
            f"{signal.description}\n"
            f"<b>۲. بازدهی هفتگی دارایی‌های کلان:</b>\n"
            f"• شاخص S&P 500: <code>{spx}</code>\n"
            f"• طلای جهانی: <code>{gold}</code>\n"
            f"• نفت خام: <code>{oil}</code>\n\n"
            f"<b>۳. ارزیابی ریسک پول هوشمند:</b>\n"
            f"• وضعیت فاز: <b>{signal.direction}</b>\n"
            f"• شاخص استرس سیستمی: <code>{signal.cross_asset_stress_index}/100</code>\n"
        )
        await self.dispatcher.send_telegram(report)
        self.dispatcher.send_email("🌐 گزارش جامع هفتگی اقتصاد کلان", report.replace("\n", "<br>"))
