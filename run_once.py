import asyncio
from macro_alpha_engine import MacroOrchestrator

async def main():
    orchestrator = MacroOrchestrator()
    print("--- در حال بررسی و ارسال گزارش وضعیت بازار به تلگرام و ایمیل ---")
    
    # ارسال گزارش جامع وضعیت نقدینگی و بازارها در هر اجرا
    await orchestrator.run_weekly_macro_report()
    
    # اسکن برای ناهنجاری‌های شدید ۳۰ روزه
    await orchestrator.run_daily_scan()
    print("--- عملیات با موفقیت انجام شد ---")

if __name__ == "__main__":
    asyncio.run(main())
