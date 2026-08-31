import asyncio
from datetime import datetime
from macro_alpha_engine import MacroOrchestrator

async def main():
    orchestrator = MacroOrchestrator()
    # اسکن روزانه
    await orchestrator.run_daily_scan()
    # اگر روز یکشنبه باشد یا اجرای دستی باشد، گزارش هفتگی هم ارسال شود
    if datetime.now().weekday() == 6:
        await orchestrator.run_weekly_macro_report()

if __name__ == "__main__":
    asyncio.run(main())
