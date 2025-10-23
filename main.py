import asyncio
from telegram_parser import collect_all, process_vacancies
from model import train_text_classifier
from database import save_to_postgres
from company_extractor import build_companies_table, save_companies_to_db

async def main():
    print("Starting Telegram Vacancy Scraper + ML Parser\n")

    messages = await collect_all()
    if not messages:
        print("No messages found")
        return

    df = process_vacancies(messages)
    if len(df) == 0:
        print("No valid vacancies")
        return

    model = train_text_classifier(df)
    save_to_postgres(df)

    companies_df = build_companies_table(df)
    save_companies_to_db(companies_df)

    print("\nDone!")

if __name__ == "__main__":
    asyncio.run(main())