import asyncio
from telegram_parser import collect_all, process_vacancies
from model import train_text_classifier
from database import save_to_postgres
from company_extractor import build_companies_table, save_companies_to_db, ensure_tables

async def main():
    print("Starting Telegram Vacancy Scraper + ML Parser\n")
    
    ensure_tables()
    
    messages = await collect_all()
    if not messages:
        print(" No messages found")
        return
    
    df = process_vacancies(messages)
    if len(df) == 0:
        print("No valid vacancies")
        return
    
    model = train_text_classifier(df)
    
    companies_df = build_companies_table(df)
    if companies_df is not None:
        save_companies_to_db(companies_df)
    
    save_to_postgres(df)
    
    print("\n Done!")

if __name__ == "__main__":
    asyncio.run(main())