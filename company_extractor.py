import re
import pandas as pd
from sqlalchemy import text
from database import engine


def extract_company_description(text):
  
    if not isinstance(text, str):
        return None

    text_before_desc = re.split(r'Описание вакансии', text, flags=re.IGNORECASE)[0]

    pattern = r'(?:Компания\s*[:\-]\s*[^\n]+|[Аа] наша|[Нн]аша команда|[Зз]анима(е|ю)мся|[Тт]ехнологическ|AI|инновац|разработк|решени)'
    match = re.search(pattern, text_before_desc)
    if match:
        start = max(0, match.start() - 50)
        end = min(len(text_before_desc), match.end() + 300)
        snippet = text_before_desc[start:end].strip()
        return snippet
    return None


def build_companies_table(vacancies_df):
   
    if "company" not in vacancies_df.columns or "description" not in vacancies_df.columns:
        print(" Vacancies DataFrame does not have 'company' or 'description' columns")
        return None

    companies = (
        vacancies_df.dropna(subset=["company"])
        .copy()
        .drop_duplicates(subset=["company"])
        .reset_index(drop=True)
    )

    companies["company_name"] = companies["company"].str.strip()
    companies["company_description"] = companies["description"].apply(extract_company_description)

    companies["company_url"] = companies["description"].str.extract(r'(https?://\S+)')

    if "location" in companies.columns:
        companies["city"] = companies["location"]
    else:
        companies["city"] = None

    companies_final = companies[["company_name", "company_description", "company_url", "city"]]
    print(f"\nExtracted {len(companies_final)} unique companies.")
    return companies_final


def save_companies_to_db(companies_df):
    """
    Сохраняет компании в отдельную таблицу companies (уникальные записи).
    """
    if companies_df is None or companies_df.empty:
        print(" No company data to save.")
        return

    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS companies (
                id SERIAL PRIMARY KEY,
                company_name TEXT UNIQUE,
                company_description TEXT,
                company_url TEXT,
                city TEXT
            );
        """))

    companies_df.to_sql("companies", con=engine, if_exists="append", index=False)
    print(f"Saved {len(companies_df)} companies into PostgreSQL.")