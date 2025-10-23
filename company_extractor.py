import re
import pandas as pd
from sqlalchemy import text
from database import engine


def extract_company_description(text):
    """Извлечь описание компании из текста вакансии"""
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
    """Построить таблицу уникальных компаний из вакансий"""
    if "company" not in vacancies_df.columns or "description" not in vacancies_df.columns:
        print("Vacancies DataFrame does not have 'company' or 'description' columns")
        return None

    companies = (
        vacancies_df.dropna(subset=["company"])
        .copy()
        .drop_duplicates(subset=["company"])
        .reset_index(drop=True)
    )

    companies["company_name"] = companies["company"].str.strip()
    companies["company_description"] = companies["description"].apply(extract_company_description)
    companies["company_url"] = companies["description"].str.extract(r'(https?://\S+)', expand=False)

    if "location" in companies.columns:
        companies["city"] = companies["location"]
    else:
        companies["city"] = None

    companies_final = companies[["company_name", "company_description", "company_url", "city"]]
    print(f"\nExtracted {len(companies_final)} unique companies.")
    return companies_final


def save_companies_to_db(companies_df):
    """Сохранить компании в БД (только новые, без дубликатов)"""
    if companies_df is None or companies_df.empty:
        print("No company data to save.")
        return

    try:
        # Получить существующие компании
        try:
            existing = pd.read_sql("SELECT company_name FROM companies", engine)
            existing_names = set(existing['company_name'].str.strip().str.upper())
        except:
            # Таблица еще не существует или пустая
            existing_names = set()
        
        # Фильтровать только новые компании
        companies_df['company_name_upper'] = companies_df['company_name'].str.strip().str.upper()
        new_companies = companies_df[~companies_df['company_name_upper'].isin(existing_names)]
        new_companies = new_companies.drop('company_name_upper', axis=1)
        
        if len(new_companies) > 0:
            new_companies.to_sql("companies", con=engine, if_exists="append", index=False)
            print(f"Saved {len(new_companies)} new companies to database")
        else:
            print("ℹNo new companies to add (all already exist)")
            
    except Exception as e:
        print(f"Error saving companies: {e}")
        import traceback
        traceback.print_exc()


def ensure_tables():
    """Создать таблицы companies и vacancies с правильной связью"""
    try:
        with engine.begin() as conn:
            # Таблица компаний
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS companies (
                    id SERIAL PRIMARY KEY,
                    company_name TEXT UNIQUE NOT NULL,
                    company_description TEXT,
                    company_url TEXT,
                    city TEXT
                );
            """))

            # Таблица вакансий с внешним ключом на companies
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS vacancies (
                    id SERIAL PRIMARY KEY,
                    position TEXT,
                    company_id INTEGER REFERENCES companies(id) ON DELETE SET NULL,
                    location TEXT,
                    salary TEXT,
                    contacts TEXT,
                    channel TEXT,
                    date TEXT,
                    description TEXT
                );
            """))
            
        print(" Tables 'companies' and 'vacancies' are ready")
    except Exception as e:
        print(f"Error creating tables: {e}")


def add_company_ids(vacancies_df):
    
    try:
        companies_df = pd.read_sql("SELECT id, company_name FROM companies", engine)
        
        if companies_df.empty:
            print("No companies in database!")
            return vacancies_df
        
        mapping = dict(zip(companies_df["company_name"].str.strip().str.lower(), companies_df["id"]))

        vacancies_df["company_id"] = vacancies_df["company"].str.strip().str.lower().map(mapping)

        new_companies = vacancies_df[vacancies_df["company_id"].isna()]["company"].dropna().unique()
        
        if len(new_companies) > 0:
            print(f"Found {len(new_companies)} companies not in DB, adding them...")
            for comp in new_companies:
                try:
                    with engine.begin() as conn:
                        conn.execute(
                            text("INSERT INTO companies (company_name) VALUES (:name) ON CONFLICT (company_name) DO NOTHING"),
                            {"name": comp.strip()}
                        )
                except Exception as e:
                    print(f"Error adding company '{comp}': {e}")

            companies_df = pd.read_sql("SELECT id, company_name FROM companies", engine)
            mapping = dict(zip(companies_df["company_name"].str.strip().str.lower(), companies_df["id"]))
            vacancies_df["company_id"] = vacancies_df["company"].str.strip().str.lower().map(mapping)

        # Проверить, остались ли вакансии без company_id
        missing = vacancies_df["company_id"].isna().sum()
        if missing > 0:
            print(f"Warning: {missing} vacancies still have no company_id")

        return vacancies_df
        
    except Exception as e:
        print(f"Error adding company IDs: {e}")
        import traceback
        traceback.print_exc()
        return vacancies_df