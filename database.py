from sqlalchemy import create_engine
from config import DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME

engine = create_engine(f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

def save_to_postgres(df):
    from company_extractor import ensure_tables, add_company_ids
    
    ensure_tables()
    
    try:
        df = add_company_ids(df)
        
        df = df.drop(columns=["company"], errors="ignore")
        
        df.to_sql("vacancies", con=engine, if_exists="append", index=False)
        print(f"\nSuccessfully saved {len(df)} vacancies with company_id to PostgreSQL!")
    except Exception as e:
        print(f"\nError saving to database: {e}")