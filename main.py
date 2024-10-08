from fastapi import FastAPI, HTTPException
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, Column, Integer, DateTime, Numeric, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import datetime
from mangum import Mangum
from google.cloud import functions_v1

app = FastAPI()

DATABASE_URL = "postgresql+psycopg2://postgres:goldpredictdb@35.223.225.122:5432/postgres"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class GoldPrice(Base):
    __tablename__ = 'Gold_goldprice'

    id = Column(Integer, primary_key=True, index=True)
    date = Column(DateTime, nullable=False)
    gold_price = Column(Numeric(10, 2), nullable=False)

Base.metadata.create_all(bind=engine)

def web_scraping():
    url = 'https://www.goldtraders.or.th/'
    response = requests.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        span_element = soup.find('span', id='DetailPlace_uc_goldprices1_lblBLSell')
        if span_element:
            try:
                price_str = span_element.get_text(strip=True).replace(',', '')
                return float(price_str)
            except ValueError:
                return None
    return None

def store_data(price: float):
    db = SessionLocal()
    try:
        now = datetime.datetime.now()
        db_price = GoldPrice(date=now, gold_price=price)
        db.add(db_price)
        db.commit()
        db.refresh(db_price)
        print(f"Successfully stored price: {price} at {now}")
        return {
            "id": db_price.id,
            "price": db_price.gold_price,
            "date": db_price.date.isoformat()
        }
    except Exception as e:
        print(f"Error storing price: {e}")
        db.rollback()
        raise
    finally:
        db.close()

@app.get("/scrap-gold-th")
def scrap_gold_th():
    data = web_scraping()
    if data is not None:
        result = store_data(data)
        return {"status": "success", **result}
    return {"status": "failure", "message": "Failed to scrape data or data is invalid."}

@app.get("/")
def health_check():
    try:
        db = SessionLocal()
        db.execute(text('SELECT 1'))
        db.close()
        return {"status": "healthy", "message": "Service is up and running."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection error: {e}")

# Entry point for Google Cloud Functions
def main(request):
    return Mangum(app)(request)
