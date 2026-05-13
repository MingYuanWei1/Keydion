import os
from sqlalchemy import create_engine
engine = create_engine("mysql+pymysql://keydion:keydion_local@127.0.0.1:3306/Keydion_db?charset=utf8mb4")
with engine.connect() as conn:
    result = conn.execute("SELECT title, category FROM news_articles LIMIT 1")
    for row in result:
        print(row)
