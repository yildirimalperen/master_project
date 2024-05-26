import pandas as pd
from sqlalchemy import create_engine

# Veritabanı bağlantısı için engine oluşturma
engine = create_engine('postgresql+psycopg2://postgres:your_password@127.0.0.1:5433/postgres')

def export_table_to_csv(table_name, csv_file_name):
    # SQL sorgusu ile tabloyu çekme
    sql_query = f"SELECT * FROM {table_name};"
    df = pd.read_sql_query(sql_query, engine)
    
    # DataFrame'i CSV dosyasına kaydetme
    df.to_csv(csv_file_name, index=False)
    print(f"{table_name} tablosu {csv_file_name} olarak kaydedildi.")

# process_data tablosunu CSV dosyasına kaydetme
export_table_to_csv('processed_data', 'process_data.csv')

# model_results_new tablosunu CSV dosyasına kaydetme
export_table_to_csv('model_results_new', 'model_results_new.csv')
