import pandas as pd
from sqlalchemy import create_engine

def get_data():
    # SQLAlchemy kullanarak veritabanı bağlantısını kur
    engine = create_engine('postgresql+psycopg2://postgres:your_password@127.0.0.1:5433/postgres')
    sql_query = """
        SELECT *
        FROM processed_data;
    """
    df = pd.read_sql_query(sql_query, engine)
    # tarih sütununu datetime formatına dönüştür
    df['Datetime'] = pd.to_datetime(df['Datetime'])
    # tarih sütununu gün bazında grupla
    df['Datetime'] = df['Datetime'].dt.date
    return df

def aggregate_data():
    # Verileri şirket bazında ve tarih bazında grupla
    df_grouped = get_data().groupby(['Company','Datetime']).mean()
    return df_grouped

def main():
    df = get_data()
    df_grouped = aggregate_data()
    print(df.head())
    print(df_grouped)

if __name__ == '__main__':
    main()