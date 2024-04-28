import pandas as pd
from sqlalchemy import create_engine

def get_data():
    # SQLAlchemy kullanarak veritabanı bağlantısını kur
    engine = create_engine('postgresql+psycopg2://postgres:your_password@127.0.0.1:5433/postgres')
    sql_query = """
        SELECT data
        FROM finance_records_new
        WHERE source = 'Alpha Vantage' AND id = 10;
    """
    df_json = pd.read_sql_query(sql_query, engine)
    return df_json

def convert_to_dataframe(df_json):
    # JSON veri yapısını düzgün bir şekilde ele al
    records = []
    companies = ["IBM", "TSLA", "AAPL", "MSFT"]  # İşlemek istediğiniz şirketlerin listesi
    for entry in df_json['data']:
        for company in companies:
            if company in entry:
                for date_time, values in entry[company].items():
                    record = {
                        'Datetime': pd.to_datetime(date_time),
                        'Company': company  # Şirket adını kayda ekleyin
                    }
                    record.update({
                        key.split('. ')[1].capitalize(): float(value) if key != '5. volume' else int(value)
                        for key, value in values.items()
                    })
                    records.append(record)
    df_final = pd.DataFrame(records)
    return df_final


def main():
    df_json = get_data()
    df_final = convert_to_dataframe(df_json)
    print(df_final.sample(20))

if __name__ == '__main__':
    main()