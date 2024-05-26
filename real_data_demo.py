import pandas as pd
from sqlalchemy import create_engine

# Veritabanı bağlantısı için engine oluşturma
engine = create_engine('postgresql+psycopg2://postgres:your_password@127.0.0.1:5433/postgres')

# Veriyi çekme
df = pd.read_sql('SELECT * FROM model_results_new', engine)

# Şirketleri ayırt etme
companies = df['Company'].unique()

# Her model için başlangıç yatırımı
initial_investment = 1000

# Sonuçları saklamak için bir sözlük
investment_results = {model: {company: initial_investment for company in companies} for model in ['Linear Regression', 'Decision Tree', 'Random Forest', 'Gradient Boosting']}

# Her şirket için yatırım getirisi hesaplama
for company in companies:
    company_data = df[df['Company'] == company].set_index('Datetime')
    hours = company_data.index.floor('H').unique()
    
    for hour in hours:
        if hour == hours[0]:
            # İlk saat için başlangıç kapanış fiyatlarını al
            initial_prices = {
                'Linear Regression': company_data.loc[hour, 'Predicted Close (Linear Regression)'],
                'Decision Tree': company_data.loc[hour, 'Predicted Close (Decision Tree)'],
                'Random Forest': company_data.loc[hour, 'Predicted Close (Random Forest)'],
                'Gradient Boosting': company_data.loc[hour, 'Predicted Close (Gradient Boosting)']
            }
        else:
            # Önceki saat için kapanış fiyatlarını al
            previous_prices = {
                'Linear Regression': company_data.loc[hour - pd.Timedelta(hours=1), 'Predicted Close (Linear Regression)'],
                'Decision Tree': company_data.loc[hour - pd.Timedelta(hours=1), 'Predicted Close (Decision Tree)'],
                'Random Forest': company_data.loc[hour - pd.Timedelta(hours=1), 'Predicted Close (Random Forest)'],
                'Gradient Boosting': company_data.loc[hour - pd.Timedelta(hours=1), 'Predicted Close (Gradient Boosting)']
            }
            
            # Her model için yatırım getirisi hesapla
            for model in ['Linear Regression', 'Decision Tree', 'Random Forest', 'Gradient Boosting']:
                current_price = company_data.loc[hour, f'Predicted Close ({model})']
                investment_results[model][company] *= (current_price / previous_prices[model])

# Sonuçları DataFrame'e dönüştürme
investment_df = pd.DataFrame(investment_results)

# Sonuçları yazdırma
print(investment_df)
