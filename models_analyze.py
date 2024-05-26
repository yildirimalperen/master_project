#!/usr/bin/env python
# coding: utf-8


import pandas as pd
from sqlalchemy import create_engine
engine = create_engine('postgresql+psycopg2://postgres:your_password@127.0.0.1:5433/postgres')
sql_query = """
        SELECT * FROM processed_data;
    """
df = pd.read_sql_query(sql_query, engine)

df['date'] = pd.to_datetime(df['Datetime']).dt.date
df['time'] = pd.to_datetime(df['Datetime']).dt.time
df['Hour'] = df['Datetime'].dt.hour
df['Minute'] = df['Datetime'].dt.minute
df['Datetime'] = pd.to_datetime(df['Datetime'])

from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error
from sklearn.ensemble import GradientBoostingRegressor

data = df
features = []
target = 'Close'

data['Datetime'] = pd.to_datetime(data['Datetime'])
split_date = pd.Timestamp('2024-05-14 15:00')

lags = [3, 4, 6, 8, 12, 16, 20, 24]
metrics = ['Close', 'Low', 'High', 'Open', 'Volume']
for lag in lags:
    for metric in metrics:
        data[f'lag_{lag}_{metric}'] = data.groupby('Company')[metric].shift(lag)
        data[f'{metric}_rolling_mean_{lag}'] = data.groupby('Company')
        [metric].transform(lambda x: x.rolling(window=lag, min_periods=1).mean())
        data[f'{metric}_rolling_std_{lag}'] = data.groupby('Company')
        [metric].transform(lambda x: x.rolling(window=lag, min_periods=1).std())

        features.append(f'lag_{lag}_{metric}')
        features.append(f'{metric}_rolling_mean_{lag}')
        features.append(f'{metric}_rolling_std_{lag}')

train_data = data[data['Datetime'] < split_date]
test_data = data[data['Datetime'] >= split_date]

companies = train_data['Company'].unique()
results = {}
mse_scores = {}

models = {
    'Linear Regression': LinearRegression(),
    'Decision Tree': DecisionTreeRegressor(),
    'Random Forest': RandomForestRegressor(n_estimators=100, random_state=1),
    'Gradient Boosting': GradientBoostingRegressor(n_estimators=100, random_state=1)
}

scaler = StandardScaler()

for company in companies:
    company_train_data = train_data[train_data['Company'] == company].dropna()
    company_test_data = test_data[test_data['Company'] == company].dropna()

    X_train = company_train_data[features]
    y_train = company_train_data[target]
    X_test = company_test_data[features]
    y_test = company_test_data[target]
    
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    for model_name, model in models.items():
        model.fit(X_train, y_train)
        predictions = model.predict(X_test)
        
        mse = mean_squared_error(y_test, predictions)
        mse_scores[f'{company}_{model_name}'] = mse
        print(f'{company} ({model_name}): Mean Squared Error (MSE) = {mse}')

        if f'{company}' not in results:
            results[f'{company}'] = pd.DataFrame({
                'Real Close': y_test,
                'Datetime': company_test_data['Datetime'],
                'Company': company
            })      
        results[f'{company}'][f'Predicted Close ({model_name})'] = predictions

df_final = pd.concat(results.values())

def write_to_db(df_final):
    engine = create_engine('postgresql+psycopg2://postgres:your_password@127.0.0.1:5433/postgres')
    df_final.to_sql('model_results_new', engine, if_exists='replace', index=False)
write_to_db(df_final)
for company, result in results.items():
    print(f'Results for {company}:')
    print(result.head(5))  


import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from sklearn.ensemble import GradientBoostingRegressor

# Şirketler listesi
companies = ['IBM', 'TSLA', 'AAPL', 'MSFT']  # Bu listeyi mevcut şirketlerle güncelleyin

# Renkler ve çizgi stilleri
colors = {
    'Real Close': '#1f77b4',  # Mavi
    'Linear Regression': '#ff7f0e',  # Turuncu
    'Decision Tree': '#2ca02c',  # Yeşil
    'Random Forest': '#d62728',  # Kırmızı
    'Gradient Boosting': '#9467bd'  # Mor
}
line_styles = {
    'Real Close': '-',
    'Linear Regression': '--',
    'Decision Tree': ':',
    'Random Forest': '-.',
    'Gradient Boosting': '-'
}

# Her şirket için grafik oluşturma
for company in companies:
    plt.figure(figsize=(15, 8))  # Grafik boyutunu ayarlayın

    plt.plot(results[company]['Datetime'], results[company]['Real Close'], 
             label='Real Close', color=colors['Real Close'], linestyle=line_styles['Real Close'], linewidth=1.2)

    if f'Predicted Close (Linear Regression)' in results[company]:
        plt.plot(results[company]['Datetime'], results[company]['Predicted Close (Linear Regression)'], 
                 label='Predicted Close (Linear Regression)', 
                 color=colors['Linear Regression'], linestyle=line_styles['Linear Regression'], linewidth=1.2)

    if f'Predicted Close (Decision Tree)' in results[company]:
        plt.plot(results[company]['Datetime'], results[company]['Predicted Close (Decision Tree)'], 
                 label='Predicted Close (Decision Tree)', 
                 color=colors['Decision Tree'], linestyle=line_styles['Decision Tree'], linewidth=1.2)

    if f'Predicted Close (Random Forest)' in results[company]:
        plt.plot(results[company]['Datetime'], results[company]['Predicted Close (Random Forest)'], 
                 label='Predicted Close (Random Forest)', 
                 color=colors['Random Forest'], linestyle=line_styles['Random Forest'], linewidth=1.2)

    if f'Predicted Close (Gradient Boosting)' in results[company]:
        plt.plot(results[company]['Datetime'], results[company]['Predicted Close (Gradient Boosting)'], 
                 label='Predicted Close (Gradient Boosting)', 
                 color=colors['Gradient Boosting'], linestyle=line_styles['Gradient Boosting'], linewidth=1.2)
    
    plt.title(f'{company} Stock Price Prediction', fontsize=20)
    plt.xlabel('Datetime', fontsize=8)
    plt.ylabel('Close Price', fontsize=16)
    plt.legend(loc='upper right', fontsize=10)
    
    plt.gca().xaxis.set_major_locator(mdates.MinuteLocator(interval=30))
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    plt.xticks(rotation=45, fontsize=12)  
    plt.yticks(fontsize=12)
    
    plt.grid(True, linestyle='--', alpha=0.5) 
    plt.tight_layout()  
    
    plt.show()


