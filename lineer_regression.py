#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
from sqlalchemy import create_engine


# In[3]:


engine = create_engine('postgresql+psycopg2://postgres:your_password@127.0.0.1:5433/postgres')


# In[5]:


sql_query = """
        SELECT *
        FROM processed_data;
    """

df = pd.read_sql_query(sql_query, engine)


# In[24]:


# Model iyileştirmesi için zaman sütunları eklendi
df['date'] = pd.to_datetime(df['Datetime']).dt.date
df['time'] = pd.to_datetime(df['Datetime']).dt.time
df['Hour'] = df['Datetime'].dt.hour
df['Minute'] = df['Datetime'].dt.minute


# In[20]:


#date kolonundan 2024 04 19 olan ve Company kolonunda IBM olan verileri getir
df_filtered = df[(df['Company'] == 'TSLA')]


# In[58]:


from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error

# Veri setini yükleyin
data = df
# Gerekli sütunları seçin
features = ['Low', 'Open', 'High', 'Volume']
target = 'Close'

# Tarih ve saat sütunu ekleme ve filtreleme
data['Datetime'] = pd.to_datetime(data['Datetime'])
split_date = pd.Timestamp('2024-05-02 15:00')

# Eğitim ve test setlerini belirleme
train_data = data[data['Datetime'] < split_date]
test_data = data[data['Datetime'] >= split_date]

# Şirketleri ayırt etmek için bir döngü kullanarak her biri için model kurun
companies = train_data['Company'].unique()
results = {}
models = {}
mse_scores = {}

for company in companies:
    # Şirkete özgü verileri filtreleyin
    company_train_data = train_data[train_data['Company'] == company]
    company_test_data = test_data[test_data['Company'] == company]
    
    # Eğitim ve test verilerini ayırın
    X_train = company_train_data[features]
    y_train = company_train_data[target]
    X_test = company_test_data[features]
    y_test = company_test_data[target]
    
    # Lineer regresyon modelini oluşturun ve eğitin
    model = LinearRegression()
    model.fit(X_train, y_train)
    
    # Test verisi üzerinde tahmin yapın
    predictions = model.predict(X_test)
    
    # Modelin performansını değerlendirin
    mse = mean_squared_error(y_test, predictions)
    mse_scores[company] = mse
    print(f'{company}: Mean Squared Error (MSE) = {mse}')


    # Test sonuçlarını ve tahminleri bir DataFrame'e kaydet
    results[company] = pd.DataFrame({
        'Real Close': y_test,
        'Predicted Close': predictions,
        'Datetime': company_test_data['Datetime']
    })
    # test sonuçlarına şirket adı ekleme
    results[company]['Company'] = company

# Sonuçları yazdırın veya analiz edin
for company, result in results.items():
    print(f'Results for {company}:')
    print(result.head(1))  # İlk beş sonucu göster

#sonuçları şirket kolonu ekleyerek birleştirme
all_results = pd.concat(results.values())




# In[59]:


import matplotlib.pyplot as plt

# results değişkeniniz varsa ve her şirketin tahminlerini içeriyorsa:
for company, result in results.items():
    plt.figure(figsize=(10, 5))  # Grafik boyutunu ayarlayın
    plt.plot(result['Datetime'], result['Real Close'], label='Real Close')
    plt.plot(result['Datetime'], result['Predicted Close'], label='Predicted Close')
    plt.title(f'{company} Stock Price Prediction')
    plt.xlabel('Datetime')
    plt.ylabel('Close Price')
    plt.legend()
    plt.xticks(rotation=45)  # Tarih etiketlerini okunabilir yapmak için çevirin
    plt.tight_layout()  # Grafik düzenini optimize et
    plt.show()



