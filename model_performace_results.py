#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from sqlalchemy import create_engine
from sklearn.metrics import mean_squared_error

# Veritabanı bağlantısı için engine oluşturma
engine = create_engine('postgresql+psycopg2://postgres:your_password@127.0.0.1:5433/postgres')

# Veriyi çekme
df = pd.read_sql('SELECT * FROM model_results_new', engine)

# Şirketleri ayırt etme
companies = df['Company'].unique()

# Modellerin performansını analiz etmek için sonuçları saklayacak bir sözlük
performance_results = {}

for company in companies:
    company_data = df[df['Company'] == company]
    
    mse_scores = {}
    
    # Gerçek değerler
    y_true = company_data['Real Close']
    
    # Linear Regression modelinin performansını hesaplama
    y_pred_lr = company_data['Predicted Close (Linear Regression)']
    mse_lr = mean_squared_error(y_true, y_pred_lr)
    mse_scores['Linear Regression'] = mse_lr
    
    # Decision Tree modelinin performansını hesaplama
    y_pred_dt = company_data['Predicted Close (Decision Tree)']
    mse_dt = mean_squared_error(y_true, y_pred_dt)
    mse_scores['Decision Tree'] = mse_dt
    
    # Random Forest modelinin performansını hesaplama
    y_pred_rf = company_data['Predicted Close (Random Forest)']
    mse_rf = mean_squared_error(y_true, y_pred_rf)
    mse_scores['Random Forest'] = mse_rf
    
    # Gradient Boosting modelinin performansını hesaplama
    y_pred_gb = company_data['Predicted Close (Gradient Boosting)']
    mse_gb = mean_squared_error(y_true, y_pred_gb)
    mse_scores['Gradient Boosting'] = mse_gb
    
    # En iyi performans gösteren modeli belirleme
    best_model = min(mse_scores, key=mse_scores.get)
    
    performance_results[company] = {
        'Linear Regression MSE': mse_lr,
        'Decision Tree MSE': mse_dt,
        'Random Forest MSE': mse_rf,
        'Gradient Boosting MSE': mse_gb,
        'Best Model': best_model,
        'Best Model MSE': mse_scores[best_model]
    }

# Performans sonuçlarını DataFrame'e dönüştürme
performance_df = pd.DataFrame(performance_results).T

# Performans sonuçlarını yazdırma
print(performance_df)


# In[10]:


import pandas as pd
from sqlalchemy import create_engine
from sklearn.metrics import mean_squared_error

# Veritabanı bağlantısı için engine oluşturma
engine = create_engine('postgresql+psycopg2://postgres:your_password@127.0.0.1:5433/postgres')

# Veriyi çekme
df = pd.read_sql('SELECT * FROM model_results_new', engine)

# Şirketleri ayırt etme
companies = df['Company'].unique()

# Sonuçları saklamak için bir liste
results_list = []

# Her şirket için tahmin hatalarını hesaplama ve sıralama
for company in companies:
    company_data = df[df['Company'] == company]
    
    # Saatlik gruplama
    company_data['Hour'] = company_data['Datetime'].dt.floor('H')
    grouped = company_data.groupby('Hour')
    
    for hour, group in grouped:
        y_true = group['Real Close']
        
        # Modellerin tahmin hatalarını hesaplama
        errors = {
            'Linear Regression': mean_squared_error(y_true, group['Predicted Close (Linear Regression)']),
            'Decision Tree': mean_squared_error(y_true, group['Predicted Close (Decision Tree)']),
            'Random Forest': mean_squared_error(y_true, group['Predicted Close (Random Forest)']),
            'Gradient Boosting': mean_squared_error(y_true, group['Predicted Close (Gradient Boosting)'])
        }
        
        # Modelleri tahmin hatalarına göre sıralama
        sorted_errors = sorted(errors.items(), key=lambda x: x[1])
        
        # Sıralı hataları tabloya ekleme
        results_list.append({
            'Company': company,
            'Hour': hour,
            'Best Model': sorted_errors[0][0],
            'Best Model MSE': sorted_errors[0][1],
            'Second Best Model': sorted_errors[1][0],
            'Second Best Model MSE': sorted_errors[1][1],
            'Third Best Model': sorted_errors[2][0],
            'Third Best Model MSE': sorted_errors[2][1],
            'Worst Model': sorted_errors[3][0],
            'Worst Model MSE': sorted_errors[3][1]
        })

# Sonuçları DataFrame'e dönüştürme
results_df = pd.DataFrame(results_list)

# Sonuçları yazdırma
print(results_df)

# Sonuçları CSV dosyasına yazma (isteğe bağlı)
results_df.to_csv('model_comparison_results.csv', index=False)


# In[11]:


results_df


# In[12]:


import pandas as pd

# Veritabanı bağlantısı için engine oluşturma
engine = create_engine('postgresql+psycopg2://postgres:your_password@127.0.0.1:5433/postgres')

# Veriyi çekme
df = pd.read_sql('SELECT * FROM model_results_new', engine)

# Şirketleri ayırt etme
companies = df['Company'].unique()

# Sonuçları saklamak için bir liste
results_list = []

# Her şirket için tahmin hatalarını hesaplama ve sıralama
for company in companies:
    company_data = df[df['Company'] == company]
    
    # Saatlik gruplama
    company_data['Hour'] = company_data['Datetime'].dt.floor('H')
    grouped = company_data.groupby('Hour')
    
    for hour, group in grouped:
        y_true = group['Real Close']
        
        # Modellerin tahmin hatalarını hesaplama
        errors = {
            'Linear Regression': mean_squared_error(y_true, group['Predicted Close (Linear Regression)']),
            'Decision Tree': mean_squared_error(y_true, group['Predicted Close (Decision Tree)']),
            'Random Forest': mean_squared_error(y_true, group['Predicted Close (Random Forest)']),
            'Gradient Boosting': mean_squared_error(y_true, group['Predicted Close (Gradient Boosting)'])
        }
        
        # Modelleri tahmin hatalarına göre sıralama
        sorted_errors = sorted(errors.items(), key=lambda x: x[1])
        
        # Sıralı hataları tabloya ekleme
        results_list.append({
            'Company': company,
            'Hour': hour,
            'Best Model': sorted_errors[0][0],
            'Best Model MSE': sorted_errors[0][1],
            'Second Best Model': sorted_errors[1][0],
            'Second Best Model MSE': sorted_errors[1][1],
            'Third Best Model': sorted_errors[2][0],
            'Third Best Model MSE': sorted_errors[2][1],
            'Worst Model': sorted_errors[3][0],
            'Worst Model MSE': sorted_errors[3][1]
        })

# Sonuçları DataFrame'e dönüştürme
results_df = pd.DataFrame(results_list)

# Genel sıralama için ortalama MSE hesaplama
average_mse = results_df.groupby(['Best Model'])['Best Model MSE'].mean().reset_index()
average_mse.columns = ['Model', 'Average Best Model MSE']
average_mse = average_mse.sort_values(by='Average Best Model MSE')

# En iyi model
best_model = average_mse.iloc[0]['Model']
best_model_mse = average_mse.iloc[0]['Average Best Model MSE']

# En kötü model
worst_model = average_mse.iloc[-1]['Model']
worst_model_mse = average_mse.iloc[-1]['Average Best Model MSE']

# Genel sıralama sonuçlarını yazdırma
print("Overall Model Performance:")
print(average_mse)

print(f"\nOverall Best Model: {best_model} with an average MSE of {best_model_mse:.4f}")
print(f"Overall Worst Model: {worst_model} with an average MSE of {worst_model_mse:.4f}")




# In[9]:


average_mse
