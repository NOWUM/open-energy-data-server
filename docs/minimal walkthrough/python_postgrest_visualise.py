import requests
import pandas as pd
import matplotlib.pyplot as plt

POSTGREST_URL = 'http://localhost:3001/rpc/opsd_national_generation_year_country'
# POSTGREST_URL = 'https://timescale.nowum.fh-aachen.de/oeds/rpc/opsd_national_generation_year_country'

def fetch_data():
    response = requests.get(POSTGREST_URL)
    response.raise_for_status() 
    data = response.json()
    return data

def plot_data(data):
    df = pd.DataFrame(data)
    
    df_total = df[df['technology'] == 'Total']
    
    plt.figure(figsize=(10, 6))
    for country in df_total['country'].unique():
        country_data = df_total[df_total['country'] == country]
        plt.plot(country_data['year'], country_data['total_production'], label=country)
    
    plt.title('Total National Production by Country')
    plt.xlabel('Year')
    plt.ylabel('Total Production (MW)')
    plt.legend()
    plt.grid(True)
    plt.show()

def main():
    data = fetch_data()
    
    print(pd.DataFrame(data).head())
    
    plot_data(data)

if __name__ == "__main__":
    main()
