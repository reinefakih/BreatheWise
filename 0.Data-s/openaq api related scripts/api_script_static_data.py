import requests
import pandas as pd
import time

# Define the API endpoint
url = "https://api.openaq.org/v2/measurements"

country_codes = {
    "Latvia": "LV",
    "Malta": "MT",
    "Cyprus": "CY",
    "Denmark": "DK",
    "Greece": "GR",
    "Italy": "IT",
    "Belgium": "BE",
    "Czech Republic": "CZ",
    "Croatia": "HR",
    "Sweden": "SE",
    "Estonia": "EE",
    "Germany": "DE",
    "Finland": "FI",
    "Lithuania": "LT",
    "Spain": "ES",
    "Luxembourg": "LU",
    "Bulgaria": "BG",
    "Poland": "PL",
    "Romania": "RO",
    "Austria": "AT",
    "Slovakia": "SK",
    "Netherlands": "NL",
    "Ireland": "IE",
    "France": "FR",
    "Hungary": "HU",
    "Portugal": "PT",
    "Slovenia": "SI"
}

years = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]
pollutants = ['pm10', 'pm25', 'so2', 'o3', 'co', 'no2', 'no', 'bc']

for year in years:
    for country in country_codes:
        for pollutant in pollutants:
            # Set the country and year of interest
            country_code = country_codes[country]  # Replace with the desired country code (e.g., "US" for United States)
            start_date = f"{year}-01-01"
            end_date = f"{year}-12-31"

            # Set the initial parameters
            params = {
                'country': country_code,
                'date_from': start_date,
                'date_to': end_date,
                'limit': 50000,  # Adjust the limit to control the number of results per page (max is 100)
                'page': 1,  # Start with the first page
                'parameter': pollutant
            }

            # Initialize an empty list to collect the data
            all_results = []

            while True:
                # Send GET request to the OpenAQ API
                time.sleep(5)
                response = requests.get(url, params=params)
                
                # Check if the request was successful
                if response.status_code == 200:
                    data = response.json()
                    
                    # Append the results to the list
                    all_results.extend(data['results'])
                    break

                elif response.status_code == 500:
                    params['limit'] = 1000
                    time.sleep(5)
                    response = requests.get(url, params=params)
                    data = response.json()
                    all_results.extend(data['results'])
                    break

                else:
                    print(f"Failed to fetch data. Status code: {response.status_code}")
                    break

            # Convert the collected data to a pandas DataFrame
            df = pd.DataFrame(all_results)
            print(df)
            if df.empty:
                pass
            else:
                df.to_csv(f'0.Data//{year}_{country}_{pollutant}.csv')