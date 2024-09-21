import aiohttp
import asyncio
import pandas as pd
import os
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

# Define the rate limit parameters
MAX_CONCURRENT_REQUESTS = 1  # Only one request at a time
REQUESTS_PER_MINUTE = 10  # Maximum requests per minute
REQUEST_DELAY = 60 / REQUESTS_PER_MINUTE  # Delay between requests in seconds
MAX_RETRIES = 5  # Number of retries on rate limit error

semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
last_request_time = 0

async def fetch_data(session, country_code, year, pollutant):
    global last_request_time
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"
    
    params = {
        'country': country_code,
        'date_from': start_date,
        'date_to': end_date,
        'limit': 1000,  # Adjust as needed
        'parameter': pollutant
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    all_results = []
    retries = 0

    async with semaphore:
        while retries < MAX_RETRIES:
            now = time.time()
            elapsed_time = now - last_request_time
            if elapsed_time < REQUEST_DELAY:
                await asyncio.sleep(REQUEST_DELAY - elapsed_time)
            
            last_request_time = time.time()
            
            try:
                async with session.get(url, params=params, headers=headers) as response:
                    # Log rate limit headers and response status
                    print(f"Response status: {response.status}")
                    print(f"Response headers: {response.headers}")
                    
                    if response.status == 200:
                        data = await response.json()
                        all_results.extend(data['results'])
                        break
                    elif response.status == 429:
                        # Handle rate limit exceeded
                        reset_time = int(response.headers.get('X-RateLimit-Reset', 60))
                        wait_time = max(reset_time - time.time(), REQUEST_DELAY)
                        print(f"Rate limit exceeded for {country_code} {year} {pollutant}. Retrying in {wait_time:.2f} seconds...")
                        await asyncio.sleep(wait_time)
                        retries += 1
                    elif response.status == 403:
                        print(f"Access forbidden for {country_code} {year} {pollutant}. Check API access and parameters.")
                        break
                    elif response.status == 500:
                        # Handle server errors if needed
                        pass
                    else:
                        print(f"Failed to fetch data for {country_code} {year} {pollutant}. Status code: {response.status}")
                        break
            except aiohttp.ClientError as e:
                print(f"Request error for {country_code} {year} {pollutant}: {e}")
                retries += 1
                await asyncio.sleep(REQUEST_DELAY)
    
    return country_code, year, pollutant, all_results

async def process_pollutants(session, country_code, year):
    tasks = [fetch_data(session, country_code, year, pollutant) for pollutant in pollutants]
    results = await asyncio.gather(*tasks)
    
    for country_code, year, pollutant, data in results:
        df = pd.DataFrame(data)
        if not df.empty:
            os.makedirs(f'0.Data/API_DATA', exist_ok=True)
            df.to_csv(f'0.Data/API_DATA/{year}_{country_code}_{pollutant}.csv', index=False)

async def main():
    async with aiohttp.ClientSession() as session:
        tasks = []
        for year in years:
            for code in country_codes:
                tasks.append(process_pollutants(session, country_codes[code], year))
        
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
