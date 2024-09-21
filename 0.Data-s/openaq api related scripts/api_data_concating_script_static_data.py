# -- import statements -- #:
import pandas as pd
import os

# -- defining the constant variables that we will be using -- #

# list of years we have:
YEARS = ['2016', '2017', '2018', '2019', '2020', '2021', '2022', '2023']

# dictionary of countries (key) we have and their codes (value):
COUNTRY_CODES = {
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

# list of pollutants we have:
POLLUTANTS = ['pm10', 'pm25', 'so2', 'o3', 'co', 'no2', 'no', 'bc']

# data file path:
DATA_PATH_FROM = '0.Data/API_DATA'
DATA_PATH_TO = '0.Data/API_DATA_CONCAT'


# -- code -- #

# defining the concating function:
def concat_csv(path_from: str, path_to: str, pollutants: list, year: int, country_code: dict):

    # create an empty list so we cann append all the rows of each csv in a certain year to it:
    dfs = []
    
    # looping over each country and the pollutants to append them into the list dfs so we can then have them in one csv
    for country in country_code:
        for pollutant in pollutants:

            # since we have the files formatted in a uniform manner we are using the manner her to get the file names
            # without mannually inputting them
            file_name = f"{year}_{country_code[country]}_{pollutant}.csv"
            
            # checking if the path to the file exists (since not all combinations we have files since there was no data available for ex.)
            if os.path.exists(path_from+'\\'+file_name):
                df = pd.read_csv(path_from+'\\'+file_name) # creating a df from the csv file
                dfs.append(df) # append the df to the list

            else:
                print(f"File {file_name} not found.") 
        
    if dfs: # if the df is not empty
        combined_df = pd.concat(dfs, axis=0) #combine the df on the level of rows
        output_file = os.path.join(path_to, f"{year}.csv") # get the path where the csv will be in
        combined_df.to_csv(output_file, index=False) # turn to csv and save (by year)
        print(f"Combined file saved as {output_file}")
    
    else:
        print(f"No files found for {year} {country_code}")


# -- running the function -- #
for year in YEARS:
    concat_csv(DATA_PATH_FROM, DATA_PATH_TO, POLLUTANTS, year, COUNTRY_CODES)