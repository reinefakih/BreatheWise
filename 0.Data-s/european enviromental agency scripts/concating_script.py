import pandas as pd
import os

def concat_csv(path_from: str, path_to: str):

    # create an empty list so we cann append all the rows of each csv in a certain year to it:
    dfs = []
    
    # checking if the path to the directory exists:
    if os.path.exists(path_from):
        files = os.listdir(path_from) # getting the files in the directory
        count = 0 # starting a count for validating that we went through all the files
        for file in files:
            if (os.path.isfile(path_from + "\\" + file)) and (os.path): # if the item in the directory is a file:
                df = pd.read_parquet(path_from + "\\" + file) # creating a df from the parquet file
                dfs.append(df) # append the df to the list
                count += 1 # append count (since the main job is done)
                print(count) # print the count to see where we are and to validate the final count

    else:
        print(f"File {path_from} not found.") 
        
    if dfs: # if the df is not empty
        combined_df = pd.concat(dfs, axis=0) #combine the df on the level of rows
        output_file = os.path.join(path_to, "air_pollution-2023-2024.csv") # get the path where the csv will be in
        combined_df.to_csv(output_file, index=False) # turn to csv and save (by year)
        print(f"Combined file saved as {output_file}")
    
    else:
        print(f"No files found")


# -- running the function -- #
concat_csv("0.Data\\air pollution data\\parquets", ".")
