# ExploratoryAnalysis.py - script to do EDA (exploratory data analysis) on the two primary data sets for the project. 

# Note - alternative approaches can be used besides local Python code.  Large datasets may require Databricks.  
# You can also use ChatGPT or similar language model for this step. 

import pandas as pd

print('Weather data analysis:')
# Read the CSV file into a DataFrame
df = pd.read_csv('src/ingestion/ingestedData/WeatherDataPure.csv')

# Display the first 5 rows of the DataFrame
print(df.head())

# Display the shape of the DataFrame (number of rows and columns)
print(df.shape)

# Display the column names of the DataFrame
print(df.columns)

# Display summary statistics for the numerical columns in the DataFrame
print(df.describe())

print('Electricity data analysis:')

# Read the CSV file into a DataFrame
df = pd.read_csv('src/ingestion/ingestedData/ElectricityDataPure.csv')

# Display the first 5 rows of the DataFrame
print(df.head())

# Display the shape of the DataFrame (number of rows and columns)
print(df.shape)

# Display the column names of the DataFrame
print(df.columns)

# Display summary statistics for the numerical columns in the DataFrame
print(df.describe())