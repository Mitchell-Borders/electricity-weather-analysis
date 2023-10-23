# DataSet2.py - script to extract data from its source and load into ADLS.

import requests
import json
import pandas as pd
import sys

def get_eia_api_key():
    with open('src/ingestion/eia_api_key.config') as f:
        api_key = f.readline()
    return api_key


def get_electricity_data():

    url = 'https://api.eia.gov/v2/electricity/retail-sales/data/'
    params = {
        'api_key': get_eia_api_key(),
        'frequency': 'monthly',
        'data[0]' : 'customers',
        'data[1]' : 'price',
        'data[2]' : 'revenue',
        'data[3]' : 'sales',
        'start': '2018-06',
        'end': '2023-06',
        'sort[0][column]' : 'period',
        'sort[0][direction]' : 'desc',
        'offset' : '0',
        'length' : '5000',
    }

    response = requests.get(url, params=params)
    response_json = json.loads(response.text)['response']
    total_count = response_json['total']
    data = response_json['data']

    for offset in range(5000, total_count + 1, 5000):
        params['offset'] = offset
        print(f"Fetching data for offset {offset}")
        response = requests.get(url, params=params)
        response_json = json.loads(response.text)['response']
        data.extend(response_json['data'])

    return data

def convert_to_csv(data):
    """
    Converts the data to a CSV format
    """
    df = pd.DataFrame(data)
    df.to_csv('src/ingestion//ingestedData/ElectricityDataPure.csv', index=False)

from azure.storage.filedatalake import DataLakeServiceClient


def initialize_storage_account(storage_account_name, storage_account_key):
    """
    Initialize the azure storage account
    :param storage_account_name: Azure storage account name
    :param storage_account_key: Azure storage account key
    """
    try:
        global service_client

        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)
    except Exception as e:
        print(e)

    if service_client:
        print("Service Client established successfully")

def upload_file_to_adls(file_system_client, directory_name, file_name_to_write, local_file_name):
    """
    Uploads a file to the storage account
    :param file_system_client: The file system to upload to
    :param directory_name: The directory to upload to
    :param file_name_to_write: The name of the file to write to in adls
    :param local_file_name: The name of the file to upload
    """
    directory_client = file_system_client.get_directory_client(directory_name)
    file_client = directory_client.create_file(file_name_to_write)
    opened_local_file = open(local_file_name, 'r')
    local_file_contents = opened_local_file.read()
    file_client.upload_data(local_file_contents, overwrite=True)
    print("File uploaded to ADLS")

def upload_weather_data_to_adls():
    try:
        # Check if file system and directory already exist
        file_system_name = "ingestion"
        directory_name = "electricity"
        file_system_client = service_client.get_file_system_client(file_system_name)
        directory_client = file_system_client.get_directory_client(directory_name)
        if not file_system_client.exists():
            # Create the file system if it doesn't exist
            file_system_client = service_client.create_file_system(file_system=file_system_name)
        if not directory_client.exists():
            # Create the file system and directory if they don't exist
            directory_client = file_system_client.create_directory(directory_name)

        # Upload the file to the directory
        upload_file_to_adls(file_system_client, directory_name, "ElectricityDataPure.csv",
                            "src/ingestion/ingestedData/ElectricityDataPure.csv")

    except Exception as e:
        print(e)

def get_adls_key():
    with open('src/ingestion/adls_account_key.config', 'r') as f:
        api_key = f.readline()
        return api_key

if __name__ == "__main__":
    # Fetch data from NOAA's API
    max_retries = 5
    retries = 0
    while retries < max_retries:
        try:
            #data = get_electricity_data()
            break
        except Exception as e:
            print(f"Error fetching data from NOAA's API: {e}")
            retries += 1
            if retries == max_retries:
                print(f"Max retries ({max_retries}) reached. Exiting.")
                sys.exit(1)
    # Convert data to CSV
    #convert_to_csv(data)
    # Write csv to ADLS
    storage_account_name = "weatherelectricitystore"
    storage_account_key = get_adls_key()
    initialize_storage_account(storage_account_name, storage_account_key)
    upload_weather_data_to_adls()

