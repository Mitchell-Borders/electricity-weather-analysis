import pandas as pd
from azure.storage.filedatalake import DataLakeFileClient
from azure.storage.filedatalake import DataLakeServiceClient
from io import BytesIO


def get_adls_key():
    with open('src/ingestion/adls_account_key.config', 'r') as f:
        api_key = f.readline()
        return api_key


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


def get_weather_df():
    # Read the CSV file from azure into a DataFrame
    file_system_name = "ingestion"
    directory_name = "weather"
    file_name = "WeatherDataPure.csv"  # specify the name of the CSV file
    file_system_client = service_client.get_file_system_client(
        file_system_name)
    file_client = file_system_client.get_file_client(
        f"{directory_name}/{file_name}")

    download_stream = file_client.download_file()
    df = pd.read_csv(BytesIO(download_stream.readall()))

    # remove the 'GHCND:' from the station column so it can be mapped later
    df['station'] = df['station'].str.replace('GHCND:', '')

    # rename value column to average_temp
    df.rename(columns={'value': 'average_temp'}, inplace=True)

    return df


def get_electricity_df():
    # Read the CSV file from azure into a DataFrame
    file_system_name = "ingestion"
    directory_name = "electricity"
    file_name = "ElectricityDataPure.csv"  # specify the name of the CSV file
    file_system_client = service_client.get_file_system_client(
        file_system_name)
    file_client = file_system_client.get_file_client(
        f"{directory_name}/{file_name}")

    download_stream = file_client.download_file()
    df = pd.read_csv(BytesIO(download_stream.readall()))

    return df


def get_ghcnd_stations_df():
    # Read the CSV file from azure into a DataFrame
    file_system_name = "ingestion"
    directory_name = "weather"
    file_name = "ghcnd_stations.csv"  # specify the name of the CSV file
    file_system_client = service_client.get_file_system_client(
        file_system_name)
    file_client = file_system_client.get_file_client(
        f"{directory_name}/{file_name}")

    download_stream = file_client.download_file()
    df = pd.read_csv(BytesIO(download_stream.readall()))

    return df


def map_stations_to_state_code(weather_df):
    us_state_codes = [
        'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
        'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
        'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
        'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
        'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
    ]
    mapping_dict = {}
    # create a dictionary of stations to state codes
    with open("src/ingestion/ingestedData/ghcnd_stations.txt") as f:
        for line in f:
            line_info = line.split()
            if line_info[4] in us_state_codes:
                mapping_dict[line_info[0]] = line_info[4]
    # where station in weather_df equals key in mapping_dict, add a column to weather_df with the value of the key
    weather_df['state_code'] = weather_df['station'].map(mapping_dict)

    return weather_df


def add_region_column(weather_df):
    region_dict = {
        'New England': ['CT', 'ME', 'MA', 'NH', 'RI', 'VT'],
        'Middle Atlantic': ['NJ', 'NY', 'PA'],
        'East North Central': ['IL', 'IN', 'MI', 'OH', 'WI'],
        'West North Central': ['IA', 'KS', 'MN', 'MO', 'NE', 'ND', 'SD'],
        'South Atlantic': ['DE', 'FL', 'GA', 'MD', 'NC', 'SC', 'VA', 'DC', 'WV'],
        'East South Central': ['AL', 'KY', 'MS', 'TN'],
        'West South Central': ['AR', 'LA', 'OK', 'TX'],
        'Mountain': ['AZ', 'CO', 'ID', 'MT', 'NV', 'NM', 'UT', 'WY'],
        'Pacific Contiguous': ['CA', 'OR', 'WA'],
        'Pacific Noncontiguous': ['AK', 'HI']
    }

    # Invert the dictionary so that each state maps to a region
    state_to_region = {state: region for region,
                       states in region_dict.items() for state in states}

    weather_df['region'] = weather_df['state_code'].map(state_to_region)

    return weather_df


def average_weather_data(weather_df):
    # Convert the 'date' column to datetime
    weather_df['date'] = pd.to_datetime(weather_df['date'])

    # Format the 'date' column to 'yyyy-MM'
    weather_df['date'] = weather_df['date'].dt.to_period('M')

    # Group by 'state_code' and 'date', and calculate the average for each group
    monthly_average = weather_df.groupby(
        ['state_code', 'date']).mean().reset_index()

    return monthly_average


def create_common_date_df(weather_df, electricity_df):
    weather_df['date'] = weather_df['date'].astype(str)

    weather_dates = weather_df['date'].unique().tolist()

    electricity_dates = electricity_df['period'].unique().tolist()

    # Create a list of dates that are in both the weather_df and electricity_df
    common_dates = [
        date for date in weather_dates if date in electricity_dates]

    # Create a new DataFrame that only contains rows where the date is in common_dates
    common_date_df = weather_df[weather_df['date'].isin(common_dates)].copy()

    # keep only the date column, and drop duplicate row values
    common_date_df.drop(columns=['average_temp'], inplace=True)
    common_date_df.drop_duplicates(inplace=True)

    return common_date_df


def transform_eletricity_data(electricity_df):
    # Don't need these columns, same data in every single row for them.
    electricity_df.drop(columns=[
                        'customers-units', 'price-units', 'revenue-units', 'sales-units'], inplace=True)

    return electricity_df


def add_common_primary_key(common_date_df, electricity_df, weather_df):
    # add a primary key to all dataframes based on date and state_code columns
    common_date_df['common_date_id'] = common_date_df['date'].astype(str) + \
        common_date_df['state_code']
    electricity_df['common_date_id'] = electricity_df['period'].astype(str) + \
        electricity_df['stateid']
    weather_df['common_date_id'] = weather_df['date'].astype(str) + \
        weather_df['state_code']


def create_local_transformed_data_files(weather_df, electricity_df, common_date_df):
    # Create a CSV file for each DataFrame
    weather_df.to_csv(
        'src/transformation/transformed_data/DIM_WeatherData.csv', index=False)
    electricity_df.to_csv(
        'src/transformation/transformed_data/DIM_ElectricityData.csv', index=False)
    common_date_df.to_csv(
        'src/transformation/transformed_data/FACT_CommonDates.csv', index=False)


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


def upload_transformed_data_to_adls():
    try:
        # Check if file system and directory already exist
        file_system_name = "transformation"
        directory_name = "weather"
        file_system_client = service_client.get_file_system_client(
            file_system_name)
        directory_client = file_system_client.get_directory_client(
            directory_name)
        if not file_system_client.exists():
            # Create the file system if it doesn't exist
            file_system_client = service_client.create_file_system(
                file_system=file_system_name)
        if not directory_client.exists():
            # Create the file system and directory if they don't exist
            directory_client = file_system_client.create_directory(
                directory_name)

        # Upload the file to the directory
        upload_file_to_adls(file_system_client, directory_name, "DIM_WeatherData.csv",
                            "src/transformation/transformed_data/DIM_WeatherData.csv")

    except Exception as e:
        print(e)

    try:
        # Check if file system and directory already exist
        file_system_name = "transformation"
        directory_name = "electricity"
        file_system_client = service_client.get_file_system_client(
            file_system_name)
        directory_client = file_system_client.get_directory_client(
            directory_name)
        if not file_system_client.exists():
            # Create the file system if it doesn't exist
            file_system_client = service_client.create_file_system(
                file_system=file_system_name)
        if not directory_client.exists():
            # Create the file system and directory if they don't exist
            directory_client = file_system_client.create_directory(
                directory_name)

        # Upload the file to the directory
        upload_file_to_adls(file_system_client, directory_name, "DIM_ElectricityData.csv",
                            "src/transformation/transformed_data/DIM_ElectricityData.csv")

    except Exception as e:
        print(e)

    try:
        # Check if file system and directory already exist
        file_system_name = "transformation"
        directory_name = "commonDate"
        file_system_client = service_client.get_file_system_client(
            file_system_name)
        directory_client = file_system_client.get_directory_client(
            directory_name)
        if not file_system_client.exists():
            # Create the file system if it doesn't exist
            file_system_client = service_client.create_file_system(
                file_system=file_system_name)
        if not directory_client.exists():
            # Create the file system and directory if they don't exist
            directory_client = file_system_client.create_directory(
                directory_name)

        # Upload the file to the directory
        upload_file_to_adls(file_system_client, directory_name, "FACT_CommonDates.csv",
                            "src/transformation/transformed_data/FACT_CommonDates.csv")
    except Exception as e:
        print(e)


if __name__ == '__main__':
    storage_account_name = "weatherelectricitystore"
    storage_account_key = get_adls_key()
    initialize_storage_account(storage_account_name, storage_account_key)
    weather_df = get_weather_df()
    print(weather_df.head())
    electricity_df = get_electricity_df()
    print(electricity_df.head())
    # Transform weather dataset
    weather_df = map_stations_to_state_code(weather_df)
    weather_df = add_region_column(weather_df)
    weather_df = average_weather_data(weather_df)
    # Transform electricity dataset
    electricity_df = transform_eletricity_data(electricity_df)
    # Create common date dataset for star schema FACT table
    common_date_df = create_common_date_df(weather_df, electricity_df)
    # Add common single primary keys to all dataframes
    add_common_primary_key(common_date_df, electricity_df, weather_df)
    # Upload transformed data to ADLS
    create_local_transformed_data_files(
        weather_df, electricity_df, common_date_df)
    upload_transformed_data_to_adls()
