# CSCI 422 Project - Mitchell Borders

## Overall Goal

The goal of this project is to analyze, on a state-by-state basis, how weather influences electricity costs. The ultimate objective is to use analytics to gain insights into these relationships.

## Ingestion

During the ingestion phase, three data sources are utilized. One originates from a static online source, while the other two are accessed through APIs that provide dynamic data that updates daily (or quarterly in certain cases).

1. **U.S. Energy Information Administration (U.S. EIA):**

   - API Link: [U.S. EIA API](https://www.eia.gov/opendata/index.php)
   - Data: Electricity and energy data
   - Storage: ElectricityDataPure.csv in the `weatherelectricity` store, electricity blob in ADLS

2. **National Oceanic and Atmospheric Administration (NOAA) - Weather Data:**

   - API Link: [NOAA API](https://www.ncdc.noaa.gov/cdo-web/webservices/v2)
   - Data: Weather data
   - Storage: WeatherDataPure.csv `weatherelectricity` store, weather blob in ADLS

3. **National Oceanic and Atmospheric Administration (NOAA) - Weather Station Mapping:**
   - Data Link: [Weather Station Mapping](https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt)
   - Data: Static mapping of weather stations to state codes
   - Storage: ghcnd_stations.txt in the `weatherelectricity` store, weather blob in ADLS

## Transformation

The `src/transformation` directory houses scripts and modules responsible for converting raw data stored in ADLS into a format for analysis. This involves data cleaning, converting data types, and creating new features.

Key transformations include:

- Converting the 'date' column to a 'yyyy-MM' format and setting it as the index for easier time-series analysis.
- Adding a 'region' column to the weather data based on the 'state_code' column, allowing for regional analysis.
- Grouping the data by 'state_code' and 'date' (by month) and calculating the average for each group, providing a monthly overview of the weather and electricity data.
- Creating a common date DataFrame containing only rows where the date is present in both the weather and electricity data, ensuring consistency across datasets.

These transformations ensure the data is in a clean, consistent format suitable for further analysis and modeling.

## Serving/Analytics

The `serving/analytics` directory is responsible for serving the transformed data and performing analytics. This includes creating visualizations, and generating reports.

Key features include:

- Using PowerBI to visualize monthly weather and electricity data by state and region, offering insights into trends and regional differences.
- Generating reports summarizing key statistics and findings from the data.

PowerBI serves as the primary tool for data visualization, providing an interactive and user-friendly exploration of the data.
