[![Open in Visual Studio Code](https://classroom.github.com/assets/open-in-vscode-718a45dd9cf7e7f842a935f5ebbe5719a5e09af4491e668f4dbf3b35d5cca122.svg)](https://classroom.github.com/online_ide?assignment_repo_id=12429757&assignment_repo_type=AssignmentRepo)

# CSCI 422 Project - Mitchell Borders

<Tell the world about your project in this readme. There should be sections that align with the data engineering lifecyle - Ingestion, Transformation, Serving - that serve as the design documentation. >

# Overall goal

The goal of this project a state-by-state basis, determine how weather impacts electricity use and impacts electricity cost.

## Ingestion

<Design documentation for ingestion goes here. Where does the data come from? How is it ingested? Where is it stored (be specific)?>
For the ingestion phase there exists three data sources. One is from a static online source. The other two are accessed through APIs, and are dynamic, updating with data daily (depending on what data I am getting, sometimes it is quarterly).
The first is from the U.S. Energy Information Administration (U.S. EIA). Data from this source will support the electricity and energy data that is needed. The API can be found at this link: https://www.eia.gov/opendata/index.php and a csv will be stored at this file location in the weatherelectricity store in ADLS: ingestion/electricity/ElectricityDataPure.csv
The second is from the National Oceanic and Atmospheric Administration. Data from this source will support the weather data that is needed. The API can be found at this link: https://www.ncdc.noaa.gov/cdo-web/webservices/v2 and a csv will be stored at this file location in the weatherelectricity store in ADLS: ingestion/weather/WeatherDataPure.csv
The third is also from the National Oceanic and Atmospheric Administration. Data from this source will support a mapping function that maps weather stations to their respective state codes.The txt file with these mappings can be found at this link: https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt and will be stored at this file location in the weatherelectricity store in ADLS: ingestion/weather/ghcnd_stations.txt

Note - if you came here looking for assignment instructions, go to SupplementaryInfo\CourseInstructions
