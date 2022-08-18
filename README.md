# Metadata Analysis for opendata.swiss

This repository contains the code and utils for a metadata analysis for [opendata.swiss](http://opendata.swiss).

You can find the corresponding Jupyter Notebook here: [kaggle](https://www.kaggle.com/code/ogdfabian/metadata-analysis-for-opendata-swiss)

## Directory structure:
* `output/`: Output json files from API extraction from [CKAN API](http://docs.ckan.org/en/latest/api/index.html)
* `tags_network/`: Stored tiles for [folium](https://python-visualization.github.io/folium/) / [leaflet](https://leafletjs.com/).
* `gephi/`: Contains project file for [gephi](https://gephi.org/). Gephi is well suited for network graphs.
* `dag_opendata_swiss.py`: Contains [Apache Airflow](https://airflow.apache.org/) DAG for data extraction.
