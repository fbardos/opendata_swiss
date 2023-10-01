import folium
import streamlit as st
from streamlit_folium import st_folium


st.set_page_config(layout="wide")
st.title('OpenData.swiss - Tags Network')
st.markdown("""
    Shows, how often tags get mentioned together in one CKAN Package from [opendata.swiss](https://opendata.swiss). The data was exported on `2023-09-30`.
    Edges (connection between 2 tags) get only displayed, when they have 2 or more occurences. The more mentions between two Tags, the tighter the connection.
    Find out how the graph is built on my [blog](https://bardos.dev/tags-network.html).
""")

st.header('Network')
map = folium.Map(
    zoom_start=4,
    location=[50, -70],
    tiles='https://raw.githubusercontent.com/fbardos/opendata_swiss/master/tags_network/{z}/{x}/{y}.png',
    attr='opendata.swiss keywords',
    max_zoom=9,
    min_zoom=4,
)
st_data = st_folium(map, width=1280, height=800)

st.header('Legend')
st.markdown("""
    For each node, the organization which uses this tag the most gets calculated. The top 20 organizations get a random color which is displayed below:
""")
st.image('https://raw.githubusercontent.com/fbardos/opendata_swiss/master/tags_network/legend.png')
