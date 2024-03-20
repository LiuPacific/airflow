FROM python:3.9.17-slim

WORKDIR /app

COPY visualizing_geojson_for_basemap.py .

RUN pip install contourpy==1.1.1 cycler==0.12.1 fonttools==4.52.4 kiwisolver==1.4.5 matplotlib==3.7.5 pillow==10.3.0 cligj==0.7.2 fiona==1.9.6 geopandas==0.13.2 pyproj==3.5.0 shapely==2.0.4 mapclassify==2.6.0


ENTRYPOINT ["python", "/app/visualizing_geojson_for_basemap.py"]

# docker build -t visualizing_geojson_for_basemap .
# docker run --name visualizing_geojson_for_basemap --rm visualizing_geojson_for_basemap "osu-geohealth" "ok_od_travel_3hour.csv"

