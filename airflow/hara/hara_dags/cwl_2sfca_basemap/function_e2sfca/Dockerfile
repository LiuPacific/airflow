FROM python:3.9.17-slim

WORKDIR /app

COPY function_e2sfca.py .

RUN pip install cligj==0.7.2 fiona==1.9.6 geopandas==0.13.2 pyproj==3.5.0 shapely==2.0.4 mapclassify==2.6.0
# RUN pip install argon2-cffi==23.1.0 argon2-cffi-bindings==21.2.0 minio==7.2.7 pycryptodome==3.20.0

ENTRYPOINT ["python", "/app/function_e2sfca.py"]

# docker build -t function_e2sfca .
# docker run --name function_e2sfca --rm function_e2sfca "osu-geohealth" "ok_od_travel_3hour.csv"

