FROM python:3.9.17-slim

WORKDIR /app

COPY object_storage_download.py .

RUN pip install argon2-cffi==23.1.0 argon2-cffi-bindings==21.2.0 minio==7.2.7 pycryptodome==3.20.0

ENTRYPOINT ["python", "/app/object_storage_download.py"]

# docker build -t s3_downloading .
# docker run --name object_storage_download --network host --rm s3_downloading "osu-geohealth" "ok_od_travel_3hour.csv"


#docker \
#    run \
#    -i \
#    --mount=type=bind,source=/tmp/j6sbgeep,target=/wVbvQE \
#    --mount=type=bind,source=/tmp/3joeu6sg,target=/tmp \
#    --mount=type=bind,source=/home/typingliu/workspace/tpy/airflow25/airflow/airflow/hara/hara_dags/cwl_2sfca/ok_od_travel_3hour.csv,target=/var/lib/cwl/stg589d5e5d-c5ed-48ba-856a-6e4e3804c232/ok_od_travel_3hour.csv,readonly \
#    --mount=type=bind,source=/home/typingliu/workspace/tpy/airflow25/airflow/airflow/hara/hara_dags/cwl_2sfca/ok_zip_acc.geojson,target=/var/lib/cwl/stg17fe6534-a5e2-4039-804b-38bcb88b5e8d/ok_zip_acc.geojson,readonly \
#    --workdir=/wVbvQE \
#    --read-only=true \
#    --net=host \
#    --user=1000:1000 \
#    --rm \
#    --cidfile=/tmp/3wxjagio/20240602140416-079888.cid \
#    --env=TMPDIR=/tmp \
#    --env=HOME=/wVbvQE \
#    function_2sfca \
#    /var/lib/cwl/stg589d5e5d-c5ed-48ba-856a-6e4e3804c232/ok_od_travel_3hour.csv \
#    /var/lib/cwl/stg17fe6534-a5e2-4039-804b-38bcb88b5e8d/ok_zip_acc.geojson \
#    60
#/app/function_2sfca.py:134: FutureWarning: Downcasting object dtype arrays on .fillna, .ffill, .bfill is deprecated and will change in a future version. Call result.infer_objects(copy=False) instead. To opt-in to the future behavior, set `pd.set_option('future.no_silent_downcasting', True)`
#  oklahoma_zipcod

