# ops




# middleware
## nginx


- `bridge` mode

```shell
docker run \
--volume /srv/nginx/:/etc/nginx/ \
-p 18080:8081 \
-p 8443:443 \
--name typing_nginx \
--restart on-failure:5 \
--add-host=host.docker.internal:host-gateway \
-d \
nginx:1.23.2
```

- `host` mode

```shell
docker run \
--volume /srv/nginx/:/etc/nginx/ \
--volume /home/typingliu/temp:/airflow/cwl/output/ \
--name typing_nginx \
--restart on-failure:5 \
--network host \
-d \
nginx:1.23.2
# -p 18081:8081 -p 8443:443 \ # not necessary and has no effect.
```

## minio

```shell
docker run -d                                  \
  -v /srv/minio_workflow/single:/data/minio/single                             \
  -e "MINIO_ROOT_USER=osu"    \
  -e "MINIO_ROOT_PASSWORD=osugeo123"    \
  -e "MINIO_BROWSER_REDIRECT_URL=http://127.0.0.1:9090"    \
  --name "minio_workflow"  \
  --restart on-failure:5  \
  --network host   \
  minio/minio:RELEASE.2024-05-28T17-19-04Z server /data/minio/single --console-address ":9090"

# -p 19000:9000 -p 19090:9090                     \
```




http://127.0.0.1:9090

# db
## postgresql

hara_postgres15



```sql
create table public.hara_serialized_dag
(
  dag_id           varchar(250)             not null
    primary key,
  fileloc          varchar(2000)            not null,
  fileloc_hash     bigint                   not null,
  data             json,
  data_compressed  bytea,
  dag_hash         varchar(32)              not null,
  processor_subdir varchar(2000),
  dag_owner        varchar(250)             null,
  cwl_json         json null, -- {"main_cwl":{}, "command_line_tools":{"s3_downloading.cwl.yaml": {}}}
  create_time      timestamp with time zone not null,
  last_updated     timestamp with time zone not null,
  row_status       smallint                 not null default 0
);

alter table public.hara_serialized_dag
  owner to airflow_user;

create index idx_fileloc_hash
  on public.hara_serialized_dag (fileloc_hash);


--

create or replace function typing_update_hara_serialized_dag_time()
returns trigger as $tp$
begin
        new.last_updated = current_timestamp;
        return new;
end
$tp$ language plpgsql;

DROP TRIGGER typing_update_time_trigger ON hara_serialized_dag;

create trigger typing_update_time_trigger
  before update or insert
  on hara_serialized_dag  -- 要用before, 因为function中会修改new, 然后返回到这里执行最终update.
  for each row
  execute procedure typing_update_hara_serialized_dag_time();



-- update hara_serialized_dag set row_status = 2 where id = 7;





create or replace function typing_create_hara_serialized_dag_time()
    returns trigger as
$tp$
begin
    new.create_time = current_timestamp;
return new;
end
$tp$ language plpgsql;

create trigger typing_create_time_trigger
  before insert
  on public.hara_serialized_dag
  for each row
  execute procedure public.typing_create_hara_serialized_dag_time();



-- 显示该table的trigger
SELECT tgname FROM pg_trigger, pg_class WHERE tgrelid=pg_class.oid AND relname='hara_serialized_dag';

```
