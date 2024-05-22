
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


