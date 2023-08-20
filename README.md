# ETL

## Running

To run the airflow deployment, run the following from the root directory and visit http://0.0.0.0:8080/login/

```bash
docker-compose up
```

## DAGS

### extract-highway-geometries

This dag is responsible for downloading highway geometries and saving them to disk

### extract-city-points

This dag is responsible for extracting the point geometries of cities and saving them to disk

### extract-gob

This dag is responsible for extracting data from [GOB](https://www.gob.mx/sesnsp/acciones-y-programas/incidencia-delictiva-del-fuero-comun-nueva-metodologia)
