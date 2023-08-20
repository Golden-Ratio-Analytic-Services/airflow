# ETL

## Running

To run the airflow deployment, run the following from the root directory and visit http://0.0.0.0:8080/login/

```bash
docker-compose up
```

## DAGS

### process-highway-geometries

This dag is responsible for downloading highway geometries, saving them to disk in a database ready format, and loading them into the database.

### process-city-points

This dag is responsible for downloading the point geometries of cities, saving them to disk, and uploading them to the database

### process-gob

This dag is responsible for extracting data from [GOB](https://www.gob.mx/sesnsp/acciones-y-programas/incidencia-delictiva-del-fuero-comun-nueva-metodologia), saving it to disk, and then uploading to the database

## Developing

Install development dependencies with

```python
python3 -m install -r requirements.txt
```

### Linting

Before submitting code for review, lint it with the following commands

```python
python3 -m black .
```

```python
python3 -m isort .
```

### Pull Requests

Pull requests should be made into the `develop` branch from feature branches.