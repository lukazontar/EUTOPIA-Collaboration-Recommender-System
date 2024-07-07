# EUTOPIA: Collaboration Recommender System

**Author:** *Luka Žontar*

<hr/>

## Introduction

This repository contains the code for the EUTOPIA collaboration recommender system.

In today's academic landscape, collaboration across disciplines and institutions is crucial due to complex scientific
papers. Yet, such collaboration is often underused, leading to isolated researchers and few connected hubs. This thesis
aims to create a system for proposing new partnerships based on research interests and trends, enhancing academic
cooperation. It focuses on building a network from scientific co-authorships and a recommender system for future
collaborations. Emphasis is on improving the EUTOPIA organization by fostering valuable, interdisciplinary academic
relationships.

<hr/>

## Setting up the environment

Environment stack:

- Python, SQL as main programming languages.
- [Mage AI](https://www.mage.ai/): an open-source data ingestion and transformation framework.
    - Using a dockerized version with
      docker-compose: [GitHub: mage-ai/compose-quickstart](https://docs.mage.ai/getting-started/setup).
- [BigQuery](https://cloud.google.com/bigquery): as the main data storage and processing engine.
- [dbt](https://www.getdbt.com/): as the main data transformation and modeling tool.

### Prerequisites

- Docker
- Python 3.10 (using [pyenv](https://github.com/pyenv-win/pyenv-win)
  and [venv](https://docs.python.org/3/library/venv.html))
    - For PyMC you need to have a *conda* environment.
- Accounts for: Google Cloud Platform, Mage AI, dbt Cloud

To run Python scripts, you need to install the requirements:

```bash
pip install -r requirements.txt
```

### Running Mage AI

To run Mage AI, you simply run the following command in the root of the repository:

```bash
docker-compose up
```

After that you can access the Mage AI UI at [http://localhost:6789](http://localhost:6789).

### Transforming data with dbt

To run `dbt` you need to have the `dbt` CLI installed. You can install it with the following command:

```bash
pip install dbt
pip install dbt-bigquery
```

After that you can execute the following command to run the models:

```bash
dbt run
```

<hr/>

## Additional Docs

Check additional documentation in the `docs` directory.

### Credentials

[Credentials](docs/data/Credentials.md): how to set up credentials for the different services.

### Data

1. [BigQuery Setup](docs/data/01%20-%20BigQuery%20Setup.md): how to set up BigQuery.
2. [Data Ingestion](docs/data/02%20-%20Data%20Ingestion.md): how to run data ingestion pipelines (both historical and
   incremental).
3. [Data Transformation](docs/data/03%20-%20Data%20Transformation.md): how to run data transformation pipelines
   in `dbt`.

### Development environment

```bash
conda env export --no-builds | findstr /V "^prefix: " > environment.yml
```

```bash
pip list --format=freeze > prod_requirements.txt
```

Filter out only the essential packages in `prod_requirements.txt`.

```bash
docker build --no-cache --tag eutopia_image_1 .
```

```bash
docker tag eutopia_image_1:latest lukazontar1/eutopia-recommender-system:version_1
```

```bash
docker push lukazontar1/eutopia-recommender-system:version_1
```

### Production environment

```bash
gcloud compute ssh --ssh-key-file C:\Users\LukaŽontar\lukazontar-pvt-eutopia eutopia-1
```

```bash
gcloud iam service-accounts keys create ./secrets/service_account_key.json --iam-account=eutopia-recommender-system@collaboration-recommender.iam.gserviceaccount.com
```

```bash
docker-compose up -d
```

To execute the test script:

```bash
docker exec -it eutopia-collaboration-recommender-system-eutopia_service_1-1 /bin/bash -c "source venv/bin/activate && python scripts/embedding/embed_articles.py"
```

To connect to the container:

```bash
docker exec -it eutopia-collaboration-recommender-system-eutopia_service_1-1 /bin/bash
```

[//]: # (docker run -dit \)

[//]: # (  -v ./secrets/service-account-key.json:~/service-account-key.json \)

[//]: # (  -e GOOGLE_APPLICATION_CREDENTIALS=~/service-account-key.json \)

[//]: # (  --name eutopia_container_1 eutopia_image_1)

[//]: # (docker exec -it eutopia_container_1 /bin/bash)
