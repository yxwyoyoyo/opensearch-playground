# Opensearch playground

## System dependencies

- [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/)
- [Python](https://www.python.org/) and [pip](https://pip.pypa.io/)

## Usage

1. Start OpenSearch. (Add option `-d` to run in background)
   ```shell
   docker-compose -f docker-compose.yml up
   ```
2. Config ENVs. (Customize configurations in `.env`)
   ```shell
   cp .env.example .env
   ```
3. Install requirements.
   ```shell
   pip install -r requirements.txt
   ```
4. Explore in jupyter notebook `opensearch.ipynb`. (Recommanded to edit first in `opensearch.py` and use `p2j` to generate a new jupyter notebook)
   ```shell
   p2j -o opensearch.py
   ```
