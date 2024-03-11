# 1. Base image
FROM bitnami/python:3.10.5

ARG APP_ENV='dev'
ARG SSH_TUNNEL='false'
ARG CODEARTIFACT_TOKEN

# 👇 python
ENV APP_DIR='/project' \
    APP_ENV=${APP_ENV} \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    POETRY_VERSION=1.7.1 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_CACHE_DIR='/var/cache/pypoetry'

# 👇
WORKDIR "$APP_DIR"

ENV PYTHONPATH "${PYTHONPATH}:/project"

RUN apt-get update -y && pip install "poetry==$POETRY_VERSION"

# 👇 just necessary files.
COPY ./poetry.lock .
COPY ./pyproject.toml .
COPY ./config.py .
COPY ./src/ /project/src/
COPY ./config/ /project/config/

# 👇 Install pytnon packages
RUN poetry install --no-interaction --no-ansi --no-dev

# 👇 Fixing Critical vulnerabilites - GHSA-xvch-5gv4-984h and CVE-2021-44906
CMD ["npm", "install", "--save", "minimist@1.2.6"]
