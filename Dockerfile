FROM python:3.10.10-slim-buster

WORKDIR /dbt

COPY ./*.yml ./
COPY ./macros ./macros
COPY ./models ./models
COPY ./dbt_project.yml ./


RUN pip install --upgrade pip
RUN pip install dbt-postgres

# Install git
RUN apt-get update -y && \
    apt-get install -y git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Run dbt deps
RUN dbt deps

ENTRYPOINT ["dbt"]
