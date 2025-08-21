FROM python:3.12-slim

WORKDIR /app

COPY dbt/ /app/

RUN pip install --upgrade pip && \
    pip install "dbt-core==1.9.2" "dbt-snowflake==1.9.1"

CMD ["bash"]
