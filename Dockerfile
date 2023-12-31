FROM apache/airflow:2.3.3
COPY requirements.txt .
RUN pip install -r requirements.txt
USER root
RUN apt-get update && apt-get install git -y
USER airflow
# COPY ["/dbt","/opt/dbt"]
# COPY ["/dbt_project.yml", "/opt/dbt/"]
# COPY ["/profiles-example.yml", "/home/airflow/.dbt/profiles.yml"]
# WORKDIR /opt/
# RUN ["echo", "***********************************"]
