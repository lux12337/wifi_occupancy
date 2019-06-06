FROM python:3-slim
ADD . /code
WORKDIR /code
RUN apt-get update
RUN pip install --upgrade pip
RUN pip install pandas \
                sqlalchemy \
                requests \
                ConfigParser \
                argparse \
                pydal \
                psycopg2-binary \
                influxdb \
                pymysql
