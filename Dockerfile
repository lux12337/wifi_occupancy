FROM python:3
ADD . /code
WORKDIR /code
RUN apt-get update
RUN pip install --upgrade pip
RUN pip install pandas sqlalchemy requests ConfigParser argparse
CMD ["python", "get_wifi_data.py"]
CMD ["python", "push_to_remote_db.py"]
