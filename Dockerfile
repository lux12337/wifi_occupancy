FROM python:3
ADD . /code
WORKDIR /code
RUN pip install pandas sqlalchemy requests ConfigParser argparse
CMD ["python", "push_to_remote_db.py"]
