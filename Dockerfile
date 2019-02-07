FROM python:2.7-slim
ADD . /code
WORKDIR /code
RUN pip install pandas sqlalchemy asn1crypto
CMD ["python", "push_to_remote_db.py"]
