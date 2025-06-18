FROM python:3.10-slim

WORKDIR /app

COPY job.py .

RUN pip install -r requirements.txt

CMD ["python", "job.py"]