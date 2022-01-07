# syntax=docker/dockerfile:1
FROM python:3
ENV PYTHONUNBUFFERED=1
WORKDIR /solarpi
COPY requirements.txt .
RUN pip install -r requirements.txt && rm -rf requirements.txt .cache
COPY main.py .
CMD ["main.py"]
ENTRYPOINT ["python"]
