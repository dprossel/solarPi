# syntax=docker/dockerfile:1
FROM python:3
ENV PYTHONUNBUFFERED=1
WORKDIR /solarpi
COPY requirements.txt /solarpi/
RUN pip install -r requirements.txt
COPY . /solarpi/

