# syntax=docker/dockerfile:1
FROM python:3

RUN groupadd -g 999 solarpiuser && useradd -r -u 999 -g solarpiuser solarpiuser
USER solarpiuser

ENV PYTHONUNBUFFERED=1
WORKDIR /solarpi
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
RUN pip install . && rm -rf *

