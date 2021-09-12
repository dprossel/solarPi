# syntax=docker/dockerfile:1
FROM python:3
ENV PYTHONUNBUFFERED=1
WORKDIR solarpi
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY main.py .
CMD ["main.py"]
ENTRYPOINT ["python"]
