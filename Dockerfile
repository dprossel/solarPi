# syntax=docker/dockerfile:1
FROM python:3

RUN groupadd -g 999 solarpi &&\
    useradd -r -u 999 -g solarpi solarpi &&\
    mkdir -p /home/solarpi &&\
    chown -R solarpi:solarpi /home/solarpi
USER solarpi

ENV PYTHONUNBUFFERED=1
ENV PATH="/home/solarpi/.local/bin:${PATH}"

WORKDIR /home/solarpi
COPY --chown=solarpi:solarpi requirements.txt .
RUN pip install --user -r requirements.txt
COPY --chown=solarpi:solarpi . .
RUN pip install --user . && rm -rf * .git* .cache

