FROM python:3.9.18-bookworm

RUN pip install --upgrade pip

WORKDIR /kafka_TP

COPY requirements.txt /kafka_TP/

RUN pip install --no-cache-dir -r requirements.txt

COPY assets/ /kafka_TP/
COPY dash_app.py /kafka_TP/
COPY weatherAdmin.py /kafka_TP/
COPY weatherLaunch.py /kafka_TP/
COPY weatherConsumer.py /kafka_TP/
COPY weatherProducer.py /kafka_TP/
COPY .env /kafka_TP/

EXPOSE 8050

# this command will create topics and launch the producer and consumer
# ENTRYPOINT [ "python3", "weatherLaunch.py" ]
ENTRYPOINT [ "python3", "-u", "weatherLaunch.py" ]
