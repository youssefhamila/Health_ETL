FROM python:3.8

WORKDIR /app

COPY docker_requirements.txt .

RUN pip install --no-cache-dir -r docker_requirements.txt

COPY src/health_api.py .
COPY src/logger.py .

ENV FLASK_APP=src/health_api

CMD [ "python", "health_api.py" ]
