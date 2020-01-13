FROM python:3.7

ENV GOOGLE_APPLICATION_CREDENTIALS=/app/tests/dummy_credentials.json

WORKDIR /app

COPY requirements.txt ./

RUN pip install -r requirements.txt

COPY . ./

CMD ["pytest"]
