FROM python:3.6-jessie

COPY . /app
WORKDIR /app

RUN pip3 install -e .

RUN pip3 install pytest

CMD ["pytest", "tests/", "-vv"]
