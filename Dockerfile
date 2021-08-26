FROM osgeo/gdal:ubuntu-small-3.3.1

RUN apt-get update \
    && apt-get install -y build-essential git python3-pip \
    && apt-get autoclean \
    && apt-get autoremove

COPY requirements.txt /tmp/
RUN python -m pip install --upgrade pip \
    && pip install -r /tmp/requirements.txt

RUN mkdir -p /code
WORKDIR /code

ADD . /code/

RUN pip install /code

CMD ["python --version"]
