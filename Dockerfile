FROM osgeo/gdal:ubuntu-small-3.3.2

RUN apt-get update \
    && apt-get install -y \
    build-essential \
    git \
    python3-pip \
    # For Psycopg2
    libpq-dev python-dev \
    && apt-get autoclean && \
    apt-get autoremove && \
    rm -rf /var/lib/{apt,dpkg,cache,log}

COPY requirements.txt /tmp/
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /tmp/requirements.txt

RUN mkdir -p /code
WORKDIR /code

COPY . /code/

RUN echo "__version__ = '$(python /code/setup.py --version | sed s/+/./g)'" > /code/tools/_version.py

RUN pip install /code

CMD ["python", "--version"]