FROM osgeo/gdal:ubuntu-small-3.4.2 as base

ENV CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

ENV DEBIAN_FRONTEND=noninteractive \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8

RUN apt-get update \
    && apt-get install -y \
        # Developer convenience
        git \
        fish \
        wget \
        unzip \
        # Build tools\
        build-essential \
        python3-pip \
        python3-dev \
        # For Psycopg2
        libpq-dev \
        # Yaml parsing speedup, I think
        libyaml-dev \
        lsb-release \
        # For SSL
        ca-certificates \
    # Cleanup
    && apt-get autoclean \
    && apt-get autoremove \
    && rm -rf /var/lib/{apt,dpkg,cache,log}

# Setup PostgreSQL APT repository and install postgresql-client-13
RUN sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list' \
    && wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - \
    && apt-get update \
    && apt-get install -y postgresql-client-13 \
    && apt-get autoclean \
    && apt-get autoremove \
    && rm -rf /var/lib/{apt,dpkg,cache,log}

COPY requirements.txt /tmp/
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /tmp/requirements.txt \
    --no-binary rasterio \
    --no-binary shapely \
    --no-binary fiona

RUN mkdir -p /code
WORKDIR /code

COPY . /code/

RUN pip install /code

CMD ["python", "--version"]

FROM base as tests
RUN pip install --no-cache-dir -r /code/requirements-test.txt
