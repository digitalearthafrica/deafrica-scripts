FROM osgeo/gdal:ubuntu-small-3.3.2

RUN apt-get update \
    && apt-get install -y \
    # Build tools
    build-essential \
    git \
    python3-pip \
    # For Psycopg2
    libpq-dev python3-dev \
    # For SSL
    ca-certificates \
    && apt-get autoclean && \
    apt-get autoremove && \
    rm -rf /var/lib/{apt,dpkg,cache,log}

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
