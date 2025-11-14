# gdal:ubuntu-small no longer comes with netcdf support compiled into gdal
FROM ghcr.io/osgeo/gdal:ubuntu-full-3.11.0 AS base

ENV CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt\
    DEBIAN_FRONTEND=noninteractive \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    USE_PYGEOS=0 \
    GS_NO_SIGN_REQUEST=YES

RUN apt-get update \
    && apt-get install -y \
        # Developer convenience
        git \
        fish \
        wget \
        unzip \
        # Build tools\
        build-essential \
        python3-dev \
        python3-full \
        # For Psycopg2
        libpq-dev \
        # Yaml parsing speedup, I think
        libyaml-dev \
        lsb-release \
        # For SSL
        ca-certificates \
        jq \
        # Postgres
        postgresql \
        postgresql-client \
        # For shapely wheel
        libgeos-dev \
    # Cleanup
    && apt-get autoclean \
    && apt-get autoremove \
    && rm -rf /var/lib/{apt,dpkg,cache,log}

# Install yq
RUN wget https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64 -O /usr/bin/yq &&\
  chmod +x /usr/bin/yq

# Install AWS CLI.
WORKDIR /tmp
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip
RUN ./aws/install

# Set up the python virtual environment
ENV VIRTUAL_ENV="/opt/venv"
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

RUN python3 -m venv $VIRTUAL_ENV 

COPY requirements.txt /tmp/
RUN python -m pip install --upgrade pip pip-tools \
    && python -m pip install --no-cache-dir -r /tmp/requirements.txt \
        --no-binary rasterio \
        --no-binary shapely \
        --no-binary fiona    

RUN mkdir -p /code
WORKDIR /code
COPY . /code/

RUN pip install /code \
    && pip cache purge

CMD ["python", "--version"]

FROM base AS tests
RUN pip install --no-cache-dir -r /code/requirements-test.txt
