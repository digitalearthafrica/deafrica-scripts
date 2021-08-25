FROM osgeo/gdal:ubuntu-small-3.3.1

RUN apt-get update && \
    apt-get install -y \
      build-essential \
      git \
      python3-pip \
    && apt-get autoclean && \
    apt-get autoremove

RUN python -m pip install --upgrade pip
RUN mkdir -p /code
COPY requirements.txt /code
RUN pip install -r /code/requirements.txt
WORKDIR /code

CMD ["python --version"]