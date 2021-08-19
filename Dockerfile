FROM osgeo/gdal:ubuntu-small-3.3.1

RUN apt-get update
RUN apt-get install -y build-essential git libpq-dev python-dev python3-pip wget
RUN apt-get autoclean
RUN apt-get autoremove
RUN rm -rf /var/lib/{apt,dpkg,cache,log}
RUN mkdir -p /conf
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /

ENTRYPOINT ["/bin/tini", "--"]

#CMD [""]