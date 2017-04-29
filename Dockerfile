FROM python:3.6

VOLUME /usr/src/sorna-gateway
WORKDIR /usr/src/sorna-gateway

# During build, we need to copy the initial version.
# Afterwards, we mount the host working copy as a volume here.
COPY . /usr/src/sorna-gateway

RUN pip install -U pip wheel setuptools
RUN pip install -r requirements-dev.txt

CMD python -m sorna.gateway
