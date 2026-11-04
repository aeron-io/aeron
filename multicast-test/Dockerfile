FROM ubuntu:26.04 AS builder

RUN apt-get update -y && apt-get install -y build-essential libbsd-dev uuid-dev curl iproute2

WORKDIR /opt/aeron
ADD . /opt/aeron

RUN ./cppbuild/cppbuild --no-tests --no-system-tests --no-unit-tests --skip-archive-api

FROM builder AS media-driver

ENTRYPOINT ./cppbuild/Release/binaries/aeronmd -Daeron.event.log=all

FROM builder AS basic-publisher

ARG CHANNEL
ENV CHANNEL=${CHANNEL}
ENTRYPOINT ./cppbuild/Release/binaries/basic_publisher -c "${CHANNEL}"

FROM builder AS basic-subscriber

ARG CHANNEL
ENV CHANNEL=${CHANNEL}
ENTRYPOINT ./cppbuild/Release/binaries/basic_subscriber -c "${CHANNEL}"

