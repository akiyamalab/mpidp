FROM ubuntu:18.04

MAINTAINER Akiyama Laboratory, Tokyo Institute of Technology <megadock@bi.cs.titech.ac.jp>

RUN apt-get update -y
RUN apt-get install -y --no-install-recommends \
                wget \
                make \
                g++ \
                openmpi-bin \
                libopenmpi-dev \
                ssh \
    && rm -rf /var/lib/apt/lists/*

ENV WORK_DIR /opt/mpidp

ADD . ${WORK_DIR}

WORKDIR ${WORK_DIR}

RUN cd ${WORK_DIR}/test && make -j8

RUN make -j8
