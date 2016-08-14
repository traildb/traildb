  
FROM ubuntu:16.04

MAINTAINER Chia-Chi Chang <c3h3.tw@gmail.com>


ENV DEBIAN_FRONTEND noninteractive


RUN apt-get update && apt-get install -y libarchive-dev libjudy-dev pkg-config git-core build-essential gfortran sudo make cmake libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm vim python

RUN cd /tmp && git clone https://github.com/traildb/traildb.git && cd traildb && ./waf configure && ./waf install && cd /tmp && rm -rf traildb/

ENV LD_LIBRARY_PATH /usr/local/lib



