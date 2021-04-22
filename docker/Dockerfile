FROM ubuntu:18.04

# This is required, otherwise pydedm fails on install
ENV LC_ALL=C.UTF-8

RUN echo 'deb http://us.archive.ubuntu.com/ubuntu trusty main multiverse' >> /etc/apt/sources.list && \
    apt-get -y check && \
    apt-get -y update && \
    apt-get -y install apt-utils && \
    apt-get -y upgrade

RUN apt-get update && \
    apt-get --no-install-recommends --allow-unauthenticated -y install \
    gfortran \
    git

RUN apt-get -y install software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get -y install \
    python3 \
    python3-dev \
    python3-pip \
    python3-setuptools

RUN pip3 install numpy==1.16.0 \
    astropy==4.1 \
    pika==1.0.0 \
    pybind11==2.6.2 \
    scipy==1.5.4 \
    git+https://github.com/telegraphic/pygedm \
    git+https://github.com/timstaley/fourpiskytools.git@master \
    git+https://bitbucket.org/jankowsk/meertrig.git@master

WORKDIR /software

#RUN git clone https://github.com/FRBs/pygedm.git && \
#    cd pygedm && \
#    python3 setup.py install

#RUN pip3 install git+https://bitbucket.org/jankowsk/meertrig.git@master

RUN git clone https://github.com/mmalenta/spring_time.git && \
    cd spring_time && \
    git checkout dev && \
    python3 setup.py install