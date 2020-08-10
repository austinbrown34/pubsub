FROM amd64/ubuntu:18.04
SHELL ["/bin/bash", "-c"]

RUN apt-get update && apt-get install -y \
    git \
    apt-utils \
    wget \
    curl \
    build-essential \
    vim
