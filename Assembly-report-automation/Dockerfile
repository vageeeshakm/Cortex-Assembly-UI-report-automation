# VERSION 1.10.9
# SOURCE: https://github.com/puckel/docker-airflow

FROM python:3.7-slim-buster
LABEL maintainer="Maintainer"

# Never prompt the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux
ENV HOME /root
ENV APP_HOME /application/
ENV C_FORCE_ROOT=true
ENV PYTHONUNBUFFERED 1

# Airflow
ARG AIRFLOW_VERSION=1.10.9
ARG AIRFLOW_USER_HOME=/root/airflow
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8

# Disable noisy "Handling signal" log messages:
# ENV GUNICORN_CMD_ARGS --log-level WARNING

# install requirements for selenium

# set display port to avoid crash
ENV DISPLAY=:99

# ARG CHROME_BROWSER_VERSION=81.0.4044.138-1
# ARG CHROME_DRIVER_VERSION=81.0.4044.138

RUN apt-get -y update
RUN apt-get install -y --no-install-recommends \
        wget \
        gnupg2

RUN apt-get install -yqq unzip

# install google chrome
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
RUN sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list'
RUN apt-get -y update
RUN apt-get install -y \
    google-chrome-stable \
    curl

# install chromedriver
RUN wget -O /tmp/chromedriver.zip http://chromedriver.storage.googleapis.com/`curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE`/chromedriver_linux64.zip
# RUN mkdir browser_drivers

RUN unzip /tmp/chromedriver.zip -d /browser_drivers/

# install firefox browser
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys A6DCF7707EBC211F
RUN apt-get -y update
RUN apt install -y software-properties-common
RUN apt-add-repository "deb http://ppa.launchpad.net/ubuntu-mozilla-security/ppa/ubuntu bionic main"
RUN apt-get -y update
RUN apt-get install -y firefox

# install firefox driver
RUN wget -O /tmp/geckodriver-v0.26.0-linux64.tar.gz https://github.com/mozilla/geckodriver/releases/download/v0.26.0/geckodriver-v0.26.0-linux64.tar.gz
RUN tar -zxvf /tmp/geckodriver-v0.26.0-linux64.tar.gz  -C /browser_drivers/

RUN mkdir downloads

# selenium requirements done

RUN set -ex \
    && buildDeps=' \
        freetds-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        libpq-dev \
        git \
    ' \
    && apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
        $buildDeps \
        freetds-bin \
        build-essential \
        default-libmysqlclient-dev \
        apt-utils \
        curl \
        rsync \
        netcat \
        locales \
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && pip install -U pip setuptools wheel \
    && apt-get purge --auto-remove -yqq $buildDeps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

RUN mkdir -p $APP_HOME
WORKDIR $APP_HOME

# Install pip packages
RUN mkdir $APP_HOME/requirements
ADD ./requirements/* $APP_HOME/requirements/
RUN pip install -r $APP_HOME/requirements/local.txt
RUN rm -rf requirements

EXPOSE 8080 5555 8793

COPY script/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
COPY config/airflow.cfg ${AIRFLOW_USER_HOME}/airflow.cfg

ADD . $APP_HOME

WORKDIR ${AIRFLOW_USER_HOME}
ENTRYPOINT ["/entrypoint.sh"]
