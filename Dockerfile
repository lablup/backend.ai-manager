FROM        ubuntu:14.04

# NOTICE:
# You need to build sorna-protocols wheel package, and copy the resulting
# ".whl" file into the project base directory.

ENV         DEBIAN_FRONTEND noninteractive
RUN         sed 's@archive.ubuntu.com@ftp.jaist.ac.jp@' -i /etc/apt/sources.list

RUN         apt-get update && apt-get upgrade -y
RUN         apt-get install -y python3-dev python3-pip
RUN         apt-get install -y sqlite3 libsqlite3-dev

RUN         mkdir /sorna
ADD         . /sorna

RUN         pip3 install --upgrade --ignore-installed pip
RUN         pip3 install -r /sorna/requirements.txt
RUN         find /sorna -name '*.whl' | xargs pip3 install
RUN         cd /sorna; python3 setup.py install

EXPOSE      5001
ENTRYPOINT  ["sorna_manager", "--kernel-driver", "local"]
