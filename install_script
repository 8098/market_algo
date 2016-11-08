#! /bin/sh
sudo apt-get update \
&& sudo apt-get upgrade -y \
&& sudo apt-get install postgresql-server-dev-all python3-dev -y \
&& sudo pip3 install --upgrade pip \
&& mkdir downloads \
&& cd downloads \
&& wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz \
&& tar -zxvf ta-lib-0.4.0-src.tar.gz \
&& cd ta-lib \
&& ./configure --prefix=/usr \
&& make \
&& sudo make install \
&& cd .. \
&& cd .. \
&& sudo pip3 install -r requirements.txt \
&& rm -rf downloads \
&& sudo mv /usr/bin/python /usr/bin/python2 \
&& sudo ln -s /usr/bin/python3 /usr/bin/python