FROM httpd:2.4
#COPY ./public-html/ /usr/local/apache2/htdocs/
COPY ./httpd.conf /usr/local/apache2/conf/httpd.conf
COPY ./cert/apache-selfsigned.crt /usr/local/apache2/conf/apache-selfsigned.crt
COPY ./cert/apache-selfsigned.key /usr/local/apache2/conf/apache-selfsigned.key
COPY ./httpd-ssl.conf /usr/local/apache2/conf/extra/httpd-ssl.conf
COPY ./cert/AC_RFB.crt /usr/local/apache2/conf/ssl.crt/AC_RFB.crt
RUN apt-get update
RUN apt-get -y install python3.7
RUN apt-get -y install libapache2-mod-wsgi-py3
RUN apt-get -y install -y python3-pip

RUN pip3 install flask
#RUN pip3 install flask
RUN sed -i \
        -e 's/^#\(Include .*httpd-ssl.conf\)/\1/' \
        -e 's/^#\(LoadModule .*mod_ssl.so\)/\1/' \
        -e 's/^#\(LoadModule .*mod_socache_shmcb.so\)/\1/' \
        conf/httpd.conf