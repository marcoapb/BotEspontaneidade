FROM httpd:2.4
COPY ./public-html/ /usr/local/apache2/htdocs/
COPY ./httpd.conf /usr/local/apache2/conf/httpd.conf
COPY ./apache-selfsigned.crt /usr/local/apache2/conf/apache-selfsigned.crt
COPY ./apache-selfsigned.key /usr/local/apache2/conf/apache-selfsigned.key
COPY ./httpd-ssl.conf /usr/local/apache2/conf/extra/httpd-ssl.conf
COPY ./AC_RFB.crt /usr/local/apache2/conf/ssl.crt/AC_RFB.crt
RUN sed -i \
        -e 's/^#\(Include .*httpd-ssl.conf\)/\1/' \
        -e 's/^#\(LoadModule .*mod_ssl.so\)/\1/' \
        -e 's/^#\(LoadModule .*mod_socache_shmcb.so\)/\1/' \
        conf/httpd.conf