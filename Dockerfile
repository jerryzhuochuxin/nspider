FROM python:3.7
RUN mkdir /nspider
COPY . /nspider
WORKDIR /nspider
CMD ["/bin/bash","RunInLinux.sh"]