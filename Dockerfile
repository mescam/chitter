FROM ubuntu
MAINTAINER Jakub Woźniak <jakub@wozniak.im>
RUN apt-get update && apt-get install -y python3-pip libpython3-dev libssl-dev 
RUN mkdir /app
COPY . /app/
WORKDIR /app/
RUN pip3 install uwsgi
RUN pip3 install -r requirements.txt
EXPOSE 8080
EXPOSE 5000
CMD uwsgi --socket 0.0.0.0:8080 --protocol http -w api:app
