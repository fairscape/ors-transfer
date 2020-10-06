FROM ubuntu:latest
RUN apt-get update -y
RUN apt-get upgrade -y
RUN apt-get update -y
RUN apt-get install curl -y
RUN apt-get install -y python3 python3-pip build-essential
COPY requirements.txt .

RUN export PATH=/sbin:/bin:/usr/bin:/usr/sbin:/usr/local/sbin:/usr/local/bin
RUN apt-get install vim -y

RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt
#RUN apt-get install -y uwsgi-plugin-python3
#RUN apt-get install -y uwsgi-plugin-python
#COPY http.ini .
RUN mkdir star
COPY ./app .
COPY . .
RUN cd /app

EXPOSE 5000
#ENTRYPOINT [ "uwsgi", "--ini", "http.ini"]
ENTRYPOINT [ "python3","./uploader.py"]
