FROM ubuntu:latest
MAINTAINER Shekhar Gulati "shekhargulati84@gmail.com"
RUN apt-get update -y
RUN apt-get install -y python-pip python-dev build-essential
RUN apt-get install -y gfortran
RUN apt-get install -y libblas3gf libblas-doc libblas-dev
RUN apt-get install -y liblapack3gf liblapack-doc liblapack-dev

COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
ENTRYPOINT ["python"]
CMD ["app.py"]
