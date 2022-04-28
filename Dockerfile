FROM python:alpine
MAINTAINER Melih Egemen Yavuz

COPY ./requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY ./service /service
WORKDIR /service

EXPOSE 5000/tcp
ENTRYPOINT ["python"]
CMD ["service.py"]
