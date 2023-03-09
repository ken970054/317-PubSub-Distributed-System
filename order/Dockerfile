FROM golang:1.19.4

RUN mkdir /app

WORKDIR /app

ADD . /app

RUN go build -o main $(ls -1 *.go)

CMD /app/main

