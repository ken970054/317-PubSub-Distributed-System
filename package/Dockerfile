FROM golang:1.19.4

RUN mkdir /app

WORKDIR /app

ADD . /app

#RUN go build -o main $(ls -1 *.go)
ENV GO111MODULE=off
RUN go get github.com/eclipse/paho.mqtt.golang
RUN go build -o main package_main.go

CMD /app/main

