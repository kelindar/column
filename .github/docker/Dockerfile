# docker build -t column_ubuntu_test . 
FROM golang:1.18-bullseye

WORKDIR /usr/

COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .
RUN go test -race -covermode atomic -coverprofile=profile.cov .
