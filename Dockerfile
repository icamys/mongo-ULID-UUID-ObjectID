FROM golang:1.19.5-alpine as builder
WORKDIR /app
COPY perftest.go ./perftest.go
COPY go.mod go.sum ./
RUN go mod download
RUN CGO_ENABLED=0 go build -gcflags "all=-N -l" -a -installsuffix cgo -o /bin/perftest ./

FROM alpine:3.17.3
WORKDIR /opt
COPY --from=builder /bin/perftest /opt
CMD /opt/perftest
