# syntax=docker/dockerfile:1

FROM golang:1.22-alpine AS build
WORKDIR /src
COPY . ./
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/app ./

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata && adduser -D -H -u 10001 appuser
USER 10001
WORKDIR /app
COPY --from=build /out/app /app/app
EXPOSE 8080
ENV PORT=8080
ENTRYPOINT ["/app/app"]
