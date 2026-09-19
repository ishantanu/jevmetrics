FROM golang:1.23-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/jevmetrics ./cmd/jevmetrics

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/jevmetrics /jevmetrics
COPY config.example.json /config.json
ENV JEVMETRICS_CONFIG=/config.json
EXPOSE 8080
ENTRYPOINT ["/jevmetrics"]
