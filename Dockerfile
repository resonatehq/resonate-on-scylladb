FROM golang:1.25 AS deps
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

FROM deps AS build
COPY . .
RUN go build -o server ./cmd/resonate

FROM deps AS tester
COPY . .
ENTRYPOINT ["go", "test"]

FROM debian:bookworm-slim AS server
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/server /server
EXPOSE 8001
ENTRYPOINT ["/server"]
CMD ["serve"]
