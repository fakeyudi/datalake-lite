# Multi-stage Docker build for datalake-lite
FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY . .

# Build CLI and listener binaries
RUN go mod tidy &&     go build -o /app/bin/dl ./cmd/dl &&     go build -o /app/bin/listener ./cmd/listener

# Final image
FROM alpine:latest

WORKDIR /app
COPY --from=builder /app/bin /app/bin

# Set default to help message
ENTRYPOINT ["/app/bin/dl"]
