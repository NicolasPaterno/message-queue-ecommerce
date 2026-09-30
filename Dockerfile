FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -o /app ./cmd/shop

FROM gcr.io/distroless/static-debian12
COPY --from=build /app /app
CMD ["/app","api"]
