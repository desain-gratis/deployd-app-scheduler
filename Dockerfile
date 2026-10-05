FROM alpine:latest

# Allow hit https endpoint from inside 
RUN apk --no-cache add ca-certificates
RUN update-ca-certificates

# important for cleanly closing connection
STOPSIGNAL SIGINT

WORKDIR /app

# Copy the compiled binary from the builder stage
COPY ./dist/scheduler .

# Command to run the application
CMD ["./scheduler"]
