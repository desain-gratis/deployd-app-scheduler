FROM ubuntu:24.04

# Allow hit https endpoint from inside 
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    bash \
    && rm -rf /var/lib/apt/lists/*

# Install chDB library
RUN curl -sL https://lib.chdb.io | bash

# important for cleanly closing connection
STOPSIGNAL SIGINT

WORKDIR /app

# Install chDB library: alternative from the curl above
# COPY ./dist/libchdb.so .

# Copy the compiled binary from the builder stage
COPY ./dist/scheduler .

# Command to run the application
CMD ["./scheduler"]
