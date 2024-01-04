# Start from a base Ubuntu image
FROM ubuntu:latest

RUN apt-get update && apt-get install -y \
    bash \
    tar \
    && rm -rf /var/lib/apt/lists/*
    
# Set the Current Working Directory inside the container
WORKDIR /app

# Copy the binary from the current directory to the Working Directory inside the container
COPY cpuhist .

# Command to run the executable
CMD ["/app/cpuhist"]
