FROM ubuntu:16.04

RUN apt-get update

# Add required dependencies
RUN apt-get install curl file build-essential pkg-config libssl-dev -y

# Install the Rust nightly toolchain
# Taken from https://hub.docker.com/r/mackeyja92/rustup/~/dockerfile/
RUN curl https://sh.rustup.rs -s > /home/install.sh && \
    chmod +x /home/install.sh && \
    sh /home/install.sh -y --verbose --default-toolchain nightly

ENV PATH "/root/.cargo/bin:$PATH"

# Move source code and scripts from local filesystem into the image
COPY src /app/src
COPY start-server.sh /app/start-server.sh
COPY conf /app/conf
COPY Cargo.toml /app/Cargo.toml
COPY Cargo.lock /app/Cargo.lock

WORKDIR /app

# Compile the application
RUN cargo build --release

# Initialize the application
CMD ["./start-server.sh"]
