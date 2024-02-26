# Copied from https://github.com/emk/rust-musl-builder/blob/master/examples/using-diesel/Dockerfile
FROM ekidd/rust-musl-builder:nightly AS builder

# Add the source code.
COPY --chown=rust:rust . ./

# Delete and re-install rustup in order to get the latest verison of Rust nightly.
# This is necessary due to a bug in Rust: https://github.com/rust-lang-nursery/rustup.rs/issues/1239
RUN rm -rf ~/.rustup
RUN curl https://sh.rustup.rs -sSf | \
    sh -s -- -y && \
    rustup target add x86_64-unknown-linux-musl

# Build the `tdb-server` application.
RUN cargo build --bin tdb-server --release

# Build the `tdb` application.
RUN cargo build --bin tdb --release

# Now, we need to build the _real_ Docker container, copying in `tdb-server`
FROM alpine:latest
RUN apk --no-cache add ca-certificates && update-ca-certificates
ENV IMAGE_NAME=tectonicdb
COPY --from=builder \
    /home/rust/src/target/x86_64-unknown-linux-musl/release/tdb-server \
    /usr/local/bin/

COPY --from=builder \
    /home/rust/src/target/x86_64-unknown-linux-musl/release/tdb \
    /usr/local/bin/

# Initialize the application
CMD /usr/local/bin/tdb-server -vv
