
FROM scratch
WORKDIR /app

COPY ./target/x86_64-unknown-linux-musl/release/syncplaympv-server .
EXPOSE 3000
EXPOSE 443

CMD ["/app/syncplaympv-server"]
