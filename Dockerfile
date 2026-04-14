FROM scratch
WORKDIR /app

COPY ./builds/server .
EXPOSE 4001

CMD ["/app/server"]
