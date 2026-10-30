# deploy

Two instances of the example chat server behind nginx round-robin, sharing one Redis:

```sh
docker compose -f examples/deploy/docker-compose.yml up -d --build   # http://localhost:8080
make conformance-redis                                      # the conformance tester against it
docker compose -f examples/deploy/docker-compose.yml logs app-1 app-2 | grep last_event_id
```

There are no sticky sessions: a client that reconnects lands on either instance and resumes from
its `Last-Event-ID`. The `X-Upstream` response header and the `instance` field of each log line
show which instance served a request.
