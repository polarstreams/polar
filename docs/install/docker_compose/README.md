# Run PolarStreams with Docker Compose

For local application development and CI environments, you can use [docker compose][docker-compose] to run PolarStreams.

You can run a single-broker cluster in developer mode using the following service definition:

```
version: "3.9"
services:
  polar:
    image: "polarstreams/polar:latest"
    environment:
      - POLAR_DEV_MODE=true
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9250/status"]
      interval: 1s
      retries: 60
      start_period: 2s
      timeout: 1s
```

[docker-compose]: https://github.com/docker/compose
