# Restate service for downloading files

![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/sagikazarmark/restate-downloader/ci.yaml?style=flat-square)
![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/sagikazarmark/restate-downloader/badge?style=flat-square)

**Restate service for downloading files.**

This service provides a durable, fault-tolerant wrapper around way for downloading files and storing them in an object store.
Built on [Restate](https://restate.dev), it ensures reliable downloads with automatic retries, state management, and seamless integration with object storage systems.

## Quickstart

1. **Start the service:**
   ```bash
   # Clone the repository and start dependencies
   docker compose up -d

   # Install dependencies and run the service
   cargo run
   ```

2. **Register the service with Restate:**
   ```bash
   # Service URL depends on how you run Restate and the service
   restate deployment register http://host.docker.internal:9080
   ```

3. **Download a file:**
   ```bash
   curl -X POST http://localhost:8080/Downloader/download \
     -H "Content-Type: application/json" \
     -d '{
       "url": "https://example.com/file",
       "output": {
         "url": "s3://my-bucket/downloads/"
       }
     }'
   ```

## Configuration

TODO

## Deployment

The recommended deployment method is using containers.

You can either build and run the container yourself or use the pre-built image from GHCR:

```
ghcr.io/sagikazarmark/restate-downloader
```

For production deployments, consider:
- Using persistent volumes for temporary storage
- Setting appropriate resource limits
- Configuring object storage credentials securely
- Setting up monitoring and logging

## License

The project is licensed under the [MIT License](LICENSE).
