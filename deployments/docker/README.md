# Docker Deployment

## Build

```bash
docker build -t undr9:local .
```

The repository also publishes container images to GitHub Container Registry through
[docker.yml](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/.github/workflows/docker.yml).
The published image name is `ghcr.io/undr9/undr9`.
The GitHub Actions workflow publishes a multi-arch manifest for `linux/amd64` and
`linux/arm64`, so Docker Desktop users on macOS and Windows and Linux servers on
either architecture can pull the same tag.

## Run

```bash
docker run --rm \
  -p 8080:8080 \
  -v undr9_data:/var/lib/undr9/data \
  -e UNDR9_ADMIN_API_KEY=replace-with-admin-key \
  -e UNDR9_WRITER_API_KEY=replace-with-writer-key \
  -e UNDR9_READER_API_KEY=replace-with-reader-key \
  undr9:local
```

## Compose

```bash
export UNDR9_ADMIN_API_KEY=replace-with-admin-key
export UNDR9_WRITER_API_KEY=replace-with-writer-key
export UNDR9_READER_API_KEY=replace-with-reader-key
docker compose up --build
```

## GHCR Pull

```bash
docker pull ghcr.io/undr9/undr9:latest
```

## GHCR Run

```bash
docker run --rm \
  -p 8080:8080 \
  -v undr9_data:/var/lib/undr9/data \
  -e UNDR9_ADMIN_API_KEY=replace-with-admin-key \
  -e UNDR9_WRITER_API_KEY=replace-with-writer-key \
  -e UNDR9_READER_API_KEY=replace-with-reader-key \
  ghcr.io/undr9/undr9:latest
```

## Production Compose

```yaml
services:
  undr9:
    image: ghcr.io/undr9/undr9:latest
    restart: unless-stopped
    ports:
      - "8080:8080"
    environment:
      UNDR9_ADMIN_API_KEY: "${UNDR9_ADMIN_API_KEY:?set UNDR9_ADMIN_API_KEY}"
      UNDR9_WRITER_API_KEY: "${UNDR9_WRITER_API_KEY:?set UNDR9_WRITER_API_KEY}"
      UNDR9_READER_API_KEY: "${UNDR9_READER_API_KEY:?set UNDR9_READER_API_KEY}"
    volumes:
      - undr9_data:/var/lib/undr9/data

volumes:
  undr9_data:
```

## Runtime Notes

- The image includes a `/readyz` healthcheck and uses `tini` as PID 1 for cleaner signal handling.
- `docker stop` sends `SIGTERM`; UNDR9 drains readiness and flushes storage state before exit.
- Keep TLS termination in a reverse proxy such as Caddy or Traefik.
- Treat the API keys as required runtime secrets; do not bake them into the image.
- Set `UNDR9_MAINTENANCE_MAX_NODES` and `UNDR9_MAINTENANCE_MAX_EDGES` to match the maintenance window you are willing to allow through the admin API.
- Use `undr9 backup-storage` and `undr9 restore-storage` for backups and restores, including `--target-lsn` when validating PITR drills.
- Run `./scripts/run-recovery-drill.sh` against a mounted data volume to capture restore timing evidence and PITR verification before production cutover.
