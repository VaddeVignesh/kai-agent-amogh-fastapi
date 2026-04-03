# Podman App Containerization

This repo now includes a Podman-ready application layer that connects to the already-running database/cache containers:

- Postgres container: `postgresdb`
- Mongo container: `mongodb`
- Redis container: `redisdb`
- Shared Podman network: `kai-agent_kai-net`

The database containers already exist and should remain untouched. The new app layer adds:

- `docker/Containerfile.api`
- `docker/Containerfile.ui`
- `docker/podman-compose.app.yaml`
- `docker/app.podman.env.example`

## 1. Prepare env file

Copy the example file and fill in the real API key if needed:

```powershell
Copy-Item "docker/app.podman.env.example" "docker/app.podman.env"
```

Important defaults already match the discovered Podman service names:

- `POSTGRES_HOST=postgresdb`
- `MONGO_URI=mongodb://mongodb:27017/`
- `REDIS_HOST=redisdb`

## 2. Build the app images

```powershell
podman build -t kai-agent-api -f docker/Containerfile.api .
podman build -t kai-agent-ui -f docker/Containerfile.ui .
```

## 3. Run with Podman Compose

```powershell
podman-compose -f docker/podman-compose.app.yaml --env-file docker/app.podman.env up -d --build
```

If `podman-compose` or `podman compose` is not installed on the machine, use the manual fallback below.

This starts:

- API container: `kai-agent-api`
- UI container: `kai-agent-ui`

Default host ports:

- API: `8001 -> 8000`
- UI: `8501 -> 8501`

## 4. Manual Podman fallback

Run the API:

```powershell
$env:GROQ_API_KEY = "<your-groq-key>"
podman run --replace -d --name kai-agent-api --network kai-agent_kai-net -p 8001:8000 `
  --env GROQ_API_KEY=$env:GROQ_API_KEY `
  --env POSTGRES_DSN=postgresql://admin:admin123@postgresdb:5432/postgres `
  --env POSTGRES_HOST=postgresdb `
  --env POSTGRES_PORT=5432 `
  --env POSTGRES_USER=admin `
  --env POSTGRES_PASSWORD=admin123 `
  --env POSTGRES_DB=postgres `
  --env MONGO_URI=mongodb://mongodb:27017/ `
  --env MONGO_DB_NAME=kai_agent `
  --env REDIS_HOST=redisdb `
  --env REDIS_PORT=6379 `
  --env REDIS_DB=0 `
  localhost/kai-agent-api:latest
```

Run the UI:

```powershell
podman run --replace -d --name kai-agent-ui --network kai-agent_kai-net -p 8501:8501 `
  --env KAI_API_URL=http://kai-agent-api:8000/query `
  --env KAI_API_BASE_URL=http://kai-agent-api:8000 `
  --env KAI_API_TIMEOUT_SEC=300 `
  localhost/kai-agent-ui:latest
```

## 5. Verify

```powershell
podman ps
podman logs kai-agent-api
podman logs kai-agent-ui
```

Check:

- API health: [http://127.0.0.1:8001/](http://127.0.0.1:8001/)
- UI: [http://127.0.0.1:8501/](http://127.0.0.1:8501/)

## 6. Stop the app layer

```powershell
podman-compose -f docker/podman-compose.app.yaml --env-file docker/app.podman.env down
```

This stops only the app containers, not the existing Postgres/Mongo/Redis containers.

Manual fallback stop:

```powershell
podman rm -f kai-agent-ui kai-agent-api
```

## Notes

- The compose file uses the existing external network `kai-agent_kai-net`.
- The API defaults to the repo's current Postgres database target: `postgres`.
- If you want to point at `pocdb` instead, update `POSTGRES_DSN` and `POSTGRES_DB` in `docker/app.podman.env`.
- `.containerignore` prevents local `.env`, `venv`, tests, and other dev-only files from being copied into the images.
