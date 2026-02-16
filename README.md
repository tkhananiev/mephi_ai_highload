# Go AI Highload Service (rolling average + z-score)

Сервис соответствует ТЗ: Go (goroutines + channels), Redis cache, Prometheus `/metrics`, простая аналитика нагрузки: rolling average (window=50) и детекция аномалий по z-score (threshold > 2σ).

## API

- `POST /api/metrics` — приём метрик
  ```json
  {"timestamp":"2026-02-17T12:34:56Z","cpu":45.2,"rps":1234}
  ```
  Ответ: `202 Accepted`.

- `GET /api/analyze` — текущая аналитика окна (mean/std/rolling/z/anomaly)
- `GET /health` — healthcheck
- `GET /metrics` — Prometheus metrics

## Local run

```bash
export REDIS_ADDR=localhost:6379
go mod download
go run .
```

## Docker

```bash
docker build -t go-ai-service:latest .
```

## Kubernetes (Minikube)

```bash
minikube start --cpus=4 --memory=8192

# Собрать образ и загрузить в docker окружение minikube
minikube image build -t go-ai-service:latest .

kubectl apply -f k8s/redis.yaml
kubectl apply -f k8s/app.yaml
kubectl apply -f k8s/hpa.yaml

kubectl get pods
kubectl port-forward svc/go-ai-service 8080:8080
```

Проверка:

```bash
curl -s http://localhost:8080/health
curl -s http://localhost:8080/api/analyze
```

## Load test (wrk)

Вариант 1 — быстрый генератор JSON через bash:

```bash
for i in $(seq 1 10); do
  curl -s -X POST http://localhost:8080/api/metrics \
    -H 'Content-Type: application/json' \
    -d '{"timestamp":"'"$(date -u +%Y-%m-%dT%H:%M:%SZ)"'","cpu":30,"rps":1000}' >/dev/null
done
```

Вариант 2 — wrk с lua скриптом:

Создай `post.lua`:

```lua
wrk.method = "POST"
wrk.headers["Content-Type"] = "application/json"
request = function()
  local ts = os.date("!%Y-%m-%dT%H:%M:%SZ")
  local cpu = 30
  local rps = 1000
  local body = string.format('{"timestamp":"%s","cpu":%d,"rps":%d}', ts, cpu, rps)
  return wrk.format(nil, "/api/metrics", nil, body)
end
```

Запуск:

```bash
wrk -t4 -c200 -d30s -s post.lua http://localhost:8080
```

## Notes

Redis используется best-effort для кеширования последнего результата (`analyze:latest`). Основная аналитика — in-memory (быстрее и стабильнее под нагрузкой).
