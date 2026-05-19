# rafkav2

Run the telemetry stack: `docker-compose -f deployment/dev/docker-compose.otlp.yml up -d`

Run the gateway: `cargo run -p rafka-gateway`

Open Jaeger: http://localhost:16687

Note: v2 uses port 4327 (OTLP gRPC) and 16687 (Jaeger UI) to avoid conflict with any running v1 rafka collector on 4317/16686.
