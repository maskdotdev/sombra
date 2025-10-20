# Kubernetes Deployment Guide for Sombra

This directory contains Kubernetes manifests for deploying Sombra-based applications.

## Prerequisites

- Kubernetes cluster (1.19+)
- kubectl configured
- Persistent volume provisioner (for database storage)
- Optional: Prometheus Operator (for metrics)
- Optional: cert-manager (for TLS)

## Quick Start

### 1. Build Your Application Image

```bash
# Build the Sombra tools image
docker build -t sombra:0.2.0 -f Dockerfile .

# Or build your application that uses Sombra
docker build -t myapp:latest .

# Push to registry
docker tag myapp:latest myregistry/myapp:latest
docker push myregistry/myapp:latest
```

### 2. Deploy to Kubernetes

```bash
# Create namespace and deploy
kubectl apply -f k8s/deployment.yaml

# Check status
kubectl -n sombra get pods
kubectl -n sombra get pvc
kubectl -n sombra get svc

# View logs
kubectl -n sombra logs -f statefulset/sombra-app
```

### 3. Access Your Application

```bash
# Port forward for local testing
kubectl -n sombra port-forward svc/sombra 8080:80

# Or configure Ingress (edit deployment.yaml first)
# Then access via: https://sombra.example.com
```

## Configuration

### Storage

Edit the PVC in `deployment.yaml`:

```yaml
spec:
  resources:
    requests:
      storage: 100Gi  # Adjust based on your needs
  storageClassName: fast-ssd  # Use your provider's SSD class
```

**Cloud Provider Storage Classes:**
- AWS: `gp3` (general purpose SSD)
- GCP: `pd-ssd` (persistent disk SSD)
- Azure: `managed-premium` (premium SSD)

### Resources

Adjust CPU and memory based on your workload:

```yaml
resources:
  requests:
    cpu: 500m      # 0.5 cores minimum
    memory: 1Gi    # 1GB minimum
  limits:
    cpu: 2000m     # 2 cores maximum
    memory: 4Gi    # 4GB maximum
```

**Recommended:**
- Small workload: 500m CPU, 1Gi RAM
- Medium workload: 1000m CPU, 2Gi RAM
- Large workload: 2000m+ CPU, 4Gi+ RAM

### Environment Variables

Edit the ConfigMap in `deployment.yaml`:

```yaml
data:
  LOG_LEVEL: "info"
  RUST_LOG: "sombra=info"
  # Add your app-specific variables
  DATABASE_PATH: "/data/sombra/graph.db"
  CACHE_SIZE: "5000"
```

## Monitoring

### Prometheus Integration

If using Prometheus Operator, the `ServiceMonitor` resource will automatically configure scraping:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: sombra
  namespace: sombra
spec:
  endpoints:
  - port: metrics
    interval: 30s
```

### Grafana Dashboard

Import the Sombra dashboard (create `grafana-dashboard.json` with your metrics):

```json
{
  "dashboard": {
    "title": "Sombra Metrics",
    "panels": [
      {
        "title": "Transaction Rate",
        "targets": [{
          "expr": "rate(sombra_transactions_committed_total[5m])"
        }]
      },
      {
        "title": "Cache Hit Rate",
        "targets": [{
          "expr": "sombra_cache_hits / (sombra_cache_hits + sombra_cache_misses) * 100"
        }]
      }
    ]
  }
}
```

## Backup Strategy

### Automated Backups with CronJob

Create a backup CronJob:

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: sombra-backup
  namespace: sombra
spec:
  schedule: "0 2 * * *"  # Daily at 2 AM
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: backup
            image: sombra:0.2.0
            command:
            - /bin/bash
            - -c
            - |
              # Checkpoint database
              sombra-repair checkpoint /data/sombra/graph.db
              
              # Copy to backup location
              DATE=$(date +%Y%m%d)
              cp /data/sombra/graph.db /backups/graph-$DATE.db
              gzip /backups/graph-$DATE.db
              
              # Upload to S3 (requires AWS credentials)
              # aws s3 cp /backups/graph-$DATE.db.gz s3://my-backups/
            volumeMounts:
            - name: data
              mountPath: /data/sombra
            - name: backups
              mountPath: /backups
          restartPolicy: OnFailure
          volumes:
          - name: data
            persistentVolumeClaim:
              claimName: sombra-data
          - name: backups
            persistentVolumeClaim:
              claimName: sombra-backups
```

## High Availability

**Note:** Sombra v0.2.0 supports single-writer mode only. For HA:

1. **Active-Passive**: Use `replicas: 1` with automatic failover
2. **Read Replicas**: Wait for v0.3.0 (MVCC support)
3. **Application Sharding**: Partition data across multiple instances

### Active-Passive Setup

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: sombra-app
spec:
  replicas: 1  # Only one writer
  # ... rest of config

---

# Standby instance (separate PVC)
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: sombra-standby
spec:
  replicas: 1
  # Configure to restore from backups
  # Promote to primary on failure
```

## Scaling Considerations

### Vertical Scaling

Increase resources for a single instance:

```bash
# Edit StatefulSet
kubectl -n sombra edit statefulset sombra-app

# Update resources in spec.template.spec.containers[0].resources
# Pods will be recreated with new limits
```

### Horizontal Scaling (Application-Level Sharding)

Deploy multiple isolated Sombra instances:

```yaml
# sombra-shard-0.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: sombra-shard-0
  namespace: sombra
spec:
  # ... config for shard 0

---

# sombra-shard-1.yaml (similar)
```

Application routes requests to appropriate shard based on entity ID.

## Troubleshooting

### Pod Not Starting

```bash
# Check events
kubectl -n sombra describe pod sombra-app-0

# Check logs
kubectl -n sombra logs sombra-app-0

# Common issues:
# - PVC not bound (check provisioner)
# - Image pull error (check registry credentials)
# - Health check failing (check application startup)
```

### Database Corruption

```bash
# Exec into pod
kubectl -n sombra exec -it sombra-app-0 -- /bin/bash

# Verify database
sombra-inspect verify /data/sombra/graph.db

# Repair if possible
sombra-repair checkpoint /data/sombra/graph.db

# Or restore from backup
# (see backup/restore procedures)
```

### Performance Issues

```bash
# Check resource usage
kubectl -n sombra top pod sombra-app-0

# Check metrics
kubectl -n sombra port-forward svc/sombra 9090:9090
# Visit http://localhost:9090/metrics

# Check disk I/O
kubectl -n sombra exec -it sombra-app-0 -- iostat -x 1
```

### Storage Full

```bash
# Check PVC usage
kubectl -n sombra exec -it sombra-app-0 -- df -h /data/sombra

# Expand PVC (if storage class supports it)
kubectl -n sombra edit pvc sombra-data
# Increase spec.resources.requests.storage

# Or checkpoint and vacuum
kubectl -n sombra exec -it sombra-app-0 -- sombra-repair vacuum /data/sombra/graph.db
```

## Security Best Practices

1. **Run as non-root**: Already configured in manifests
2. **Read-only root filesystem**: Enabled in security context
3. **Network policies**: Restrict traffic to/from pods
4. **Secrets management**: Use Kubernetes Secrets or external vault
5. **RBAC**: Limit service account permissions
6. **Pod Security Policies**: Enforce security standards

Example NetworkPolicy:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: sombra-netpol
  namespace: sombra
spec:
  podSelector:
    matchLabels:
      app: sombra
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          name: ingress-nginx
    ports:
    - protocol: TCP
      port: 8080
  egress:
  - to:
    - namespaceSelector: {}
    ports:
    - protocol: TCP
      port: 53  # DNS
```

## Production Checklist

Before going to production:

- [ ] Resource limits configured appropriately
- [ ] PVC sized correctly (with growth buffer)
- [ ] Liveness/readiness probes tested
- [ ] Monitoring and alerting configured
- [ ] Backup CronJob scheduled and tested
- [ ] Backup restoration tested
- [ ] Network policies applied
- [ ] Security context configured (non-root, read-only FS)
- [ ] Secrets properly managed (not in ConfigMap)
- [ ] Ingress TLS configured (cert-manager)
- [ ] Pod disruption budget set
- [ ] Node affinity/anti-affinity configured
- [ ] Disaster recovery plan documented
- [ ] Runbooks created for common issues

## Additional Resources

- [Sombra Documentation](../docs/)
- [Production Deployment Guide](../docs/production.md)
- [Performance Tuning](../docs/performance.md)
- [Troubleshooting Guide](../docs/operations.md)
- [GitHub Issues](https://github.com/maskdotdev/sombra/issues)

---

For support, please file an issue at: https://github.com/maskdotdev/sombra/issues
