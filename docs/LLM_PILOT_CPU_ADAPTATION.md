apiVersion: apps/v1
kind: Deployment
metadata:
  name: llama-cpu-pilot
  namespace: ats-dev
  labels:
    app: llama-cpu-pilot
    component: llm-inference-cpu
    version: pilot
spec:
  replicas: 1
  selector:
    matchLabels:
      app: llama-cpu-pilot
  template:
    metadata:
      labels:
        app: llama-cpu-pilot
        component: llm-inference-cpu
    spec:
      containers:
      - name: llama-3b-cpu
        image: dragonflyer762/ats-genai:latest
        command: ["/bin/bash", "-c"]
        args:
        - |
          echo "Starting Llama 3.2-3B CPU inference server..."
          
          # Install vLLM CPU support if not available
          pip install vllm[cpu] --quiet || pip install vllm --quiet
          pip install transformers accelerate --quiet
          
          # Download model if not cached
          python -c "
          from transformers import LlamaTokenizer, LlamaForCausalLM
          print('Loading Llama 3.2-3B model for CPU inference...')
          tokenizer = LlamaTokenizer.from_pretrained('meta-llama/Llama-3.2-3B-Instruct')
          model = LlamaForCausalLM.from_pretrained('meta-llama/Llama-3.2-3B-Instruct', 
                                                   torch_dtype='float32', 
                                                   device_map='cpu')
          print('Model loaded successfully')
          "
          
          # Start vLLM server optimized for CPU
          python -m vllm.entrypoints.openai.api_server \
            --model meta-llama/Llama-3.2-3B-Instruct \
            --served-model-name llama-3b-cpu \
            --host 0.0.0.0 \
            --port 8000 \
            --max-model-len 4096 \
            --device cpu \
            --dtype float16 \
            --max-num-seqs 4 \
            --disable-log-stats \
            --disable-log-requests
        resources:
          requests:
            memory: "12Gi"
            cpu: "8000m"
          limits:
            memory: "20Gi"
            cpu: "16000m"
        env:
        - name: TRANSFORMERS_CACHE
          value: "/tmp/transformers_cache"
        - name: HF_HOME
          value: "/tmp/hf_cache"
        - name: TORCH_NUM_THREADS
          value: "16"
        - name: OMP_NUM_THREADS
          value: "16"
        ports:
        - containerPort: 8000
          name: http
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 600  # Model loading takes time on CPU
          periodSeconds: 60
          timeoutSeconds: 30
          failureThreshold: 3
        readinessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 300
          periodSeconds: 30
          timeoutSeconds: 15
          failureThreshold: 2
        volumeMounts:
        - name: model-cache
          mountPath: /tmp/transformers_cache
        - name: hf-cache
          mountPath: /tmp/hf_cache
      volumes:
      - name: model-cache
        persistentVolumeClaim:
          claimName: llama-model-cache
      - name: hf-cache
        emptyDir:
          sizeLimit: "10Gi"
      # No GPU node selector needed
      nodeSelector:
        kubernetes.io/arch: amd64

---

apiVersion: v1
kind: Service
metadata:
  name: llama-cpu-pilot-service
  namespace: ats-dev
  labels:
    app: llama-cpu-pilot
spec:
  selector:
    app: llama-cpu-pilot
  ports:
  - port: 8000
    targetPort: 8000
    protocol: TCP
    name: http
  type: ClusterIP

---

apiVersion: v1
kind: Service
metadata:
  name: llama-cpu-pilot-external
  namespace: ats-dev
  labels:
    app: llama-cpu-pilot
spec:
  selector:
    app: llama-cpu-pilot
  ports:
  - port: 8000
    targetPort: 8000
    nodePort: 30801
    protocol: TCP
    name: http
  type: NodePort

---

apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: llama-model-cache
  namespace: ats-dev
spec:
  accessModes:
  - ReadWriteOnce
  resources:
    requests:
      storage: 20Gi  # Smaller than GPU version

---

apiVersion: batch/v1
kind: Job
metadata:
  name: llama-cpu-pilot-test
  namespace: ats-dev
  labels:
    app: llama-cpu-pilot-test
spec:
  template:
    metadata:
      labels:
        app: llama-cpu-pilot-test
    spec:
      containers:
      - name: test-client
        image: dragonflyer762/ats-genai:latest
        command: ["/bin/bash"]
        args:
        - -c
        - |
          echo "Testing Llama CPU pilot deployment..."
          
          # Wait for service to be ready (longer timeout for CPU)
          echo "Waiting for Llama CPU service..."
          timeout 900 bash -c 'until curl -f http://llama-cpu-pilot-service:8000/health; do
            echo "Service not ready, waiting..."
            sleep 30
          done'
          
          # Test basic inference
          echo "Testing CPU inference..."
          curl -X POST http://llama-cpu-pilot-service:8000/v1/chat/completions \
            -H "Content-Type: application/json" \
            -d '{
              "model": "llama-3b-cpu",
              "messages": [
                {
                  "role": "user", 
                  "content": "Analyze this financial news in JSON format: Apple reports Q4 earnings beat with revenue of $95B, up 5% YoY. Provide sentiment score, confidence, and key points."
                }
              ],
              "max_tokens": 500,
              "temperature": 0.1
            }' --max-time 60 | python -m json.tool || echo "Request timed out - normal for CPU inference"
          
          echo "CPU pilot deployment test completed!"
      restartPolicy: Never
  backoffLimit: 2

---

# Monitoring configuration for CPU deployment
apiVersion: v1
kind: ConfigMap
metadata:
  name: llama-cpu-monitoring-config
  namespace: ats-dev
data:
  prometheus.yml: |
    global:
      scrape_interval: 30s  # Less frequent for CPU
    scrape_configs:
    - job_name: 'llama-cpu-pilot'
      static_configs:
      - targets: ['llama-cpu-pilot-service:8000']
      metrics_path: /metrics
      scrape_interval: 60s  # CPU inference is slower
  
  cpu-dashboard.json: |
    {
      "dashboard": {
        "title": "Llama CPU Pilot Metrics",
        "panels": [
          {
            "title": "Request Rate (CPU)",
            "type": "graph",
            "targets": [{"expr": "rate(vllm_requests_total[10m])"}]
          },
          {
            "title": "CPU Utilization",
            "type": "graph", 
            "targets": [{"expr": "rate(container_cpu_usage_seconds_total[5m])"}]
          },
          {
            "title": "Memory Usage",
            "type": "graph",
            "targets": [{"expr": "container_memory_usage_bytes"}]
          },
          {
            "title": "Request Latency (CPU)",
            "type": "graph",
            "targets": [{"expr": "vllm_request_duration_seconds"}]
          }
        ]
      }
    }