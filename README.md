# Práctica Creativa Álvaro Jiménez Arce — IBDN Flight Prediction

Repositorio GitHub:  
https://github.com/aljarce44/IBDN_PracticaCreativa_Flight_Prediction

## Descarga del proyecto

Para descargar el proyecto desde GitHub:

```bash
git clone https://github.com/aljarce44/IBDN_PracticaCreativa_Flight_Prediction.git practica_creativa
cd practica_creativa```

## README

Desplegar la práctica completa en 4 entornos:

1. Docker local  
2. Kubernetes local  
3. Docker en Google Cloud mediante VM  
4. Kubernetes en Google Cloud mediante GKE  

---

# 0. Requisitos

## 0.1. Requisitos instalados

Antes de ejecutar la práctica, el equipo debe tener instalado:

- Docker Desktop
- Docker Compose
- kubectl
- Google Cloud CLI: gcloud
- Navegador web
- Conexión a Internet

Si se usa Windows, también debe estar instalado:

- WSL 2 con Ubuntu

Si se usa Linux, no hace falta WSL.

Comprobar versiones:

```bash
docker --version
docker compose version
kubectl version --client
gcloud version
```

Versiones usadas/recomendadas:

```text
Docker Desktop: versión reciente
Docker Compose: v2.x
kubectl: compatible con Kubernetes actual
gcloud CLI: versión reciente
Spark: 3.5.3
Kafka: 7.6.1
Cassandra: 4.1
Python: 3.12
MinIO: latest
```

En Google Cloud debe existir:

```text
Proyecto: ibdn-flight-stack-alvaro
Zona: europe-west1-b
VM Docker: ibdn-docker-vm
Cluster GKE: ibdn-flight-cluster
Artifact Registry: europe-west1-docker.pkg.dev/ibdn-flight-stack-alvaro/ibdn-docker
```

---

## 0.2. Abrir entorno

Abrir Docker Desktop, si es necesario, y esperar a que esté iniciado completamente.

Si se usa Windows, abrir PowerShell o CMD y entrar en WSL:

```powershell
wsl
```

Si se usa Linux, abrir una terminal normal.

Entrar en la carpeta del proyecto descargado:

```bash
cd practica_creativa
```

Comprobar que estamos en la carpeta correcta:

```bash
ls
```

Debe aparecer algo parecido a:

```text
docker-compose.yml
k8s
models
resources
lakehouse
docker
```

Comprobar configuración de Google Cloud:

```bash
gcloud config get-value project
gcloud config get-value compute/region
gcloud config get-value compute/zone
```

Debe salir:

```text
ibdn-flight-stack-alvaro
europe-west1
europe-west1-b
```

Si no sale, configurar:

```bash
gcloud config set project ibdn-flight-stack-alvaro
gcloud config set compute/region europe-west1
gcloud config set compute/zone europe-west1-b
```

---

# 1. Docker local

Web final:

```text
http://127.0.0.1:5001/flights/delays/predict_kafka
```

## 1.1. Construir imágenes

Desde la carpeta `practica_creativa`:

```bash
docker compose build flask
docker compose build spark
```

---

## 1.2. Levantar servicios

```bash
docker compose up -d
```

Esperar:

```text
2-5 minutos
```

Comprobar contenedores:

```bash
docker compose ps
```

Debe aparecer `Up` en:

```text
cassandra-flight-docker
flight-flask
flight-kafka
flight-spark-streaming
flight-zookeeper
minio-lakehouse-docker
```

Si Kafka o Spark no aparecen como `Up`, ejecutar:

```bash
docker compose up -d
```

Esperar 1 minuto y volver a comprobar:

```bash
docker compose ps
```

---

## 1.3. Preparar Cassandra

Cassandra puede tardar aunque el contenedor aparezca como `Up`. Esperar 1 minuto antes de ejecutar estos comandos.

Crear keyspace:

```bash
docker exec -it cassandra-flight-docker cqlsh -e "CREATE KEYSPACE IF NOT EXISTS agile_data_science WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
```

Crear tabla de predicciones:

```bash
docker exec -it cassandra-flight-docker cqlsh -e "CREATE TABLE IF NOT EXISTS agile_data_science.flight_delay_ml_response (uuid text PRIMARY KEY, origin text, dest text, carrier text, route text, distance double, prediction text, timestamp text, day_of_month int, day_of_week int, day_of_year int, dep_delay double, flight_date text);"
```

Crear tabla de distancias:

```bash
docker exec -it cassandra-flight-docker cqlsh -e "CREATE TABLE IF NOT EXISTS agile_data_science.flight_distances (origin text, dest text, distance double, PRIMARY KEY (origin, dest));"
```

Insertar distancias de prueba:

```bash
docker exec -it cassandra-flight-docker cqlsh -e "INSERT INTO agile_data_science.flight_distances (origin, dest, distance) VALUES ('ATL', 'SFO', 2139);"
docker exec -it cassandra-flight-docker cqlsh -e "INSERT INTO agile_data_science.flight_distances (origin, dest, distance) VALUES ('SFO', 'ATL', 2139);"
docker exec -it cassandra-flight-docker cqlsh -e "INSERT INTO agile_data_science.flight_distances (origin, dest, distance) VALUES ('JFK', 'LAX', 2475);"
docker exec -it cassandra-flight-docker cqlsh -e "INSERT INTO agile_data_science.flight_distances (origin, dest, distance) VALUES ('LAX', 'JFK', 2475);"
```

Comprobar tablas:

```bash
docker exec -it cassandra-flight-docker cqlsh -k agile_data_science -e "DESCRIBE TABLES;"
```

Debe aparecer:

```text
flight_delay_ml_response
flight_distances
```

---

## 1.4. Preparar MinIO y modelos

Crear bucket y carpetas:

```bash
docker run --rm --network practica_creativa_default --entrypoint /bin/sh minio/mc -c "mc alias set local http://minio:9000 admin password123 && mc mb -p local/lakehouse && mc mb -p local/lakehouse/models && mc mb -p local/lakehouse/checkpoints"
```

Subir modelos:

```bash
docker run --rm --network practica_creativa_default -v "$(pwd)/models:/models" --entrypoint /bin/sh minio/mc -c "mc alias set local http://minio:9000 admin password123 && mc cp -r /models/* local/lakehouse/models/"
```

Comprobar modelos:

```bash
docker run --rm --network practica_creativa_default --entrypoint /bin/sh minio/mc -c "mc alias set local http://minio:9000 admin password123 && mc ls local/lakehouse/models/"
```

Deben aparecer:

```text
arrival_bucketizer_2.0.bin/
numeric_vector_assembler.bin/
spark_random_forest_classifier.flight_delays.5.0.bin/
string_indexer_model_Carrier.bin/
string_indexer_model_Dest.bin/
string_indexer_model_Origin.bin/
string_indexer_model_Route.bin/
```

---

## 1.5. Preparar Kafka

Asegurar que Zookeeper y Kafka están levantados:

```bash
docker compose up -d zookeeper kafka
```

Esperar:

```text
1 minuto
```

Crear topics:

```bash
docker exec -it flight-kafka kafka-topics --bootstrap-server flight-kafka:29092 --create --if-not-exists --topic flight-delay-ml-request --partitions 1 --replication-factor 1
docker exec -it flight-kafka kafka-topics --bootstrap-server flight-kafka:29092 --create --if-not-exists --topic flight-delay-ml-response --partitions 1 --replication-factor 1
```

Comprobar topics:

```bash
docker exec -it flight-kafka kafka-topics --bootstrap-server flight-kafka:29092 --list
```

Debe aparecer:

```text
flight-delay-ml-request
flight-delay-ml-response
```

---

## 1.6. Limpiar checkpoints y reiniciar Spark/Flask

Limpiar checkpoints de Spark:

```bash
docker run --rm --network practica_creativa_default --entrypoint /bin/sh minio/mc -c "mc alias set local http://minio:9000 admin password123 && mc rm -r --force local/lakehouse/checkpoints || true && mc mb -p local/lakehouse/checkpoints"
```

Reiniciar Spark y Flask:

```bash
docker compose restart spark flask
```

Esperar:

```text
1-2 minutos
```

Comprobar Spark:

```bash
docker compose logs spark --tail 150
```

Debe aparecer:

```text
Streaming arrancado. Esperando mensajes en Kafka...
```

o:

```text
Batch vacío, esperando mensajes...
```

---

## 1.7. Probar Docker local

Abrir en el navegador:

```text
http://127.0.0.1:5001/flights/delays/predict_kafka
```

Rellenar el formulario de predicción y pulsar **Submit**, en el botón naranja.

Comprobar que Cassandra ha guardado la predicción:

```bash
docker exec -it cassandra-flight-docker cqlsh -e "SELECT uuid, origin, dest, carrier, route, distance, prediction, timestamp FROM agile_data_science.flight_delay_ml_response;"
```

Debe aparecer al menos una fila con la predicción.

Antes de pasar a Kubernetes local, parar Docker local para liberar recursos:

```bash
docker compose down
```

---

# 2. Kubernetes local

Web final:

```text
http://127.0.0.1:5002/flights/delays/predict_kafka
```

Todos los comandos de este apartado se ejecutan desde la terminal, dentro de `practica_creativa`.

---

## 2.1. Cambiar a Kubernetes local

```bash
kubectl config use-context docker-desktop
```

Comprobar nodo:

```bash
kubectl get nodes
```

Debe aparecer un nodo en estado:

```text
Ready
```

---

## 2.2. Crear registry local y subir imágenes

Crear registry local:

```bash
docker run -d -p 5000:5000 --restart=always --name local-registry registry:2 || docker start local-registry
```

Etiquetar imágenes:

```bash
docker tag practica_creativa-flask:latest localhost:5000/practica_creativa-flask:visual-local
docker tag practica_creativa-spark:latest localhost:5000/practica_creativa-spark:latest
```

Subir imágenes:

```bash
docker push localhost:5000/practica_creativa-flask:visual-local
docker push localhost:5000/practica_creativa-spark:latest
```

---

## 2.3. Aplicar Kubernetes local

Aplicar manifiestos:

```bash
kubectl apply -f ./k8s/01-infra.yml
kubectl apply -f ./k8s/02-app.yml
```

Parar Flask y Spark inmediatamente para preparar primero Cassandra, MinIO y Kafka:

```bash
kubectl scale deployment flask -n flight-stack --replicas=0
kubectl scale deployment spark-streaming -n flight-stack --replicas=0
```

Actualizar imágenes:

```bash
kubectl set image deployment/flask flask=localhost:5000/practica_creativa-flask:visual-local -n flight-stack
kubectl set image deployment/spark-streaming spark-streaming=localhost:5000/practica_creativa-spark:latest -n flight-stack
```

Esperar:

```text
1-2 minutos
```

Comprobar pods:

```bash
kubectl get pods -n flight-stack
```

Deben aparecer los servicios principales:

```text
cassandra
kafka
minio
zookeeper
```

---

## 2.4. Arreglar/estabilizar Kafka

Parar Kafka:

```bash
kubectl scale deployment kafka -n flight-stack --replicas=0
```

Aplicar ajuste:

```bash
kubectl patch deployment kafka -n flight-stack --type strategic -p '{"spec":{"template":{"spec":{"enableServiceLinks":false}}}}'
```

Levantar Kafka:

```bash
kubectl scale deployment kafka -n flight-stack --replicas=1
```

Esperar:

```text
1 minuto
```

Comprobar:

```bash
kubectl get pods -n flight-stack
```

Kafka debe aparecer:

```text
1/1 Running
```

---

## 2.5. Preparar Cassandra en Kubernetes local

Crear keyspace:

```bash
kubectl exec -it -n flight-stack deployment/cassandra -- cqlsh -e "CREATE KEYSPACE IF NOT EXISTS agile_data_science WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
```

Crear tabla de predicciones:

```bash
kubectl exec -it -n flight-stack deployment/cassandra -- cqlsh -e "CREATE TABLE IF NOT EXISTS agile_data_science.flight_delay_ml_response (uuid text PRIMARY KEY, origin text, dest text, carrier text, route text, distance double, prediction text, timestamp text, day_of_month int, day_of_week int, day_of_year int, dep_delay double, flight_date text);"
```

Crear tabla de distancias:

```bash
kubectl exec -it -n flight-stack deployment/cassandra -- cqlsh -e "CREATE TABLE IF NOT EXISTS agile_data_science.flight_distances (origin text, dest text, distance double, PRIMARY KEY (origin, dest));"
```

Insertar distancias:

```bash
kubectl exec -it -n flight-stack deployment/cassandra -- cqlsh -e "INSERT INTO agile_data_science.flight_distances (origin, dest, distance) VALUES ('ATL', 'SFO', 2139);"
kubectl exec -it -n flight-stack deployment/cassandra -- cqlsh -e "INSERT INTO agile_data_science.flight_distances (origin, dest, distance) VALUES ('SFO', 'ATL', 2139);"
kubectl exec -it -n flight-stack deployment/cassandra -- cqlsh -e "INSERT INTO agile_data_science.flight_distances (origin, dest, distance) VALUES ('JFK', 'LAX', 2475);"
kubectl exec -it -n flight-stack deployment/cassandra -- cqlsh -e "INSERT INTO agile_data_science.flight_distances (origin, dest, distance) VALUES ('LAX', 'JFK', 2475);"
```

Comprobar tablas:

```bash
kubectl exec -it -n flight-stack deployment/cassandra -- cqlsh -k agile_data_science -e "DESCRIBE TABLES;"
```

Debe aparecer:

```text
flight_delay_ml_response
flight_distances
```

Levantar Flask:

```bash
kubectl scale deployment flask -n flight-stack --replicas=1
```

Esperar:

```text
30-60 segundos
```

Comprobar Flask:

```bash
kubectl get pods -n flight-stack
```

Flask debe aparecer:

```text
1/1 Running
```

Comprobar que Flask tiene la web nueva:

```bash
kubectl exec -it -n flight-stack deployment/flask -- grep -n "Flight Delay Control Tower\|history-toggle" /app/resources/web/templates/flight_delays_predict_kafka.html
```

Debe aparecer:

```text
Flight Delay Control Tower
history-toggle
```

---

## 2.6. Preparar MinIO en Kubernetes local

Crear bucket y carpetas:

```bash
kubectl exec -it -n flight-stack deployment/minio -- sh -c "mc alias set local http://localhost:9000 admin password123 && mc mb -p local/lakehouse && mc mb -p local/lakehouse/models && mc mb -p local/lakehouse/checkpoints"
```

Abrir otra terminal.

Si se usa Windows, entrar en WSL:

```powershell
wsl
```

Entrar de nuevo en la carpeta del proyecto:

```bash
cd practica_creativa
```

Abrir port-forward de MinIO y dejar esta terminal abierta:

```bash
kubectl port-forward -n flight-stack svc/minio 9001:9000
```

Abrir otra terminal.

Si se usa Windows, entrar en WSL:

```powershell
wsl
```

Entrar de nuevo en la carpeta del proyecto:

```bash
cd practica_creativa
```

Subir modelos:

```bash
rm -f mc
curl -L https://dl.min.io/client/mc/release/linux-amd64/mc -o mc
chmod +x mc
./mc alias set local http://127.0.0.1:9001 admin password123
./mc rm -r --force local/lakehouse/models
./mc mb -p local/lakehouse/models
./mc cp -r models/* local/lakehouse/models/
./mc ls local/lakehouse/models/
```

Deben aparecer:

```text
arrival_bucketizer_2.0.bin/
numeric_vector_assembler.bin/
spark_random_forest_classifier.flight_delays.5.0.bin/
string_indexer_model_Carrier.bin/
string_indexer_model_Dest.bin/
string_indexer_model_Origin.bin/
string_indexer_model_Route.bin/
```

Cerrar la terminal del port-forward de MinIO.

---

## 2.7. Preparar topics Kafka en Kubernetes local

Obtener pod exacto de Kafka:

```bash
KAFKA_POD=$(kubectl get pod -n flight-stack -l app=kafka -o jsonpath='{.items[0].metadata.name}')
echo $KAFKA_POD
```

Crear topics:

```bash
kubectl exec -it -n flight-stack $KAFKA_POD -c kafka -- kafka-topics --bootstrap-server localhost:29092 --create --if-not-exists --topic flight-delay-ml-request --partitions 1 --replication-factor 1
kubectl exec -it -n flight-stack $KAFKA_POD -c kafka -- kafka-topics --bootstrap-server localhost:29092 --create --if-not-exists --topic flight-delay-ml-response --partitions 1 --replication-factor 1
```

Comprobar topics:

```bash
kubectl exec -it -n flight-stack $KAFKA_POD -c kafka -- kafka-topics --bootstrap-server localhost:29092 --list
```

Debe aparecer:

```text
flight-delay-ml-request
flight-delay-ml-response
```

---

## 2.8. Limpiar checkpoints y levantar Spark

Limpiar checkpoints:

```bash
kubectl exec -it -n flight-stack deployment/minio -- sh -c "mc alias set local http://localhost:9000 admin password123 && mc rm -r --force local/lakehouse/checkpoints || true && mc mb -p local/lakehouse/checkpoints"
```

Levantar Spark:

```bash
kubectl scale deployment spark-streaming -n flight-stack --replicas=1
```

Esperar:

```text
2-4 minutos
```

Comprobar Spark:

```bash
kubectl get pods -n flight-stack
```

Spark debe aparecer:

```text
spark-streaming    1/1 Running
```

Comprobar logs:

```bash
kubectl logs -n flight-stack deployment/spark-streaming --tail 150
```

Debe aparecer:

```text
Streaming arrancado. Esperando mensajes en Kafka...
```

o:

```text
Batch vacío, esperando mensajes...
```

---

## 2.9. Probar Kubernetes local

Abrir port-forward de Flask:

```bash
kubectl port-forward -n flight-stack svc/flask 5002:5001
```

Dejar esa terminal abierta.

Abrir en el navegador:

```text
http://127.0.0.1:5002/flights/delays/predict_kafka
```

Rellenar el formulario de predicción y pulsar **Submit**, igual, en el botón naranja.

Comprobar Cassandra:

```bash
kubectl exec -it -n flight-stack deployment/cassandra -- cqlsh -e "SELECT uuid, origin, dest, carrier, route, distance, prediction, timestamp FROM agile_data_science.flight_delay_ml_response;"
```

Debe aparecer al menos una predicción.

---

# 3. Docker en Google Cloud

VM:

```text
ibdn-docker-vm
```

Web final:

```text
http://EXTERNAL_IP_VM:5001/flights/delays/predict_kafka
```

---

## 3.1. Arrancar VM

Desde terminal local:

```bash
gcloud compute instances start ibdn-docker-vm --zone=europe-west1-b
```

Esperar:

```text
1-2 minutos
```

Comprobar VM:

```bash
gcloud compute instances list
```

Debe aparecer:

```text
ibdn-docker-vm    RUNNING
```

Apuntar la IP pública de la columna:

```text
EXTERNAL_IP
```

---

## 3.2. Entrar en la VM

```bash
gcloud compute ssh ibdn-docker-vm --zone=europe-west1-b
```

Dentro de la VM:

```bash
cd ~/practica_creativa
```

---

## 3.3. Levantar Docker en la VM

```bash
docker compose up -d
```

Esperar:

```text
2-5 minutos
```

Comprobar:

```bash
docker compose ps
```

Deben aparecer `Up`:

```text
cassandra-flight-docker
flight-flask
flight-kafka
flight-spark-streaming
flight-zookeeper
minio-lakehouse-docker
```

Si Kafka o Spark no aparecen como `Up`, ejecutar:

```bash
docker compose up -d
```

Esperar 1 minuto y volver a comprobar:

```bash
docker compose ps
```

---

## 3.4. Preparar Cassandra en la VM

```bash
docker exec -it cassandra-flight-docker cqlsh -e "CREATE KEYSPACE IF NOT EXISTS agile_data_science WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
docker exec -it cassandra-flight-docker cqlsh -e "CREATE TABLE IF NOT EXISTS agile_data_science.flight_delay_ml_response (uuid text PRIMARY KEY, origin text, dest text, carrier text, route text, distance double, prediction text, timestamp text, day_of_month int, day_of_week int, day_of_year int, dep_delay double, flight_date text);"
docker exec -it cassandra-flight-docker cqlsh -e "CREATE TABLE IF NOT EXISTS agile_data_science.flight_distances (origin text, dest text, distance double, PRIMARY KEY (origin, dest));"
docker exec -it cassandra-flight-docker cqlsh -e "INSERT INTO agile_data_science.flight_distances (origin, dest, distance) VALUES ('ATL', 'SFO', 2139);"
docker exec -it cassandra-flight-docker cqlsh -e "INSERT INTO agile_data_science.flight_distances (origin, dest, distance) VALUES ('SFO', 'ATL', 2139);"
docker exec -it cassandra-flight-docker cqlsh -e "INSERT INTO agile_data_science.flight_distances (origin, dest, distance) VALUES ('JFK', 'LAX', 2475);"
docker exec -it cassandra-flight-docker cqlsh -e "INSERT INTO agile_data_science.flight_distances (origin, dest, distance) VALUES ('LAX', 'JFK', 2475);"
```

Comprobar tablas:

```bash
docker exec -it cassandra-flight-docker cqlsh -k agile_data_science -e "DESCRIBE TABLES;"
```

---

## 3.5. Preparar MinIO y Kafka en la VM

```bash
docker run --rm --network practica_creativa_default --entrypoint /bin/sh minio/mc -c "mc alias set local http://minio:9000 admin password123 && mc mb -p local/lakehouse && mc mb -p local/lakehouse/models && mc mb -p local/lakehouse/checkpoints"
```

```bash
docker run --rm --network practica_creativa_default -v "$(pwd)/models:/models" --entrypoint /bin/sh minio/mc -c "mc alias set local http://minio:9000 admin password123 && mc cp -r /models/* local/lakehouse/models/"
```

Asegurar Kafka:

```bash
docker compose up -d zookeeper kafka
```

Esperar:

```text
1 minuto
```

Crear topics:

```bash
docker exec -it flight-kafka kafka-topics --bootstrap-server flight-kafka:29092 --create --if-not-exists --topic flight-delay-ml-request --partitions 1 --replication-factor 1
docker exec -it flight-kafka kafka-topics --bootstrap-server flight-kafka:29092 --create --if-not-exists --topic flight-delay-ml-response --partitions 1 --replication-factor 1
```

Comprobar:

```bash
docker exec -it flight-kafka kafka-topics --bootstrap-server flight-kafka:29092 --list
```

Debe aparecer:

```text
flight-delay-ml-request
flight-delay-ml-response
```

---

## 3.6. Limpiar checkpoints y reiniciar Spark/Flask en la VM

```bash
docker run --rm --network practica_creativa_default --entrypoint /bin/sh minio/mc -c "mc alias set local http://minio:9000 admin password123 && mc rm -r --force local/lakehouse/checkpoints || true && mc mb -p local/lakehouse/checkpoints"
```

```bash
docker compose restart spark flask
```

Esperar:

```text
1-2 minutos
```

Comprobar Spark:

```bash
docker compose logs spark --tail 150
```

Debe aparecer:

```text
Streaming arrancado. Esperando mensajes en Kafka...
```

---

## 3.7. Probar Docker en Google Cloud

Abrir en el navegador:

```text
http://EXTERNAL_IP_VM:5001/flights/delays/predict_kafka
```

Rellenar el formulario de predicción y pulsar **Submit**, igual que con los otros.

Comprobar Cassandra:

```bash
docker exec -it cassandra-flight-docker cqlsh -e "SELECT uuid, origin, dest, carrier, route, distance, prediction, timestamp FROM agile_data_science.flight_delay_ml_response;"
```

Salir de la VM:

```bash
exit
```

---

# 4. Kubernetes en Google Cloud

Web final:

```text
http://EXTERNAL_IP_LOADBALANCER:5001/flights/delays/predict_kafka
```

Todos los comandos de este apartado se ejecutan desde terminal local, dentro de `practica_creativa`.

---

## 4.1. Conectar con GKE

```bash
gcloud container clusters get-credentials ibdn-flight-cluster --zone europe-west1-b --project ibdn-flight-stack-alvaro
```

Comprobar contexto:

```bash
kubectl config current-context
```

Debe salir:

```text
gke_ibdn-flight-stack-alvaro_europe-west1-b_ibdn-flight-cluster
```

Comprobar nodos:

```bash
kubectl get nodes
```

Deben aparecer nodos `gke-...`.

---

## 4.2. Definir imágenes

```bash
PROJECT_ID=ibdn-flight-stack-alvaro
REGION=europe-west1
REPO=ibdn-docker
TAG=visual-v1
```

---

## 4.3. Aplicar Kubernetes en GKE

```bash
kubectl apply -f ./k8s/01-infra.yml
kubectl apply -f ./k8s/02-app.yml
```

Parar Flask y Spark inmediatamente para preparar primero Cassandra, MinIO y Kafka:

```bash
kubectl scale deployment flask -n flight-stack --replicas=0
kubectl scale deployment spark-streaming -n flight-stack --replicas=0
```

Actualizar imágenes:

```bash
kubectl set image deployment/flask flask=$REGION-docker.pkg.dev/$PROJECT_ID/$REPO/practica_creativa-flask:$TAG -n flight-stack
kubectl set image deployment/spark-streaming spark-streaming=$REGION-docker.pkg.dev/$PROJECT_ID/$REPO/practica_creativa-spark:$TAG -n flight-stack
```

Esperar:

```text
1-2 minutos
```

Comprobar pods:

```bash
kubectl get pods -n flight-stack
```

Deben aparecer:

```text
cassandra
kafka
minio
zookeeper
```

---

## 4.4. Estabilizar Kafka en GKE

Parar Kafka:

```bash
kubectl scale deployment kafka -n flight-stack --replicas=0
```

Aplicar ajuste:

```bash
kubectl patch deployment kafka -n flight-stack --type strategic -p '{"spec":{"template":{"spec":{"enableServiceLinks":false}}}}'
```

Levantar Kafka:

```bash
kubectl scale deployment kafka -n flight-stack --replicas=1
```

Esperar:

```text
1 minuto
```

Comprobar:

```bash
kubectl get pods -n flight-stack
```

Kafka debe aparecer:

```text
1/1 Running
```

---

## 4.5. Preparar Cassandra en GKE

Crear keyspace y tablas:

```bash
kubectl exec -it -n flight-stack deployment/cassandra -- cqlsh -e "CREATE KEYSPACE IF NOT EXISTS agile_data_science WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
kubectl exec -it -n flight-stack deployment/cassandra -- cqlsh -e "CREATE TABLE IF NOT EXISTS agile_data_science.flight_delay_ml_response (uuid text PRIMARY KEY, origin text, dest text, carrier text, route text, distance double, prediction text, timestamp text, day_of_month int, day_of_week int, day_of_year int, dep_delay double, flight_date text);"
kubectl exec -it -n flight-stack deployment/cassandra -- cqlsh -e "CREATE TABLE IF NOT EXISTS agile_data_science.flight_distances (origin text, dest text, distance double, PRIMARY KEY (origin, dest));"
```

Insertar distancias:

```bash
kubectl exec -it -n flight-stack deployment/cassandra -- cqlsh -e "INSERT INTO agile_data_science.flight_distances (origin, dest, distance) VALUES ('ATL', 'SFO', 2139);"
kubectl exec -it -n flight-stack deployment/cassandra -- cqlsh -e "INSERT INTO agile_data_science.flight_distances (origin, dest, distance) VALUES ('SFO', 'ATL', 2139);"
kubectl exec -it -n flight-stack deployment/cassandra -- cqlsh -e "INSERT INTO agile_data_science.flight_distances (origin, dest, distance) VALUES ('JFK', 'LAX', 2475);"
kubectl exec -it -n flight-stack deployment/cassandra -- cqlsh -e "INSERT INTO agile_data_science.flight_distances (origin, dest, distance) VALUES ('LAX', 'JFK', 2475);"
```

Comprobar tablas:

```bash
kubectl exec -it -n flight-stack deployment/cassandra -- cqlsh -k agile_data_science -e "DESCRIBE TABLES;"
```

Levantar Flask:

```bash
kubectl scale deployment flask -n flight-stack --replicas=1
```

Esperar:

```text
30-60 segundos
```

---

## 4.6. Preparar MinIO en GKE

Crear bucket y carpetas:

```bash
kubectl exec -it -n flight-stack deployment/minio -- sh -c "mc alias set local http://localhost:9000 admin password123 && mc mb -p local/lakehouse && mc mb -p local/lakehouse/models && mc mb -p local/lakehouse/checkpoints"
```

Abrir otra terminal.

Si se usa Windows, entrar en WSL:

```powershell
wsl
```

Entrar de nuevo en la carpeta del proyecto:

```bash
cd practica_creativa
```

Abrir port-forward de MinIO y dejar esta terminal abierta:

```bash
kubectl port-forward -n flight-stack svc/minio 9001:9000
```

Abrir otra terminal.

Si se usa Windows, entrar en WSL:

```powershell
wsl
```

Entrar de nuevo en la carpeta del proyecto:

```bash
cd practica_creativa
```

Subir modelos:

```bash
rm -f mc
curl -L https://dl.min.io/client/mc/release/linux-amd64/mc -o mc
chmod +x mc
./mc alias set local http://127.0.0.1:9001 admin password123
./mc rm -r --force local/lakehouse/models
./mc mb -p local/lakehouse/models
./mc cp -r models/* local/lakehouse/models/
./mc ls local/lakehouse/models/
```

Cerrar port-forward de MinIO.

---

## 4.7. Preparar Kafka topics en GKE

Obtener pod exacto:

```bash
KAFKA_POD=$(kubectl get pod -n flight-stack -l app=kafka -o jsonpath='{.items[0].metadata.name}')
echo $KAFKA_POD
```

Crear topics:

```bash
kubectl exec -it -n flight-stack $KAFKA_POD -c kafka -- kafka-topics --bootstrap-server localhost:29092 --create --if-not-exists --topic flight-delay-ml-request --partitions 1 --replication-factor 1
kubectl exec -it -n flight-stack $KAFKA_POD -c kafka -- kafka-topics --bootstrap-server localhost:29092 --create --if-not-exists --topic flight-delay-ml-response --partitions 1 --replication-factor 1
```

Comprobar topics:

```bash
kubectl exec -it -n flight-stack $KAFKA_POD -c kafka -- kafka-topics --bootstrap-server localhost:29092 --list
```

Debe aparecer:

```text
flight-delay-ml-request
flight-delay-ml-response
```

---

## 4.8. Limpiar checkpoints y levantar Spark en GKE

Limpiar checkpoints:

```bash
kubectl exec -it -n flight-stack deployment/minio -- sh -c "mc alias set local http://localhost:9000 admin password123 && mc rm -r --force local/lakehouse/checkpoints || true && mc mb -p local/lakehouse/checkpoints"
```

Levantar Spark:

```bash
kubectl scale deployment spark-streaming -n flight-stack --replicas=1
```

Esperar:

```text
2-4 minutos
```

Comprobar Spark:

```bash
kubectl logs -n flight-stack deployment/spark-streaming --tail 150
```

Debe aparecer:

```text
Streaming arrancado. Esperando mensajes en Kafka...
```

---

## 4.9. Exponer web GKE

```bash
kubectl patch svc flask -n flight-stack -p '{"spec":{"type":"LoadBalancer"}}'
```

Comprobar IP pública:

```bash
kubectl get svc flask -n flight-stack
```

Al principio puede aparecer:

```text
EXTERNAL-IP   <pending>
```

Esperar:

```text
1-5 minutos
```

Volver a comprobar:

```bash
kubectl get svc flask -n flight-stack
```

Cuando aparezca una IP pública, abrir en navegador:

```text
http://EXTERNAL_IP_LOADBALANCER:5001/flights/delays/predict_kafka
```

Rellenar el formulario de predicción y pulsar **Submit**, en el botón naranja.

Comprobar Cassandra:

```bash
kubectl exec -it -n flight-stack deployment/cassandra -- cqlsh -e "SELECT uuid, origin, dest, carrier, route, distance, prediction, timestamp FROM agile_data_science.flight_delay_ml_response;"
```

Debe aparecer al menos una predicción.

---

# Predicciones anteriores

Si quiere ver las anteriores predicciones puede pulsar el botón **Ver anteriores predicciones** y se ven.
