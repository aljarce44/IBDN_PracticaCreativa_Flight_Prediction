Práctica Creativa Álvaro Jiménez Arce IBDN Flight Prediction GitHub:


README: 
Desplegar la práctica completa en 4 entornos:
Docker local
Kubernetes local
Docker en Google Cloud mediante VM
Kubernetes en Google Cloud mediante GKE
0. Requisitos
0.1. Requisitos instalados
Antes de ejecutar la práctica, el equipo debe tener instalado:
Docker Desktop
Docker Compose
kubectl
Google Cloud CLI: gcloud
Navegador web
Conexión a Internet
Si se usa Windows, también debe estar instalado:
WSL 2 con Ubuntu
Si se usa Linux, no hace falta WSL.
Comprobar versiones:
docker --version
docker compose version
kubectl version --client
gcloud version
Versiones usadas/recomendadas:
Docker Desktop: versión reciente
Docker Compose: v2.x
kubectl: compatible con Kubernetes actual
gcloud CLI: versión reciente
Spark: 3.5.3
Kafka: 7.6.1
Cassandra: 4.1
Python: 3.12
MinIO: latest
En Google Cloud debe existir:
Proyecto: ibdn-flight-stack-alvaro
Zona: europe-west1-b
VM Docker: ibdn-docker-vm
Cluster GKE: ibdn-flight-cluster
Artifact Registry: europe-west1-docker.pkg.dev/ibdn-flight-stack-alvaro/ibdn-docker
0.2. Abrir entorno
Abrir Docker Desktop, si es necesario, y esperar a que esté iniciado completamente.
Si se usa Windows, abrir PowerShell o CMD y entrar en WSL:
wsl
Si se usa Linux, abrir una terminal normal.
Entrar en la carpeta del proyecto descargado:
cd practica_creativa
Comprobar que estamos en la carpeta correcta:
ls
Debe aparecer algo parecido a:
docker-compose.yml
k8s
models
resources
lakehouse
docker
Comprobar configuración de Google Cloud:
gcloud config get-value project
gcloud config get-value compute/region
gcloud config get-value compute/zone
Debe salir:
ibdn-flight-stack-alvaro
europe-west1
europe-west1-b
Si no sale, configurar:
gcloud config set project ibdn-flight-stack-alvaro
gcloud config set compute/region europe-west1
gcloud config set compute/zone europe-west1-b
1. Docker local
Web final:
http://127.0.0.1:5001/flights/delays/predict_kafka
1.1. Construir imágenes
Desde la carpeta practica_creativa:
docker compose build flask
docker compose build spark
1.2. Levantar servicios
docker compose up -d
Esperar: 2-5 minutos
Comprobar contenedores:
docker compose ps
Debe aparecer Up en:
cassandra-flight-docker
flight-flask
flight-kafka
flight-spark-streaming
flight-zookeeper
minio-lakehouse-docker
Si Kafka o Spark no aparecen como Up, ejecutar:
docker compose up -d
Esperar 1 minuto y volver a comprobar:
docker compose ps


1.3. Preparar Cassandra
Cassandra puede tardar aunque el contenedor aparezca como Up. Esperar 1 minuto antes de ejecutar estos comandos.
Crear keyspace:
docker exec -it cassandra-flight-docker cqlsh -e "CREATE KEYSPACE IF NOT EXISTS agile_data_science WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
Crear tabla de predicciones:
docker exec -it cassandra-flight-docker cqlsh -e "CREATE TABLE IF NOT EXISTS agile_data_science.flight_delay_ml_response (uuid text PRIMARY KEY, origin text, dest text, carrier text, route text, distance double, prediction text, timestamp text, day_of_month int, day_of_week int, day_of_year int, dep_delay double, flight_date text);"
Crear tabla de distancias:
docker exec -it cassandra-flight-docker cqlsh -e "CREATE TABLE IF NOT EXISTS agile_data_science.flight_distances (origin text, dest text, distance double, PRIMARY KEY (origin, dest));"
Insertar distancias de prueba:
docker exec -it cassandra-flight-docker cqlsh -e "INSERT INTO agile_data_science.flight_distances (origin, dest, distance) VALUES ('ATL', 'SFO', 2139);"
docker exec -it cassandra-flight-docker cqlsh -e "INSERT INTO agile_data_science.flight_distances (origin, dest, distance) VALUES ('SFO', 'ATL', 2139);"
docker exec -it cassandra-flight-docker cqlsh -e "INSERT INTO agile_data_science.flight_distances (origin, dest, distance) VALUES ('JFK', 'LAX', 2475);"
docker exec -it cassandra-flight-docker cqlsh -e "INSERT INTO agile_data_science.flight_distances (origin, dest, distance) VALUES ('LAX', 'JFK', 2475);"
Comprobar tablas:
docker exec -it cassandra-flight-docker cqlsh -k agile_data_science -e "DESCRIBE TABLES;"
Debe aparecer:
flight_delay_ml_response
flight_distances
1.4. Preparar MinIO y modelos
Crear bucket y carpetas:
docker run --rm --network practica_creativa_default --entrypoint /bin/sh minio/mc -c "mc alias set local http://minio:9000 admin password123 && mc mb -p local/lakehouse && mc mb -p local/lakehouse/models && mc mb -p local/lakehouse/checkpoints"
Subir modelos:
docker run --rm --network practica_creativa_default -v "$(pwd)/models:/models" --entrypoint /bin/sh minio/mc -c "mc alias set local http://minio:9000 admin password123 && mc cp -r /models/* local/lakehouse/models/"
Comprobar modelos:
docker run --rm --network practica_creativa_default --entrypoint /bin/sh minio/mc -c "mc alias set local http://minio:9000 admin password123 && mc ls local/lakehouse/models/"
Deben aparecer:
arrival_bucketizer_2.0.bin/
numeric_vector_assembler.bin/
spark_random_forest_classifier.flight_delays.5.0.bin/
string_indexer_model_Carrier.bin/
string_indexer_model_Dest.bin/
string_indexer_model_Origin.bin/
string_indexer_model_Route.bin/

1.5. Preparar Kafka
Asegurar que Zookeeper y Kafka están levantados:
docker compose up -d zookeeper kafka
Esperar: 1 minuto
Crear topics:
docker exec -it flight-kafka kafka-topics --bootstrap-server flight-kafka:29092 --create --if-not-exists --topic flight-delay-ml-request --partitions 1 --replication-factor 1
docker exec -it flight-kafka kafka-topics --bootstrap-server flight-kafka:29092 --create --if-not-exists --topic flight-delay-ml-response --partitions 1 --replication-factor 1
Comprobar topics:
docker exec -it flight-kafka kafka-topics --bootstrap-server flight-kafka:29092 --list
Debe aparecer:
flight-delay-ml-request
flight-delay-ml-response

1.6. Limpiar checkpoints y reiniciar Spark/Flask
Limpiar checkpoints de Spark:
docker run --rm --network practica_creativa_default --entrypoint /bin/sh minio/mc -c "mc alias set local http://minio:9000 admin password123 && mc rm -r --force local/lakehouse/checkpoints || true && mc mb -p local/lakehouse/checkpoints"
Reiniciar Spark y Flask:
docker compose restart spark flask
Esperar: 1-2 minutos
Comprobar Spark:
docker compose logs spark --tail 150
Debe aparecer:
Streaming arrancado. Esperando mensajes en Kafka...
o:
Batch vacío, esperando mensajes...

1.7. Probar Docker local
Abrir en el navegador:
http://127.0.0.1:5001/flights/delays/predict_kafka
Rellenar el formulario de predicción y pulsar Submit, en el botón naranja:
Comprobar que Cassandra ha guardado la predicción:
docker exec -it cassandra-flight-docker cqlsh -e "SELECT uuid, origin, dest, carrier, route, distance, prediction, timestamp FROM agile_data_science.flight_delay_ml_response;"
Debe aparecer al menos una fila con la predicción.
Antes de pasar a Kubernetes local, parar Docker local para liberar recursos:
docker compose down

2. Kubernetes local
Web final:
http://127.0.0.1:5002/flights/delays/predict_kafka
Todos los comandos de este apartado se ejecutan desde la terminal, dentro de practica_creativa.

2.1. Cambiar a Kubernetes local
kubectl config use-context docker-desktop
Comprobar nodo:
kubectl get nodes
Debe aparecer un nodo en estado:
Ready
2.2. Crear registry local y subir imágenes
Crear registry local:
docker run -d -p 5000:5000 --restart=always --name local-registry registry:2 || docker start local-registry
Etiquetar imágenes:
docker tag practica_creativa-flask:latest localhost:5000/practica_creativa-flask:visual-local
docker tag practica_creativa-spark:latest localhost:5000/practica_creativa-spark:latest
Subir imágenes:
docker push localhost:5000/practica_creativa-flask:visual-local
docker push localhost:5000/practica_creativa-spark:latest

2.3. Aplicar Kubernetes local
Aplicar manifiestos:
kubectl apply -f ./k8s/01-infra.yml
kubectl apply -f ./k8s/02-app.yml
Parar Flask y Spark inmediatamente para preparar primero Cassandra, MinIO y Kafka:
kubectl scale deployment flask -n flight-stack --replicas=0
kubectl scale deployment spark-streaming -n flight-stack --replicas=0
Actualizar imágenes:
kubectl set image deployment/flask flask=localhost:5000/practica_creativa-flask:visual-local -n flight-stack
kubectl set image deployment/spark-streaming spark-streaming=localhost:5000/practica_creativa-spark:latest -n flight-stack
Esperar: 1-2 minutos
Comprobar pods:
kubectl get pods -n flight-stack
Deben aparecer los servicios principales:
cassandra
kafka
minio
zookeeper
2.4. Arreglar/estabilizar Kafka
Parar Kafka:
kubectl scale deployment kafka -n flight-stack --replicas=0
Aplicar ajuste:
kubectl patch deployment kafka -n flight-stack --type strategic -p '{"spec":{"template":{"spec":{"enableServiceLinks":false}}}}'
Levantar Kafka:
kubectl scale deployment kafka -n flight-stack --replicas=1
Esperar: 1 minuto
Comprobar:
kubectl get pods -n flight-stack
Kafka debe aparecer:
1/1 Running

2.5. Preparar Cassandra en Kubernetes local
Crear keyspace:
kubectl exec -it -n flight-stack deployment/cassandra -- cqlsh -e "CREATE KEYSPACE IF NOT EXISTS agile_data_science WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
Crear tabla de predicciones:
kubectl exec -it -n flight-stack deployment/cassandra -- cqlsh -e "CREATE TABLE IF NOT EXISTS agile_data_science.flight_delay_ml_response (uuid text PRIMARY KEY, origin text, dest text, carrier text, route text, distance double, prediction text, timestamp text, day_of_month int, day_of_week int, day_of_year int, dep_delay double, flight_date text);"
Crear tabla de distancias:
kubectl exec -it -n flight-stack deployment/cassandra -- cqlsh -e "CREATE TABLE IF NOT EXISTS agile_data_science.flight_distances (origin text, dest text, distance double, PRIMARY KEY (origin, dest));"
Insertar distancias:
kubectl exec -it -n flight-stack deployment/cassandra -- cqlsh -e "INSERT INTO agile_data_science.flight_distances (origin, dest, distance) VALUES ('ATL', 'SFO', 2139);"
kubectl exec -it -n flight-stack deployment/cassandra -- cqlsh -e "INSERT INTO agile_data_science.flight_distances (origin, dest, distance) VALUES ('SFO', 'ATL', 2139);"
kubectl exec -it -n flight-stack deployment/cassandra -- cqlsh -e "INSERT INTO agile_data_science.flight_distances (origin, dest, distance) VALUES ('JFK', 'LAX', 2475);"
kubectl exec -it -n flight-stack deployment/cassandra -- cqlsh -e "INSERT INTO agile_data_science.flight_distances (origin, dest, distance) VALUES ('LAX', 'JFK', 2475);"
Comprobar tablas:
kubectl exec -it -n flight-stack deployment/cassandra -- cqlsh -k agile_data_science -e "DESCRIBE TABLES;"
Debe aparecer:
flight_delay_ml_response
flight_distances
Levantar Flask:
kubectl scale deployment flask -n flight-stack --replicas=1
Esperar: 30-60 segundos
Comprobar Flask:
kubectl get pods -n flight-stack
Flask debe aparecer:
1/1 Running
Comprobar que Flask tiene la web nueva:
kubectl exec -it -n flight-stack deployment/flask -- grep -n "Flight Delay Control Tower\|history-toggle" /app/resources/web/templates/flight_delays_predict_kafka.html
Debe aparecer:
Flight Delay Control Tower
history-toggle

2.6. Preparar MinIO en Kubernetes local
Crear bucket y carpetas:
kubectl exec -it -n flight-stack deployment/minio -- sh -c "mc alias set local http://localhost:9000 admin password123 && mc mb -p local/lakehouse && mc mb -p local/lakehouse/models && mc mb -p local/lakehouse/checkpoints"
Abrir otra terminal.
Si se usa Windows, entrar en WSL:
wsl
Entrar de nuevo en la carpeta del proyecto:
cd practica_creativa
Abrir port-forward de MinIO y dejar esta terminal abierta:
kubectl port-forward -n flight-stack svc/minio 9001:9000
Abrir otra terminal.
Si se usa Windows, entrar en WSL:
wsl
Entrar de nuevo en la carpeta del proyecto:
cd practica_creativa
Subir modelos:
rm -f mc
curl -L https://dl.min.io/client/mc/release/linux-amd64/mc -o mc
chmod +x mc
./mc alias set local http://127.0.0.1:9001 admin password123
./mc rm -r --force local/lakehouse/models
./mc mb -p local/lakehouse/models
./mc cp -r models/* local/lakehouse/models/
./mc ls local/lakehouse/models/
Deben aparecer:
arrival_bucketizer_2.0.bin/
numeric_vector_assembler.bin/
spark_random_forest_classifier.flight_delays.5.0.bin/
string_indexer_model_Carrier.bin/
string_indexer_model_Dest.bin/
string_indexer_model_Origin.bin/
string_indexer_model_Route.bin/
Cerrar la terminal del port-forward de MinIO.
2.7. Preparar topics Kafka en Kubernetes local
Obtener pod exacto de Kafka:
KAFKA_POD=$(kubectl get pod -n flight-stack -l app=kafka -o jsonpath='{.items[0].metadata.name}')
echo $KAFKA_POD
Crear topics:
kubectl exec -it -n flight-stack $KAFKA_POD -c kafka -- kafka-topics --bootstrap-server localhost:29092 --create --if-not-exists --topic flight-delay-ml-request --partitions 1 --replication-factor 1
kubectl exec -it -n flight-stack $KAFKA_POD -c kafka -- kafka-topics --bootstrap-server localhost:29092 --create --if-not-exists --topic flight-delay-ml-response --partitions 1 --replication-factor 1
Comprobar topics:
kubectl exec -it -n flight-stack $KAFKA_POD -c kafka -- kafka-topics --bootstrap-server localhost:29092 --list
Debe aparecer:
flight-delay-ml-request
flight-delay-ml-response

2.8. Limpiar checkpoints y levantar Spark
Limpiar checkpoints:
kubectl exec -it -n flight-stack deployment/minio -- sh -c "mc alias set local http://localhost:9000 admin password123 && mc rm -r --force local/lakehouse/checkpoints || true && mc mb -p local/lakehouse/checkpoints"
Levantar Spark:
kubectl scale deployment spark-streaming -n flight-stack --replicas=1
Esperar: 2-4 minutos
Comprobar Spark:
kubectl get pods -n flight-stack
Spark debe aparecer:
spark-streaming    1/1 Running
Comprobar logs:
kubectl logs -n flight-stack deployment/spark-streaming --tail 150
Debe aparecer:
Streaming arrancado. Esperando mensajes en Kafka...
o:
Batch vacío, esperando mensajes...

2.9. Probar Kubernetes local
Abrir port-forward de Flask:
kubectl port-forward -n flight-stack svc/flask 5002:5001
Dejar esa terminal abierta.
Abrir en el navegador:
http://127.0.0.1:5002/flights/delays/predict_kafka
Rellenar el formulario de predicción y pulsar Submit, igual, en el botón naranja.
Comprobar Cassandra:
kubectl exec -it -n flight-stack deployment/cassandra -- cqlsh -e "SELECT uuid, origin, dest, carrier, route, distance, prediction, timestamp FROM agile_data_science.flight_delay_ml_response;"
Debe aparecer al menos una predicción.
3. Docker en Google Cloud
VM:
ibdn-docker-vm
Web final:
http://EXTERNAL_IP_VM:5001/flights/delays/predict_kafka

3.1. Arrancar VM
Desde terminal local:
gcloud compute instances start ibdn-docker-vm --zone=europe-west1-b
Esperar: 1-2 minutos
Comprobar VM:
gcloud compute instances list
Debe aparecer:
ibdn-docker-vm    RUNNING
Apuntar la IP pública de la columna:
EXTERNAL_IP

3.2. Entrar en la VM
gcloud compute ssh ibdn-docker-vm --zone=europe-west1-b
Dentro de la VM:
cd ~/practica_creativa

3.3. Levantar Docker en la VM
docker compose up -d
Esperar: 2-5 minutos
Comprobar:
docker compose ps
Deben aparecer Up:
cassandra-flight-docker
flight-flask
flight-kafka
flight-spark-streaming
flight-zookeeper
minio-lakehouse-docker
Si Kafka o Spark no aparecen como Up, ejecutar:
docker compose up -d
Esperar 1 minuto y volver a comprobar:
docker compose ps

3.4. Preparar Cassandra en la VM
docker exec -it cassandra-flight-docker cqlsh -e "CREATE KEYSPACE IF NOT EXISTS agile_data_science WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
docker exec -it cassandra-flight-docker cqlsh -e "CREATE TABLE IF NOT EXISTS agile_data_science.flight_delay_ml_response (uuid text PRIMARY KEY, origin text, dest text, carrier text, route text, distance double, prediction text, timestamp text, day_of_month int, day_of_week int, day_of_year int, dep_delay double, flight_date text);"
docker exec -it cassandra-flight-docker cqlsh -e "CREATE TABLE IF NOT EXISTS agile_data_science.flight_distances (origin text, dest text, distance double, PRIMARY KEY (origin, dest));"
docker exec -it cassandra-flight-docker cqlsh -e "INSERT INTO agile_data_science.flight_distances (origin, dest, distance) VALUES ('ATL', 'SFO', 2139);"
docker exec -it cassandra-flight-docker cqlsh -e "INSERT INTO agile_data_science.flight_distances (origin, dest, distance) VALUES ('SFO', 'ATL', 2139);"
docker exec -it cassandra-flight-docker cqlsh -e "INSERT INTO agile_data_science.flight_distances (origin, dest, distance) VALUES ('JFK', 'LAX', 2475);"
docker exec -it cassandra-flight-docker cqlsh -e "INSERT INTO agile_data_science.flight_distances (origin, dest, distance) VALUES ('LAX', 'JFK', 2475);"
Comprobar tablas:
docker exec -it cassandra-flight-docker cqlsh -k agile_data_science -e "DESCRIBE TABLES;"

3.5. Preparar MinIO y Kafka en la VM
docker run --rm --network practica_creativa_default --entrypoint /bin/sh minio/mc -c "mc alias set local http://minio:9000 admin password123 && mc mb -p local/lakehouse && mc mb -p local/lakehouse/models && mc mb -p local/lakehouse/checkpoints"
docker run --rm --network practica_creativa_default -v "$(pwd)/models:/models" --entrypoint /bin/sh minio/mc -c "mc alias set local http://minio:9000 admin password123 && mc cp -r /models/* local/lakehouse/models/"
Asegurar Kafka:
docker compose up -d zookeeper kafka
Esperar: 1 minuto
Crear topics:
docker exec -it flight-kafka kafka-topics --bootstrap-server flight-kafka:29092 --create --if-not-exists --topic flight-delay-ml-request --partitions 1 --replication-factor 1
docker exec -it flight-kafka kafka-topics --bootstrap-server flight-kafka:29092 --create --if-not-exists --topic flight-delay-ml-response --partitions 1 --replication-factor 1
Comprobar:
docker exec -it flight-kafka kafka-topics --bootstrap-server flight-kafka:29092 --list
Debe aparecer:
flight-delay-ml-request
flight-delay-ml-response

3.6. Limpiar checkpoints y reiniciar Spark/Flask en la VM
docker run --rm --network practica_creativa_default --entrypoint /bin/sh minio/mc -c "mc alias set local http://minio:9000 admin password123 && mc rm -r --force local/lakehouse/checkpoints || true && mc mb -p local/lakehouse/checkpoints"
docker compose restart spark flask
Esperar: 1-2 minutos
Comprobar Spark:
docker compose logs spark --tail 150
Debe aparecer:
Streaming arrancado. Esperando mensajes en Kafka...

3.7. Probar Docker en Google Cloud
Abrir en el navegador:
http://EXTERNAL_IP_VM:5001/flights/delays/predict_kafka
Rellenar el formulario de predicción y pulsar Submit, igual que con los otros.
Comprobar Cassandra:
docker exec -it cassandra-flight-docker cqlsh -e "SELECT uuid, origin, dest, carrier, route, distance, prediction, timestamp FROM agile_data_science.flight_delay_ml_response;"
Salir de la VM:
exit
4. Kubernetes en Google Cloud
Web final:
http://EXTERNAL_IP_LOADBALANCER:5001/flights/delays/predict_kafka
Todos los comandos de este apartado se ejecutan desde terminal local, dentro de practica_creativa

4.1. Conectar con GKE
gcloud container clusters get-credentials ibdn-flight-cluster --zone europe-west1-b --project ibdn-flight-stack-alvaro
Comprobar contexto:
kubectl config current-context
Debe salir:
gke_ibdn-flight-stack-alvaro_europe-west1-b_ibdn-flight-cluster
Comprobar nodos:
kubectl get nodes
Deben aparecer nodos gke-...
4.2. Definir imágenes
PROJECT_ID=ibdn-flight-stack-alvaro
REGION=europe-west1
REPO=ibdn-docker
TAG=visual-v1

4.3. Aplicar Kubernetes en GKE
kubectl apply -f ./k8s/01-infra.yml
kubectl apply -f ./k8s/02-app.yml
Parar Flask y Spark inmediatamente para preparar primero Cassandra, MinIO y Kafka:
kubectl scale deployment flask -n flight-stack --replicas=0
kubectl scale deployment spark-streaming -n flight-stack --replicas=0
Actualizar imágenes:
kubectl set image deployment/flask flask=$REGION-docker.pkg.dev/$PROJECT_ID/$REPO/practica_creativa-flask:$TAG -n flight-stack
kubectl set image deployment/spark-streaming spark-streaming=$REGION-docker.pkg.dev/$PROJECT_ID/$REPO/practica_creativa-spark:$TAG -n flight-stack
Esperar: 1-2 minutos
Comprobar pods:
kubectl get pods -n flight-stack
Deben aparecer:
cassandra
kafka
minio
zookeeper

4.4. Estabilizar Kafka en GKE
Parar Kafka:
kubectl scale deployment kafka -n flight-stack --replicas=0
Aplicar ajuste:
kubectl patch deployment kafka -n flight-stack --type strategic -p '{"spec":{"template":{"spec":{"enableServiceLinks":false}}}}'
Levantar Kafka:
kubectl scale deployment kafka -n flight-stack --replicas=1
Esperar: 1 minuto
Comprobar:
kubectl get pods -n flight-stack
Kafka debe aparecer:
1/1 Running

4.5. Preparar Cassandra en GKE
Crear keyspace y tablas:
kubectl exec -it -n flight-stack deployment/cassandra -- cqlsh -e "CREATE KEYSPACE IF NOT EXISTS agile_data_science WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
kubectl exec -it -n flight-stack deployment/cassandra -- cqlsh -e "CREATE TABLE IF NOT EXISTS agile_data_science.flight_delay_ml_response (uuid text PRIMARY KEY, origin text, dest text, carrier text, route text, distance double, prediction text, timestamp text, day_of_month int, day_of_week int, day_of_year int, dep_delay double, flight_date text);"
kubectl exec -it -n flight-stack deployment/cassandra -- cqlsh -e "CREATE TABLE IF NOT EXISTS agile_data_science.flight_distances (origin text, dest text, distance double, PRIMARY KEY (origin, dest));"
Insertar distancias:
kubectl exec -it -n flight-stack deployment/cassandra -- cqlsh -e "INSERT INTO agile_data_science.flight_distances (origin, dest, distance) VALUES ('ATL', 'SFO', 2139);"
kubectl exec -it -n flight-stack deployment/cassandra -- cqlsh -e "INSERT INTO agile_data_science.flight_distances (origin, dest, distance) VALUES ('SFO', 'ATL', 2139);"
kubectl exec -it -n flight-stack deployment/cassandra -- cqlsh -e "INSERT INTO agile_data_science.flight_distances (origin, dest, distance) VALUES ('JFK', 'LAX', 2475);"
kubectl exec -it -n flight-stack deployment/cassandra -- cqlsh -e "INSERT INTO agile_data_science.flight_distances (origin, dest, distance) VALUES ('LAX', 'JFK', 2475);"
Comprobar tablas:
kubectl exec -it -n flight-stack deployment/cassandra -- cqlsh -k agile_data_science -e "DESCRIBE TABLES;"
Levantar Flask:
kubectl scale deployment flask -n flight-stack --replicas=1
Esperar: 30-60 segundos

4.6. Preparar MinIO en GKE
Crear bucket y carpetas:
kubectl exec -it -n flight-stack deployment/minio -- sh -c "mc alias set local http://localhost:9000 admin password123 && mc mb -p local/lakehouse && mc mb -p local/lakehouse/models && mc mb -p local/lakehouse/checkpoints"
Abrir otra terminal.
Si se usa Windows, entrar en WSL:
wsl
Entrar de nuevo en la carpeta del proyecto:
cd practica_creativa
Abrir port-forward de MinIO y dejar esta terminal abierta:
kubectl port-forward -n flight-stack svc/minio 9001:9000
Abrir otra terminal.
Si se usa Windows, entrar en WSL:
wsl
Entrar de nuevo en la carpeta del proyecto:
cd practica_creativa
Subir modelos:
rm -f mc
curl -L https://dl.min.io/client/mc/release/linux-amd64/mc -o mc
chmod +x mc
./mc alias set local http://127.0.0.1:9001 admin password123
./mc rm -r --force local/lakehouse/models
./mc mb -p local/lakehouse/models
./mc cp -r models/* local/lakehouse/models/
./mc ls local/lakehouse/models/
Cerrar port-forward de MinIO.
4.7. Preparar Kafka topics en GKE
Obtener pod exacto:
KAFKA_POD=$(kubectl get pod -n flight-stack -l app=kafka -o jsonpath='{.items[0].metadata.name}')
echo $KAFKA_POD
Crear topics:
kubectl exec -it -n flight-stack $KAFKA_POD -c kafka -- kafka-topics --bootstrap-server localhost:29092 --create --if-not-exists --topic flight-delay-ml-request --partitions 1 --replication-factor 1
kubectl exec -it -n flight-stack $KAFKA_POD -c kafka -- kafka-topics --bootstrap-server localhost:29092 --create --if-not-exists --topic flight-delay-ml-response --partitions 1 --replication-factor 1
Comprobar topics:
kubectl exec -it -n flight-stack $KAFKA_POD -c kafka -- kafka-topics --bootstrap-server localhost:29092 --list
Debe aparecer:
flight-delay-ml-request
flight-delay-ml-response

4.8. Limpiar checkpoints y levantar Spark en GKE
Limpiar checkpoints:
kubectl exec -it -n flight-stack deployment/minio -- sh -c "mc alias set local http://localhost:9000 admin password123 && mc rm -r --force local/lakehouse/checkpoints || true && mc mb -p local/lakehouse/checkpoints"
Levantar Spark:
kubectl scale deployment spark-streaming -n flight-stack --replicas=1
Esperar: 2-4 minutos
Comprobar Spark:
kubectl logs -n flight-stack deployment/spark-streaming --tail 150
Debe aparecer:
Streaming arrancado. Esperando mensajes en Kafka...

4.9. Exponer web GKE
kubectl patch svc flask -n flight-stack -p '{"spec":{"type":"LoadBalancer"}}'
Comprobar IP pública:
kubectl get svc flask -n flight-stack
Al principio puede aparecer:
EXTERNAL-IP   <pending>
Esperar: 1-5 minutos
Volver a comprobar:
kubectl get svc flask -n flight-stack
Cuando aparezca una IP pública, abrir en navegador:
http://EXTERNAL_IP_LOADBALANCER:5001/flights/delays/predict_kafka
Rellenar el formulario de predicción y pulsar Submit, en el botón naranja.
Comprobar Cassandra:
kubectl exec -it -n flight-stack deployment/cassandra -- cqlsh -e "SELECT uuid, origin, dest, carrier, route, distance, prediction, timestamp FROM agile_data_science.flight_delay_ml_response;"
Debe aparecer al menos una predicción.


Si quiere ver las anteriores predicciones puede pulsar el botón ‘Ver anteriores predicciones’ y se ven.
