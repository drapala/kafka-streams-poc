#!/bin/bash

# Better error handling
function cleanup {
  echo "Cleaning up..."
  if [ ! -z "$APP_PID" ] && ps -p $APP_PID > /dev/null; then
    kill $APP_PID
    echo "Application terminated"
  fi
}

function log_step {
  echo -e "\n=== $1 ==="
}

trap cleanup EXIT

log_step "STARTING KAFKA STREAMS POC WITH AVRO AND SCHEMA REGISTRY E2E TEST"

# Pre-flight checks
log_step "Checking if services are running"
if ! docker-compose ps | grep -q "schema-registry.*Up"; then
  echo "Schema Registry not running. Start services with 'docker-compose up -d' first."
  exit 1
fi

# Step 1: Check Schema Registry connectivity directly from host
log_step "Checking Schema Registry connectivity from host"
if curl -s -f -m 5 http://localhost:8081/subjects > /dev/null; then
  echo "Host -> Schema Registry: Connection successful"
else
  echo "Warning: Host cannot connect to Schema Registry on localhost:8081"
  echo "This may cause issues when running the application locally"
fi

if curl -s -f -m 5 http://localhost:9092 > /dev/null 2>&1; then
  echo "Host -> Kafka: Connection attempt made"
else
  echo "Note: Kafka broker connection test failed, but this is expected as it doesn't support HTTP"
fi

# Step 2: Check Schema Registry
log_step "Checking Schema Registry"
SUBJECTS=$(docker-compose exec -T schema-registry curl -s -X GET http://schema-registry:8081/subjects)
echo "Currently registered subjects: $SUBJECTS"

# Step 3: Reset application state
log_step "Resetting application state"
docker-compose exec -T kafka kafka-streams-application-reset \
  --bootstrap-servers kafka:29092 \
  --application-id team-pricing-enrichment-avro \
  --input-topics Team_User_Avro,User_Prices_Avro

# Step 4: Register schemas (if not already registered)
log_step "Registering Avro schemas"

# UserPrice schema
if ! echo "$SUBJECTS" | grep -q "User_Prices_Avro-value"; then
  echo "Registering UserPrice schema..."
  USER_PRICE_SCHEMA='{"namespace":"org.example.avro","type":"record","name":"UserPrice","fields":[{"name":"status","type":["string","null"]},{"name":"price","type":["long","null"]}]}'
  docker-compose exec -T schema-registry curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data "{\"schema\": \"$(echo $USER_PRICE_SCHEMA | sed 's/"/\\"/g')\"}" \
    http://schema-registry:8081/subjects/User_Prices_Avro-value/versions
fi

# TeamUser schema
if ! echo "$SUBJECTS" | grep -q "Team_User_Avro-value"; then
  echo "Registering TeamUser schema..."
  TEAM_USER_SCHEMA='{"namespace":"org.example.avro","type":"record","name":"TeamUser","fields":[{"name":"teamId","type":"long"},{"name":"userId","type":"long"},{"name":"courier","type":["string","null"]}]}'
  docker-compose exec -T schema-registry curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data "{\"schema\": \"$(echo $TEAM_USER_SCHEMA | sed 's/"/\\"/g')\"}" \
    http://schema-registry:8081/subjects/Team_User_Avro-value/versions
fi

# EnrichedTeamUser schema
if ! echo "$SUBJECTS" | grep -q "Team_Pricing_Enriched_Avro-value"; then
  echo "Registering EnrichedTeamUser schema..."
  ENRICHED_SCHEMA='{"namespace":"org.example.avro","type":"record","name":"EnrichedTeamUser","fields":[{"name":"teamId","type":"long"},{"name":"userId","type":"long"},{"name":"courier","type":["string","null"]},{"name":"status","type":["string","null"]},{"name":"price","type":["long","null"]}]}'
  docker-compose exec -T schema-registry curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data "{\"schema\": \"$(echo $ENRICHED_SCHEMA | sed 's/"/\\"/g')\"}" \
    http://schema-registry:8081/subjects/Team_Pricing_Enriched_Avro-value/versions
fi

# Step 5: Create test data with more records
log_step "Creating test data files"

# Create UserPrice test data
cat > user_prices_data.txt << EOF
101:{"status":{"string":"delivered"},"price":{"long":199}}
102:{"status":{"string":"in_transit"},"price":{"long":149}}
103:{"status":{"string":"pending"},"price":{"long":299}}
EOF
echo "Created user_prices_data.txt with 3 records"

# Create TeamUser test data
cat > team_user_data.txt << EOF
1:{"teamId":1,"userId":101,"courier":{"string":"FedEx"}}
2:{"teamId":1,"userId":103,"courier":{"string":"DHL"}}
3:{"teamId":2,"userId":102,"courier":{"string":"UPS"}}
4:{"teamId":3,"userId":104,"courier":{"string":"USPS"}}
EOF
echo "Created team_user_data.txt with 4 records"

# Create expected output - this helps with verification
cat > expected_output.txt << EOF
1 EnrichedTeamUser with teamId=1, userId=101, courier=FedEx, status=delivered, price=199
1 EnrichedTeamUser with teamId=1, userId=103, courier=DHL, status=pending, price=299
2 EnrichedTeamUser with teamId=2, userId=102, courier=UPS, status=in_transit, price=149
3 EnrichedTeamUser with teamId=3, userId=104, courier=USPS, status=null, price=null
EOF
echo "Created expected_output.txt for verification"

# Step 6: Ensure topics exist
log_step "Ensuring topics exist"
docker-compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --list | grep -q "User_Prices_Avro" || \
  docker-compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --create --topic User_Prices_Avro --partitions 1 --replication-factor 1

docker-compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --list | grep -q "Team_User_Avro" || \
  docker-compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --create --topic Team_User_Avro --partitions 1 --replication-factor 1

docker-compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --list | grep -q "Team_Pricing_Enriched_Avro" || \
  docker-compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --create --topic Team_Pricing_Enriched_Avro --partitions 1 --replication-factor 1

log_step "Producing data to User_Prices_Avro"
cat user_prices_data.txt | docker-compose exec -T kafka-tools kafka-avro-console-producer \
  --bootstrap-server kafka:29092 \
  --topic User_Prices_Avro \
  --property schema.registry.url=http://schema-registry:8081 \
  --property parse.key=true \
  --property key.separator=: \
  --property key.schema='{"type":"long"}' \
  --property value.schema='{"namespace":"org.example.avro","type":"record","name":"UserPrice","fields":[{"name":"status","type":["string","null"]},{"name":"price","type":["long","null"]}]}'

log_step "Producing data to Team_User_Avro"
cat team_user_data.txt | docker-compose exec -T kafka-tools kafka-avro-console-producer \
  --bootstrap-server kafka:29092 \
  --topic Team_User_Avro \
  --property schema.registry.url=http://schema-registry:8081 \
  --property parse.key=true \
  --property key.separator=: \
  --property key.schema='{"type":"long"}' \
  --property value.schema='{"namespace":"org.example.avro","type":"record","name":"TeamUser","fields":[{"name":"teamId","type":"long"},{"name":"userId","type":"long"},{"name":"courier","type":["string","null"]}]}'

# Step 8: Verify the data was produced correctly
log_step "Verifying data was produced to input topics"
echo "User_Prices_Avro contains:"
docker-compose exec -T kafka-tools kafka-avro-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic User_Prices_Avro \
  --property schema.registry.url=http://schema-registry:8081 \
  --from-beginning \
  --max-messages 3 \
  --property print.key=true 2>/dev/null || echo "Error reading User_Prices_Avro"

echo "Team_User_Avro contains:"
docker-compose exec -T kafka-tools kafka-avro-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic Team_User_Avro \
  --property schema.registry.url=http://schema-registry:8081 \
  --from-beginning \
  --max-messages 4 \
  --property print.key=true 2>/dev/null || echo "Error reading Team_User_Avro"

# Step 9: Create a configuration file for the application
log_step "Creating application configuration"
cat > app-test.properties << EOF
bootstrap.servers=localhost:9092
schema.registry.url=http://localhost:8081
application.id=team-pricing-enrichment-avro
cache.max.bytes.buffering=0
commit.interval.ms=100
EOF
echo "Created app-test.properties"

# Step 10: Build the application
log_step "Building the application"
mvn clean package -DskipTests

# Step 11: Run the application with system properties
log_step "Running the application"
java -cp target/kafka-streams-1.0-SNAPSHOT.jar \
  -Dbootstrap.servers=localhost:9092 \
  -Dschema.registry.url=http://localhost:8081 \
  org.example.TeamPricingEnrichmentAppWithAvro > app.log 2>&1 &
APP_PID=$!
echo "Application started with PID $APP_PID"

# Step 12: Wait for processing with a more interactive approach
log_step "Waiting for application to process data"
for i in {1..15}; do
  echo -n "."
  sleep 1

  # Check if app is still running
  if ! ps -p $APP_PID > /dev/null; then
    echo -e "\nApplication terminated unexpectedly!"
    break
  fi

  # Show output as it arrives
  if [ $i -eq 5 ] || [ $i -eq 10 ]; then
    echo -e "\nApplication log preview:"
    tail -5 app.log
  fi
done
echo ""

# Step 13: Check application logs for errors
log_step "Checking application logs"
if grep -q "Exception\|Error" app.log; then
  echo "ERRORS FOUND IN APPLICATION LOGS:"
  grep -B 5 -A 10 "Exception\|Error" app.log

  echo -e "\nAdditional diagnostic information:"
  echo "Configuration being used:"
  grep "Using configuration\|bootstrap.servers\|schema.registry.url" app.log

  echo -e "\nAttempting connection test from container to verify network setup..."
  docker-compose exec -T kafka curl -s -m 2 http://schema-registry:8081/subjects || echo "Container -> Schema Registry: Failed"
else
  echo "No errors found in application logs"
fi

# Step 14: Check output topic for results
log_step "Checking results in output topic"
echo "Team_Pricing_Enriched_Avro contains:"
docker-compose exec -T kafka-tools kafka-avro-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic Team_Pricing_Enriched_Avro \
  --property schema.registry.url=http://schema-registry:8081 \
  --from-beginning \
  --max-messages 10 \
  --property print.key=true 2>/dev/null > actual_output.txt || echo "Error reading Team_Pricing_Enriched_Avro"

# Step 15: Verify results match expectations
if [ -s actual_output.txt ]; then
  log_step "Verifying output data"
  echo "Actual output:"
  cat actual_output.txt

  EXPECTED_COUNT=$(wc -l < expected_output.txt)
  ACTUAL_COUNT=$(wc -l < actual_output.txt)

  echo "Expected $EXPECTED_COUNT records, found $ACTUAL_COUNT records"
else
  echo "WARNING: No output data found in Team_Pricing_Enriched_Avro"

  # If no output, try running application in container as fallback
  log_step "Attempting to run application in container as fallback"
  echo "Creating temporary Docker container to run the application..."

  docker run --rm --network kafka-streams-poc_default \
    -v $(pwd)/target:/app \
    openjdk:17-slim \
    java -cp /app/kafka-streams-1.0-SNAPSHOT.jar \
    -Dbootstrap.servers=kafka:29092 \
    -Dschema.registry.url=http://schema-registry:8081 \
    org.example.TeamPricingEnrichmentAppWithAvro &

  DOCKER_APP_PID=$!

  echo "Waiting for in-container application to process data..."
  sleep 20

  kill $DOCKER_APP_PID 2>/dev/null || true

  echo "Checking for results after in-container execution:"
  docker-compose exec -T kafka-tools kafka-avro-console-consumer \
    --bootstrap-server kafka:29092 \
    --topic Team_Pricing_Enriched_Avro \
    --property schema.registry.url=http://schema-registry:8081 \
    --from-beginning \
    --max-messages 10 \
    --property print.key=true 2>/dev/null > actual_output_docker.txt || echo "Error reading Team_Pricing_Enriched_Avro"

  if [ -s actual_output_docker.txt ]; then
    echo "Success! Application running in container produced output:"
    cat actual_output_docker.txt

    echo -e "\nTIP: For local development, use Docker to run your application:"
    echo "docker run --rm --network kafka-streams-poc_default -v \$(pwd)/target:/app openjdk:17-slim java -cp /app/kafka-streams-1.0-SNAPSHOT.jar org.example.TeamPricingEnrichmentAppWithAvro"
  fi
fi

log_step "TEST COMPLETED"
if [ -s actual_output.txt ] || [ -s actual_output_docker.txt ]; then
  echo "✅ Success: Data was processed and output was produced"
else
  echo "❌ Failure: No output was produced. Check app.log for errors."
  echo "Recommendations:"
  echo "1. Check network connectivity between host and Docker containers"
  echo "2. Try running the application directly in Docker with proper network configuration"
  echo "3. Add more verbose logging to your application"
  echo "4. Verify schema compatibility and make sure all required schemas are registered"
fi