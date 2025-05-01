package org.example;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.example.avro.EnrichedTeamUser;
import org.example.avro.TeamUser;
import org.example.avro.UserPrice;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class TeamPricingEnrichmentAppWithAvro {

    private static Properties loadConfig() {
        final Properties props = new Properties();

        // Default values
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "team-pricing-enrichment-avro");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:29092");
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schema-registry:8081");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

        // Override with system properties if provided
        String bootstrapServers = System.getProperty("bootstrap.servers");
        if (bootstrapServers != null && !bootstrapServers.isEmpty()) {
            System.out.println("Overriding bootstrap.servers with: " + bootstrapServers);
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        }

        String schemaRegistryUrl = System.getProperty("schema.registry.url");
        if (schemaRegistryUrl != null && !schemaRegistryUrl.isEmpty()) {
            System.out.println("Overriding schema.registry.url with: " + schemaRegistryUrl);
            props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        }

        // Optional: Load from config file if specified
        String configFile = System.getProperty("config.file");
        if (configFile != null && !configFile.isEmpty()) {
            try (InputStream input = new FileInputStream(configFile)) {
                Properties fileProps = new Properties();
                fileProps.load(input);
                props.putAll(fileProps);
                System.out.println("Loaded properties from file: " + configFile);
            } catch (IOException ex) {
                System.err.println("Warning: Could not load config file: " + configFile);
                ex.printStackTrace();
            }
        }

        return props;
    }

    public static void main(String[] args) {
        // 1. Configure our Streams application
        final Properties props = loadConfig();

        // Print the configuration being used
        System.out.println("Using configuration:");
        System.out.println("bootstrap.servers: " + props.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
        System.out.println("schema.registry.url: " + props.getProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG));

        // Verify Schema Registry connectivity
        try {
            URL url = new URL(props.getProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG));
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(5000);
            connection.connect();

            int responseCode = connection.getResponseCode();
            System.out.println("Schema Registry connection test: " + responseCode +
                    (responseCode == 200 ? " (OK)" : " (FAILED)"));

        } catch (Exception e) {
            System.err.println("Warning: Failed to connect to Schema Registry: " + e.getMessage());
        }

        // 2. Create the StreamsBuilder and define the topology
        final StreamsBuilder builder = new StreamsBuilder();
        createEnrichmentTopology(builder, props.getProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG));

        // 3. Build the topology
        final Topology topology = builder.build();
        System.out.println(topology.describe());

        // 4. Create the KafkaStreams instance and start it
        final KafkaStreams streams = new KafkaStreams(topology, props);

        // 5. Set up shutdown hook for graceful termination
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.err.println("Error starting Kafka Streams: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }

    static void createEnrichmentTopology(StreamsBuilder builder, String schemaRegistryUrl) {
        // Configure Avro serdes for our data types
        Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                schemaRegistryUrl);

        SpecificAvroSerde<TeamUser> teamUserSerde = new SpecificAvroSerde<>();
        teamUserSerde.configure(serdeConfig, false); // false for value serdes

        SpecificAvroSerde<UserPrice> userPriceSerde = new SpecificAvroSerde<>();
        userPriceSerde.configure(serdeConfig, false);

        SpecificAvroSerde<EnrichedTeamUser> enrichedTeamUserSerde = new SpecificAvroSerde<>();
        enrichedTeamUserSerde.configure(serdeConfig, false);

        // 1. Read Team_User as a stream
        KStream<Long, TeamUser> teamUserStream = builder.stream(
                "Team_User_Avro",
                Consumed.with(Serdes.Long(), teamUserSerde)
        );

        // Add debugging for original TeamUser stream
        teamUserStream.peek((key, value) ->
                        System.out.println("Original TeamUser: key=" + key + ", value=" + value))
                .foreach((k, v) -> {});

        // 2. Re-key by userId to enable joining
        KStream<Long, TeamUser> teamUserByUserId = teamUserStream
                .selectKey((teamId, teamUser) -> teamUser.getUserId());

        // Add debugging for re-keyed TeamUser stream
        teamUserByUserId.peek((key, value) ->
                        System.out.println("Rekeyed TeamUser: key=" + key + ", value=" + value))
                .foreach((k, v) -> {});

        // 3. Read User_Prices as a table
        KTable<Long, UserPrice> userPriceTable = builder.table(
                "User_Prices_Avro",
                Consumed.with(Serdes.Long(), userPriceSerde)
        );

        // Add debugging for UserPrice table
        userPriceTable.toStream().peek((key, value) ->
                        System.out.println("UserPrice: key=" + key + ", value=" + value))
                .foreach((k, v) -> {});

        // 4. Perform the left join
        KStream<Long, EnrichedTeamUser> enrichedStream = teamUserByUserId
                .leftJoin(
                        userPriceTable,
                        (teamUser, userPrice) -> {
                            try {
                                EnrichedTeamUser enriched = new EnrichedTeamUser();
                                enriched.setTeamId(teamUser.getTeamId());
                                enriched.setUserId(teamUser.getUserId());
                                enriched.setCourier(teamUser.getCourier());

                                if (userPrice != null) {
                                    enriched.setStatus(userPrice.getStatus());
                                    enriched.setPrice(userPrice.getPrice());
                                } else {
                                    enriched.setStatus(null);
                                    enriched.setPrice(null);
                                }

                                return enriched;
                            } catch (Exception e) {
                                // Log error and return a default object
                                System.err.println("Error enriching record: " + e.getMessage());
                                e.printStackTrace();

                                EnrichedTeamUser errorRecord = new EnrichedTeamUser();
                                errorRecord.setTeamId(teamUser.getTeamId());
                                errorRecord.setUserId(teamUser.getUserId());
                                errorRecord.setCourier(teamUser.getCourier());
                                errorRecord.setStatus("ERROR");
                                errorRecord.setPrice(null);

                                return errorRecord;
                            }
                        }
                );

        // Add debugging for enriched stream
        enrichedStream.peek((key, value) ->
                        System.out.println("Enriched: key=" + key + ", value=" + value))
                .foreach((k, v) -> {});

        // 5. Re-key by teamId for logical organization
        KStream<Long, EnrichedTeamUser> enrichedByTeamId = enrichedStream
                .selectKey((userId, enriched) -> enriched.getTeamId());

        // Add debugging for final re-keyed stream
        enrichedByTeamId.peek((key, value) ->
                        System.out.println("Final output: key=" + key + ", value=" + value))
                .foreach((k, v) -> {});

        // 6. Output to the result topic
        enrichedByTeamId.to(
                "Team_Pricing_Enriched_Avro",
                Produced.with(Serdes.Long(), enrichedTeamUserSerde)
        );
    }
}