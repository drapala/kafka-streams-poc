package org.example;

import model.EnrichedTeamUser;
import model.TeamUser;
import model.UserPrice;
import serde.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class TeamPricingEnrichmentApp {

    public static void main(String[] args) {
        // 1. Configure our Streams application
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "team-pricing-enrichment");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        // Uncomment for exactly-once processing
        // props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        // 2. Create the StreamsBuilder and define the topology
        final StreamsBuilder builder = new StreamsBuilder();
        createEnrichmentTopology(builder);

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
            System.exit(1);
        }
        System.exit(0);
    }

    static void createEnrichmentTopology(StreamsBuilder builder) {
        // Configure serdes for our data types
        JsonSerde<TeamUser> teamUserSerde = new JsonSerde<>(TeamUser.class);
        JsonSerde<UserPrice> userPriceSerde = new JsonSerde<>(UserPrice.class);
        JsonSerde<EnrichedTeamUser> enrichedTeamUserSerde = new JsonSerde<>(EnrichedTeamUser.class);

        // 1. Read Team_User as a stream
        KStream<Long, TeamUser> teamUserStream = builder.stream(
                "Team_User",
                Consumed.with(Serdes.Long(), teamUserSerde)
        );

        // 2. Re-key by userId to enable joining
        KStream<Long, TeamUser> teamUserByUserId = teamUserStream
                .selectKey((teamId, teamUser) -> teamUser.getUserId());

        // 3. Read User_Prices as a table
        KTable<Long, UserPrice> userPriceTable = builder.table(
                "User_Prices",
                Consumed.with(Serdes.Long(), userPriceSerde)
        );

        // 4. Perform the left join
        KStream<Long, EnrichedTeamUser> enrichedStream = teamUserByUserId
                .leftJoin(
                        userPriceTable,
                        (teamUser, userPrice) -> {
                            try {
                                return new EnrichedTeamUser(
                                        teamUser.getTeamId(),
                                        teamUser.getUserId(),
                                        teamUser.getCourier(),
                                        userPrice != null ? userPrice.getStatus() : null,
                                        userPrice != null ? userPrice.getPrice() : null
                                );
                            } catch (Exception e) {
                                // Log error and return null or default object
                                System.err.println("Error enriching record: " + e.getMessage());
                                return new EnrichedTeamUser(
                                        teamUser.getTeamId(),
                                        teamUser.getUserId(),
                                        teamUser.getCourier(),
                                        "ERROR",
                                        null
                                );
                            }
                        }
                );

        // 5. Re-key by teamId for logical organization
        KStream<Long, EnrichedTeamUser> enrichedByTeamId = enrichedStream
                .selectKey((userId, enriched) -> enriched.getTeamId());

        // 6. Output to the result topic
        enrichedByTeamId.to(
                "Team_Pricing_Enriched",
                Produced.with(Serdes.Long(), enrichedTeamUserSerde)
        );
    }
}