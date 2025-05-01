package org.example;

import model.EnrichedTeamUser;
import model.TeamUser;
import model.UserPrice;
import serde.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class TeamPricingEnrichmentTest {
    public static void main(String[] args) {
        // Set up properties for the test driver
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "team-pricing-enrichment-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        // Create topology
        StreamsBuilder builder = new StreamsBuilder();
        createEnrichmentTopology(builder);

        // Create serdes for testing
        JsonSerde<TeamUser> teamUserSerde = new JsonSerde<>(TeamUser.class);
        JsonSerde<UserPrice> userPriceSerde = new JsonSerde<>(UserPrice.class);
        JsonSerde<EnrichedTeamUser> enrichedTeamUserSerde = new JsonSerde<>(EnrichedTeamUser.class);

        // Create test driver
        TopologyTestDriver testDriver = new TopologyTestDriver(builder.build(), props);

        // Create test topics
        TestInputTopic<Long, UserPrice> userPricesTopic = testDriver.createInputTopic(
                "User_Prices",
                Serdes.Long().serializer(),
                userPriceSerde.serializer()
        );

        TestInputTopic<Long, TeamUser> teamUserTopic = testDriver.createInputTopic(
                "Team_User",
                Serdes.Long().serializer(),
                teamUserSerde.serializer()
        );

        TestOutputTopic<Long, EnrichedTeamUser> outputTopic = testDriver.createOutputTopic(
                "Team_Pricing_Enriched",
                Serdes.Long().deserializer(),
                enrichedTeamUserSerde.deserializer()
        );

        // Scenario 1: Pricing data arrives first (at 8am)
        System.out.println("Testing scenario: Pricing data arrives first (at 8am)");
        userPricesTopic.pipeInput(123L, new UserPrice("ACTIVE", 1000L));

        // Scenario 1: Then team data arrives (at 9am)
        teamUserTopic.pipeInput(456L, new TeamUser(456L, 123L, "FastCourier"));

        // Check results
        System.out.println("Output records:");
        outputTopic.readRecordsToList().forEach(record -> {
            System.out.println("Team ID: " + record.key() +
                    ", User ID: " + record.value().getUserId() +
                    ", Status: " + record.value().getStatus() +
                    ", Price: " + record.value().getPrice());
        });

        // Scenario 2: What if team data arrives but no matching pricing exists?
        System.out.println("\nTesting scenario: Team data without matching pricing");
        teamUserTopic.pipeInput(789L, new TeamUser(789L, 999L, "SlowCourier"));

        // Check results again
        System.out.println("Output records:");
        outputTopic.readRecordsToList().forEach(record -> {
            System.out.println("Team ID: " + record.key() +
                    ", User ID: " + record.value().getUserId() +
                    ", Status: " + record.value().getStatus() +
                    ", Price: " + record.value().getPrice());
        });

        // Close the test driver
        testDriver.close();
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