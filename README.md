# Kafka Streams POC

This repository contains a proof of concept implementation demonstrating the use of Kafka Streams for real-time data enrichment.

## Overview

This project showcases how to use Kafka Streams to process and enrich data in real-time. Specifically, it demonstrates a team pricing enrichment application that joins team user data with user pricing information.

The application performs a streaming join between:
- Team user data (containing teamId, userId, and courier information)
- User pricing data (containing status and price information)

The result is an enriched team user record that combines both data sources, enabling real-time pricing information to be attached to team user records.

## Project Structure

```
kafka-streams-poc/
├── src/
│   ├── main/java/
│   │   ├── model/
│   │   │   ├── EnrichedTeamUser.java     # Combined model with team and pricing data
│   │   │   ├── TeamUser.java             # Model for team user data
│   │   │   └── UserPrice.java            # Model for user pricing data
│   │   ├── org/example/
│   │   │   └── TeamPricingEnrichmentApp.java  # Main application class
│   │   ├── serde/
│   │   │   └── JsonSerde.java            # JSON serializer/deserializer
│   │   └── util/
│   │       └── TopologyVisualizer.java   # Utility for visualizing topology
│   └── test/java/org/example/
│       └── TeamPricingEnrichmentTest.java  # Test cases for the enrichment logic
├── .gitignore
└── pom.xml
```

## Features

- Real-time enrichment of team user data with pricing information
- Stream-table join pattern using Kafka Streams DSL
- Custom JSON serialization/deserialization for complex data types
- Error handling for malformed or incomplete data
- Re-keying operations to facilitate joins on different key types
- Topology visualization utilities for debugging and documentation
- Comprehensive test cases using the TopologyTestDriver

## Prerequisites

- Java 11 or higher
- Apache Maven
- Apache Kafka cluster (or local Kafka installation)
- Jackson library for JSON serialization/deserialization

## Getting Started

### Clone the repository

```bash
git clone https://github.com/drapala/kafka-streams-poc.git
cd kafka-streams-poc
```

### Build the project

```bash
mvn clean package
```

### Run the application

```bash
java -jar target/kafka-streams-poc-1.0-SNAPSHOT.jar
```

## Configuration

The application is configured in the `TeamPricingEnrichmentApp` class:

- **Application ID**: `team-pricing-enrichment`
- **Bootstrap Servers**: `localhost:9092`
- **Input Topics**:
  - `Team_User`: Source of team user data
  - `User_Prices`: Source of pricing data
- **Output Topic**:
  - `Team_Pricing_Enriched`: Destination for enriched records

Additional configuration options that can be enabled:

```java
// For exactly-once processing semantics
props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
```

## How It Works

The application creates a Kafka Streams topology with the following steps:

1. Reads `Team_User` records as a KStream
2. Re-keys the stream by userId (rather than teamId) to prepare for joining
3. Reads `User_Prices` as a KTable (optimized for lookups)
4. Performs a left join between team users and pricing data
5. Creates enriched records containing all information
6. Re-keys the enriched stream by teamId
7. Writes the results to the `Team_Pricing_Enriched` topic

### Data Flow Diagram

```
Team_User Topic    User_Prices Topic
     |                  |
     v                  v
 KStream            KTable
     |                  |
     |  Re-key by userId|
     v                  |
 KStream               /
     |                /
     |               /
     v              v
   Left Join Operation
           |
           v
   Enriched Stream
           |
           | Re-key by teamId
           v
  Team_Pricing_Enriched Topic
```

### Error Handling

The join operation includes error handling that:
- Catches exceptions during record enrichment
- Creates a valid output record even when pricing data is missing
- Marks records with errors to prevent downstream processing failures

## Usage Example

1. Start Kafka and create the required topics:
   ```bash
   kafka-topics --create --topic Team_User --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   kafka-topics --create --topic User_Prices --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   kafka-topics --create --topic Team_Pricing_Enriched --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   ```

2. Run the application:
   ```bash
   java -cp target/kafka-streams-poc-1.0-SNAPSHOT.jar org.example.TeamPricingEnrichmentApp
   ```

3. Produce sample data:
   ```bash
   # Add user pricing data
   kafka-console-producer --topic User_Prices --bootstrap-server localhost:9092
   {"userId": 123, "status": "ACTIVE", "price": 1000}
   
   # Add team user data
   kafka-console-producer --topic Team_User --bootstrap-server localhost:9092
   {"teamId": 456, "userId": 123, "courier": "FastCourier"}
   ```

4. Consume the enriched output:
   ```bash
   kafka-console-consumer --topic Team_Pricing_Enriched --bootstrap-server localhost:9092 --from-beginning
   ```

   Expected output:
   ```json
   {"teamId": 456, "userId": 123, "courier": "FastCourier", "status": "ACTIVE", "price": 1000}
   ```

## Testing

The project includes a test class `TeamPricingEnrichmentTest` that demonstrates how to use the `TopologyTestDriver` to test Kafka Streams applications without requiring a running Kafka cluster.

The test class covers the following scenarios:
1. When pricing data arrives before team user data (early enrichment)
2. When team user data has no matching pricing information (partial enrichment)

Run the tests using:

```bash
mvn test
```

Or run the test class directly:

```bash
java -cp target/kafka-streams-poc-1.0-SNAPSHOT-tests.jar org.example.TeamPricingEnrichmentTest
```

Sample test output:
```
Testing scenario: Pricing data arrives first (at 8am)
Output records:
Team ID: 456, User ID: 123, Status: ACTIVE, Price: 1000

Testing scenario: Team data without matching pricing
Output records:
Team ID: 789, User ID: 999, Status: null, Price: null
```

## Development

### Adding New Models

To add new models:

1. Create a new Java class in the `model` package with appropriate fields, getters, setters, and constructors
2. Ensure the model class has a default constructor for JSON deserialization
3. Use the existing `JsonSerde` for serialization/deserialization
4. Update the stream processing topology to incorporate the new model

### Extending the Application

To add more processing steps:

1. Modify the `createEnrichmentTopology()` method in `TeamPricingEnrichmentApp`
2. Add new transformations, filters, or join operations
3. Use the `TopologyVisualizer` to debug the updated topology
4. Write tests using `TopologyTestDriver` to verify your changes

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/your-feature`)
3. Commit your changes (`git commit -am 'Add your feature'`)
4. Push to the branch (`git push origin feature/your-feature`)
5. Create a new Pull Request

## License

[MIT License](LICENSE)

## Acknowledgments

- Apache Kafka and Kafka Streams
- All contributors to this project
