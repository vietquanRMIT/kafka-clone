package com.example.kafka.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Loads static cluster metadata from {@code cluster.json} so both the client and server share the
 * exact same routing knowledge (topics, partitions, broker ownership, etc.).
 * Basically acts as a simple in-memory "controller". Since there are a lot of different use cases
 * (e.g., client-side routing, server-side partition assignment), there are several different variables or states
 * to accommodate them all.
 */
public final class ClusterMetadata {

    private static final Logger logger = LoggerFactory.getLogger(ClusterMetadata.class);

//  Maps of brokers and topic routes (id -> host/port) -> "I have a broker ID 'broker-1'. What is its IP address and Port?"
    private static final Map<String, BrokerNode> brokers;

//  topic -> partition -> brokerId -> I want to write to topic "my-topic", partition 0. Which broker is the leader?
    private static final Map<String, Map<Integer, String>> topicRoutes;

//  Usage: Used by the Server on startup to create the right folders (brokerId -> topic -> partitions) -> "I am broker 'broker-1'. Which topics and partitions do I own? (used on server startup)"
    private static final Map<String, Map<String, List<Integer>>> brokerAssignments;

//  Usage: Used by Producer when the user doesn't specify a partition. -> "How many partitions does topic 'my-topic' have?"
    private static final Map<String, Integer> partitionCounts;

    private ClusterMetadata() {
    }


    static {
        Map<String, BrokerNode> brokerMap = new HashMap<>();

        //  topic -> partition -> brokerId
        Map<String, Map<Integer, String>> routes = new HashMap<>();
        Map<String, Map<String, List<Integer>>> assignments = new HashMap<>();

        try (InputStream inputStream = ClusterMetadata.class.getClassLoader().getResourceAsStream("cluster.json")) {
            if (inputStream == null) {
                throw new IllegalStateException("cluster.json not found on the classpath");
            }

            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(inputStream);
            if (root == null || root.isEmpty()) {
                throw new IllegalStateException("cluster.json is empty or malformed");
            }

            JsonNode brokersNode = root.path("brokers");
            if (!brokersNode.isArray()) {
                throw new IllegalStateException("cluster.json missing brokers array");
            }

            for (JsonNode brokerNode : brokersNode) {
                String id = brokerNode.path("id").asText(null);
                String host = brokerNode.path("host").asText(null);
                int port = brokerNode.path("port").asInt(-1);
                if (id == null || host == null || port < 0) {
                    throw new IllegalStateException("Invalid broker entry in cluster.json");
                }
                brokerMap.put(id, new BrokerNode(id, host, port));
            }

            JsonNode routesNode = root.path("routes");
            if (!routesNode.isObject()) {
                throw new IllegalStateException("cluster.json missing routes object");
            }

            routesNode.fields().forEachRemaining(topicEntry -> {
                String topic = topicEntry.getKey();
                JsonNode partitionsNode = topicEntry.getValue();
                if (!partitionsNode.isObject()) {
                    logger.warn("Topic {} has invalid partition mapping", topic);
                    return;
                }

                Map<Integer, String> partitionLeaders = new ConcurrentHashMap<>();
                partitionsNode.fields().forEachRemaining(partitionEntry -> {
                    try {
                        int partitionId = Integer.parseInt(partitionEntry.getKey());
                        String brokerId = partitionEntry.getValue().asText(null);
                        if (brokerId == null) {
                            logger.warn("Partition {} for topic {} does not map to a broker", partitionEntry.getKey(), topic);
                            return;
                        }
                        partitionLeaders.put(partitionId, brokerId);
                        assignments
                                .computeIfAbsent(brokerId, ignored -> new HashMap<>())
                                .computeIfAbsent(topic, ignored -> new ArrayList<>())
                                .add(partitionId);
                    } catch (NumberFormatException ex) {
                        logger.warn("Skipping non-numeric partition id {} for topic {}", partitionEntry.getKey(), topic);
                    }
                });

                routes.put(topic, partitionLeaders);
            });

        } catch (IOException e) {
            throw new ExceptionInInitializerError("Failed to load cluster metadata: " + e.getMessage());
        }

        brokers = Map.copyOf(brokerMap);
        topicRoutes = toUnmodifiableNestedMap(routes);
        brokerAssignments = buildImmutableAssignments(assignments);
        partitionCounts = Map.copyOf(partitionCounts(routes));

        logger.info("Loaded {} brokers and {} topic routes from cluster.json", brokers.size(), topicRoutes.size());
    }

    private static Map<String, Integer> partitionCounts(Map<String, Map<Integer, String>> routes) {
        Map<String, Integer> counts = new HashMap<>();
        routes.forEach((topic, partitions) -> counts.put(topic, partitions.size()));
        return counts;
    }

    private static Map<String, Map<Integer, String>> toUnmodifiableNestedMap(Map<String, Map<Integer, String>> routes) {
        Map<String, Map<Integer, String>> snapshot = new HashMap<>();
        routes.forEach((topic, partitions) -> snapshot.put(topic, Map.copyOf(partitions)));
        return Map.copyOf(snapshot);
    }

    private static Map<String, Map<String, List<Integer>>> buildImmutableAssignments(Map<String, Map<String, List<Integer>>> assignments) {
        Map<String, Map<String, List<Integer>>> snapshot = new HashMap<>();
        assignments.forEach((brokerId, topics) -> {
            Map<String, List<Integer>> topicSnapshot = new HashMap<>();
            topics.forEach((topic, partitions) -> {
                partitions.sort(Integer::compareTo);
                topicSnapshot.put(topic, List.copyOf(partitions));
            });
            snapshot.put(brokerId, Map.copyOf(topicSnapshot));
        });
        return Map.copyOf(snapshot);
    }

    public static BrokerNode getLeader(String topic, int partition) {
        Map<Integer, String> partitions = topicRoutes.get(topic);
        if (partitions == null) {
            return null;
        }
        String brokerId = partitions.get(partition);
        return brokerId == null ? null : brokers.get(brokerId);
    }

    public static int getPartitionCount(String topic) {
        return partitionCounts.getOrDefault(topic, 0);
    }

    public static Map<Integer, String> getPartitionAssignments(String topic) {
        Map<Integer, String> partitions = topicRoutes.get(topic);
        return partitions == null ? Map.of() : partitions;
    }

    public static Map<String, List<Integer>> getAssignmentsForBroker(String brokerId) {
        if (brokerId == null) {
            logger.warn("getAssignmentsForBroker called with null brokerId");
            return Map.of();
        }
        Map<String, List<Integer>> assignments = brokerAssignments.get(brokerId);
        return assignments == null ? Map.of() : assignments;
    }

    public static boolean isLeader(String brokerId, String topic, int partition) {
        if (brokerId == null) {
            return false;
        }
        Map<Integer, String> partitions = topicRoutes.get(topic);
        if (partitions == null) {
            return false;
        }
        String leaderId = partitions.get(partition);
        return brokerId.equals(leaderId);
    }

    public static Map<String, BrokerNode> getBrokers() {
        return brokers;
    }
}
