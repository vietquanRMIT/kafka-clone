package com.example.kafka.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Loads static cluster metadata from {@code cluster.json} so both the client and server share the
 * exact same routing knowledge (topics, partitions, broker ownership, etc.).
 */
public final class ClusterMetadata {

    private static final Logger logger = LoggerFactory.getLogger(ClusterMetadata.class);

    private static final Map<String, BrokerNode> brokers;
    private static final Map<String, Map<Integer, String>> topicRoutes;
    private static final Map<String, Map<String, List<Integer>>> brokerAssignments;
    private static final Map<String, Integer> partitionCounts;

    private ClusterMetadata() {
    }

    public record BrokerNode(String id, String host, int port) {
        public String addressKey() {
            return host + ":" + port;
        }
    }

    static {
        Map<String, BrokerNode> brokerMap = new HashMap<>();
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

        brokers = Collections.unmodifiableMap(brokerMap);
        topicRoutes = toUnmodifiableNestedMap(routes);
        brokerAssignments = buildImmutableAssignments(assignments);
        partitionCounts = Collections.unmodifiableMap(new HashMap<>(partitionCounts(routes)));

        logger.info("Loaded {} brokers and {} topic routes from cluster.json", brokers.size(), topicRoutes.size());
    }

    private static Map<String, Integer> partitionCounts(Map<String, Map<Integer, String>> routes) {
        Map<String, Integer> counts = new HashMap<>();
        routes.forEach((topic, partitions) -> counts.put(topic, partitions.size()));
        return counts;
    }

    private static Map<String, Map<Integer, String>> toUnmodifiableNestedMap(Map<String, Map<Integer, String>> routes) {
        Map<String, Map<Integer, String>> snapshot = new HashMap<>();
        routes.forEach((topic, partitions) -> snapshot.put(topic, Collections.unmodifiableMap(new HashMap<>(partitions))));
        return Collections.unmodifiableMap(snapshot);
    }

    private static Map<String, Map<String, List<Integer>>> buildImmutableAssignments(Map<String, Map<String, List<Integer>>> assignments) {
        Map<String, Map<String, List<Integer>>> snapshot = new HashMap<>();
        assignments.forEach((brokerId, topics) -> {
            Map<String, List<Integer>> topicSnapshot = new HashMap<>();
            topics.forEach((topic, partitions) -> {
                partitions.sort(Integer::compareTo);
                topicSnapshot.put(topic, Collections.unmodifiableList(new ArrayList<>(partitions)));
            });
            snapshot.put(brokerId, Collections.unmodifiableMap(topicSnapshot));
        });
        return Collections.unmodifiableMap(snapshot);
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
        Map<String, List<Integer>> assignments = brokerAssignments.get(brokerId);
        return assignments == null ? Map.of() : assignments;
    }

    public static boolean isLeader(String brokerId, String topic, int partition) {
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
