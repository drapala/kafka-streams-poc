package util;

import org.apache.kafka.streams.Topology;

public class TopologyVisualizer {

    /**
     * Print a formatted description of the stream topology
     */
    public static void printTopology(Topology topology) {
        System.out.println("============= Topology Description =============");
        System.out.println(topology.describe());
        System.out.println("================================================");
    }
}