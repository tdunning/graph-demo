package com.tdunning.graph;

import org.apache.zookeeper.*;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

public class ZGraphTest {
    private ZGraph zg;

    @Test
    public void hyperCubeResistance() throws IOException, InterruptedException, KeeperException {
        ZooKeeper zk = new ZooKeeper("localhost", 30000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                // ignore
            }
        });

        try {
            zk.create("/graphx", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException e) {
            // ignore
        }

        // make the nodes
        zg = new ZGraph(zk, "/graphx");
        for (int i = 0; i < 32; i++) {
            zg.addNode(i, i + "");
        }

        // connect them up as a hypercube
        for (int i = 0; i < 32; i++) {
            for (int step = 1; step < 32; step <<= 1) {
                zg.connect(i, i ^ step);
                zg.connect(i ^ step, i);
            }
        }

        // set boundary conditions
        Node node = zg.getNode(0, null);
        node.setWeight(0);
        zg.update(node);

        node = zg.getNode(31, null);
        node.setWeight(1);
        zg.update(node);

        // and run the simulation
        Random gen = new Random();
        for (int i = 0; i < 100000; i++) {
            int n = gen.nextInt(32);
            if (i % 100 == 0 && n != 0 && n != 31) {
                zg.reweight(n);
                System.out.printf("%5.4f   %5.4f   %5.4f   %5.4f   %5.4f\n", weight(0), weight(1), weight(7), 1 - weight(30), 1 - weight(31));
            }
        }
    }

    private double weight(int n) throws InterruptedException, KeeperException {
        return zg.getNode(n, null).getWeight();
    }
}
