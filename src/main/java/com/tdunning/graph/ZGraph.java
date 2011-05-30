package com.tdunning.graph;


import com.google.common.collect.Lists;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * A graph stored in a Zookeeper directory.  Updates to the graph are done atomically.
 */
public class ZGraph {
    private ZooKeeper zk;
    private String root;

    public ZGraph(ZooKeeper zk, String root) {
        this.zk = zk;
        this.root = root;
    }

    public void addNode(int id, String name) throws InterruptedException, KeeperException {
        Node g = new Node(name, id);
        zk.create(path(id), g.toBytes().array(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    /**
     * Connect node id1 to node id2.  Retry until success.  Should only retry if the operation failed
     * due to version issues, all other ZK errors will exit via an exception.
     * @param id1  The from node
     * @param id2  The to node
     * @throws KeeperException On ZK error other than simultaneous update.
     * @throws InterruptedException  If the operation was interrupted, probably due to thread termination.
     */
    public void connect(int id1, int id2) throws KeeperException, InterruptedException {
        while (true) {
            List<OpResult> results = Lists.newArrayList();
            try {
                Stat s1 = new Stat();
                Node g1 = getNode(id1, s1);

                Stat s2 = new Stat();
                Node g2 = getNode(id2, s2);

                g1.connectTo(id2);
                g2.connectFrom(id1);

                List<OpResult> r = zk.multi(Arrays.asList(
                        Op.setData(path(id1), g1.toBytes().array(), s1.getVersion()),
                        Op.setData(path(id2), g2.toBytes().array(), -1)),
                        results);
                return;
            } catch (KeeperException.BadVersionException e) {
                System.out.printf("%s\n", Arrays.deepToString(results.toArray()));;
                e.printStackTrace();
                // retry version issues
            }
        }
    }

    /**
     * Resets the weight in a node to the average of the neighbors.  The update will retry
     * until it succeeds without the neighbors changing during the update.
     * @param id  The node to re-average.
     * @throws KeeperException             On ZK error other than simultaneous update.
     * @throws InterruptedException        If the operation was interrupted for some reason.
     */
    public void reweight(int id) throws KeeperException, InterruptedException {
        while (true) {
            try {
                Stat s0 = new Stat();
                Node g1 = getNode(id, s0);

                double mean = 0;
                Set<Integer> neighbors = g1.getIn();
                List<Op> ops = Lists.newArrayList();
                for (Integer neighbor : neighbors) {
                    Stat s = new Stat();
                    Node n = getNode(neighbor, s);

                    ops.add(Op.check(path(neighbor), s.getVersion()));
                    mean += n.getWeight();
                }

                g1.setWeight(mean / neighbors.size());

                ops.add(Op.setData(path(id), g1.toBytes().array(), s0.getVersion()));
                zk.multi(ops);
                return;
            } catch (KeeperException.BadVersionException e) {
                // retry
            }
        }
    }

    public Node getNode(int id1, Stat s) throws InterruptedException, KeeperException {
        return Node.fromBytes(zk.getData(path(id1), false, s));
    }

    private String path(int id) {
        return root + "/" + id;
    }

    public void update(Node node) throws InterruptedException, KeeperException {
        zk.setData(path(node.getId()), node.toBytes().array(), -1);
    }
}
