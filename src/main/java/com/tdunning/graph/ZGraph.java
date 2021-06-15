/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tdunning.graph;


import com.google.common.collect.Lists;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * A graph stored in a Zookeeper directory.  Updates to the graph are done atomically.
 */
public class ZGraph {
    private ZooKeeper zk;
    private String root;

    public ZGraph(ZooKeeper zk, String root) throws InterruptedException, KeeperException {
        this.zk = zk;
        this.root = root;
        try {
            zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException e) {
            // ignore
        }
    }

    public void addNode(int id) throws InterruptedException, KeeperException {
        Node.createNode(zk, root, id);
    }

    /**
     * Connect node id1 to node id2.  Retry until success.  Should only retry if the operation
     * failed due to version issues, all other ZK errors will exit via an exception.  By doing
     * updates to both involved nodes, we can guarantee that the graph represented in ZK is
     * always consistent in that a connection on one node will be reflected by the reverse
     * connection on the target of the connection.
     *
     * @param id1 The from node
     * @param id2 The to node
     * @throws KeeperException      On ZK error other than simultaneous update.
     * @throws InterruptedException If the operation was interrupted, probably due to thread
     *                              termination.
     */
    public void connect(int id1, int id2) throws KeeperException, InterruptedException {
        while (true) {
            try {
                Node g1 = Node.readNode(zk, root, id1);
                Node g2 = Node.readNode(zk, root, id2);

                // update both nodes simultaneously
                g1.connectTo(id2);
                g2.connectFrom(id1);
                List<OpResult> results = zk.multi(Arrays.asList(g1.updateOp(root), g2.updateOp(root)));
                return;
            } catch (KeeperException.BadVersionException e) {
                System.out.printf("%s\n", Arrays.deepToString(e.getResults().toArray()));
                e.printStackTrace();
                // retry version issues
            }
        }
    }

    /**
     * Resets the weight in a node to the average of the neighbors.  The update will retry until it
     * succeeds without the neighbors changing during the update.
     *
     * @param id The node to re-average.
     * @throws KeeperException      On ZK error other than simultaneous update.
     * @throws InterruptedException If the operation was interrupted for some reason.
     */
    public void reWeight(int id) throws KeeperException, InterruptedException {
        while (true) {
            try {
                Node g1 = getNode(id);

                // accumulate the average of the neighbors
                // and collect a list of version checks for them
                double mean = 0;
                Set<Integer> neighbors = g1.getIn();
                List<Op> ops = Lists.newArrayList();
                for (Integer neighbor : neighbors) {
                    Node n = getNode(neighbor);

                    // each version check will be used to be sure the neighbors haven't changed
                    ops.add(n.checkOp(root));
                    mean += n.getWeight();
                }

                // set up the update to the node itself
                g1.setWeight(mean / neighbors.size());
                ops.add(g1.updateOp(root));

                // now do the update in ZK, fail if we collided with another update
                zk.multi(ops);
                return;
            } catch (KeeperException.BadVersionException e) {
                // retry
            }
        }
    }

    /**
     * Read a node from ZK
     *
     * @param id The node id of the node
     * @return The newly constructed local image of the node.
     * @throws InterruptedException If we should be shutting down due to interruption.
     * @throws KeeperException      If ZK threw an error.
     */
    public Node getNode(int id) throws InterruptedException, KeeperException {
        return Node.readNode(zk, root, id);
    }

    /**
     * Write data back to a node.
     *
     * @param node The node to write back.
     * @throws InterruptedException If we are shutting down.
     * @throws KeeperException      If there was an error writing to the node.
     */
    public void update(Node node) throws InterruptedException, KeeperException {
        // use a multi to avoid looking inside th node.
        zk.multi(Arrays.asList(node.updateOp(root)));
    }
}
