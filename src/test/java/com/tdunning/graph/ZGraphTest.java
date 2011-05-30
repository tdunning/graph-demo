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

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

public class ZGraphTest {
  private static final Watcher IGNORE_EVENTS = new Watcher() {
    @Override
    public void process(WatchedEvent event) {
      // ignore
    }
  };

  private double[] expected = {0, 12.0 / 32, 15.0 / 32, 17.0 / 32, 20.0 / 32, 1};

  /**
   * This class builds a graph which consists of a 5-dimensional hyper-cube.  Each
   * node in the graph has a value which is set to the average of the direct neighbors
   * of that node except that node 0 and node 31 are set to 0 and 1 respectively. This
   * progressive averaging is the same as solving for the voltages if each link in the
   * graph represents a unit resistor.  These voltages can be solved for analytically
   * and should have values 0, 3/8, 15/32, 17/32, 5/8, 1 for nodes that having Hamming
   * weight 0, 1, 2, 3, 4, 5 respectively.
   *
   * This problem is of no intrinsic interested insofar as Zookeeper is concerned except
   * that it illustrates a kind of graph algorithm were the node being updated should only
   * be updated if the neighboring voltages have not changed since the new value has been
   * computed.  Since nodes are connected bidirectionally, the only solution without the
   * multi operation would be to keep the entire graph in a single file.  This would
   * greatly increase the number of contentious updates.
   * @throws java.io.IOException     If we can't read or write to ZK
   * @throws InterruptedException    If somebody interrupted a ZK operation (can't happen, really)
   * @throws org.apache.zookeeper.KeeperException  If something else failed in ZK.
   */
  @Test
  public void hyperCubeResistance() throws IOException, InterruptedException, KeeperException {
    ZooKeeper zk = new ZooKeeper("localhost", 3000, IGNORE_EVENTS);

    // make the nodes
    ZGraph zg = new ZGraph(zk, "/graphx");
    for (int i = 0; i < 32; i++) {
      zg.addNode(i);
    }

    // connect them up as a hypercube
    for (int i = 0; i < 32; i++) {
      for (int step = 1; step < 32; step <<= 1) {
        zg.connect(i, i ^ step);
        zg.connect(i ^ step, i);
      }
    }

    // set boundary conditions
    Node node = zg.getNode(0);
    node.setWeight(0);
    zg.update(node);

    node = zg.getNode(31);
    node.setWeight(1);
    zg.update(node);

    // and run the simulation.  This consists of leaving nodes
    // 0 and 31 fixed and setting randomly chosen other nodes
    // to the average of their incoming neighbors.  This progressive
    // relaxation should converge to a solution.
    Random gen = new Random();
    for (int i = 0; i < 10000; i++) {
      // pick a random node from 1 to 30 inclusive
      int n = gen.nextInt(30) + 1;
      zg.reWeight(n);
      if (i % 1000 == 0) {
        // every so often, print the errors versus expected values
        int testNode = 0;
        System.out.printf("%10d", i);
        for (double x : expected) {
          // print error
          System.out.printf("%10.6f", x - zg.getNode(testNode).getWeight());
          testNode = 2 * testNode + 1;
        }
        System.out.printf("\n");
      }
    }

    int testNode = 0;
    for (double x : expected) {
      Assert.assertEquals(String.format("Node %d", testNode), x, zg.getNode(testNode).getWeight(), 1e-6);
      testNode = 2 * testNode + 1;
    }
  }
}
