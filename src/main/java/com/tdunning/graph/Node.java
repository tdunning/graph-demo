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

import com.google.common.collect.Sets;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.nio.ByteBuffer;
import java.util.Set;

/**
 * A graph consists of <ul> <li> A display name</li> <li> An integer id</li> <li> A list of node
 * id's this node connects to</li> <li> A list of node id's that connect to this node</li> <li> A
 * floating point "weight"</li> <li>the ZK version that this node originally came from</li></ul>
 * <p/>
 * This class includes simplistic serialization to allow it to be put into a Zookeeper znode.
 */
public class Node {
  private int id;
  private Set<Integer> out = Sets.newHashSet();
  private Set<Integer> in = Sets.newHashSet();
  private double weight;
  private int version;

  /**
   * Private constructor called from factory method readNode.
   * @param id   The id.
   */
  private Node(int id) {
    this.id = id;
  }

  /**
   * Factory method that creates a new node and writes it to Zookeeper.
   *
   * @param zk     The Zookeeper connection to write to.
   * @param root   The place where nodes are kept
   * @param id     The id of the node, used to form a path for the data in Zookeeper
   * @throws InterruptedException      If somebody is terminating this thread
   * @throws KeeperException           If we get an error writing to Zookeeper.
   * @return  The new node.
   */
  public static Node createNode(ZooKeeper zk, String root, int id) throws InterruptedException, KeeperException {
    Node r = new Node(id);
    zk.create(r.path(root), r.toBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    return r;
  }

  /**
   * Reads a node from Zookeeper given the id.
   *
   * @param zk     The Zookeeper connection to write to.
   * @param root   The place where nodes are kept
   * @param id     The id of the node, used to form a path for the data in Zookeeper
   * @return       The node read from Zookeeeper
   * @throws InterruptedException      If somebody is terminating this thread
   * @throws KeeperException           If we get an error writing to Zookeeper.
   */
  public static Node readNode(ZooKeeper zk, String root, int id) throws InterruptedException, KeeperException {
    Stat s = new Stat();
    Node r = Node.fromBytes(zk.getData(root + "/" + id, false, s));
    r.version = s.getVersion();
    return r;
  }

  /**
   * Set up a connection to another node.  Don't write back to ZK yet.
   *
   * @param other The node to connect to.
   */
  public void connectTo(int other) {
    out.add(other);
  }

  /**
   * Set up a connection from another node.  Don't write back to ZK yet.
   *
   * @param other The node to connect from.
   */
  public void connectFrom(int other) {
    in.add(other);
  }

  /**
   * Creates an update operation that can be put into a transaction with other operations.
   * @param root  The root directory used to store the graph.
   * @return The Op that will update ZK with this nodes data.
   */
  public Op updateOp(String root) {
    return Op.setData(path(root), toBytes(), version);
  }

  /**
   * Creates an check operation that can be put into a transaction with other operations.
   * This check will verify that this node has not been changed in ZK since it was read.
   * @param root  The root directory used to store the graph.
   * @return The Op that will check the vesion of this node in ZK.
   */
  public Op checkOp(String root) {
    return Op.check(path(root), version);
  }


  /**
   * Get nodes that connect to this node.
   *
   * @return A set of node id's.
   */
  public Set<Integer> getIn() {
    return in;
  }

  public double getWeight() {
    return weight;
  }

  public void setWeight(double weight) {
    this.weight = weight;
  }

  private String path(String root) {
    return root + "/" + id;
  }

  /**
   * Serialize a node.
   *
   * @return A newly created byte buffer containing the serialized form of the node.
   */
  private ByteBuffer toByteBuffer() {
    ByteBuffer r = ByteBuffer.allocate(4 + 8 + 4 + 4 * out.size() + 4 + 4 * in.size());
    r.putInt(id);
    r.putDouble(weight);
    r.putInt(out.size());
    for (Integer x : out) {
      r.putInt(x);
    }
    r.putInt(in.size());
    for (Integer x : in) {
      r.putInt(x);
    }
    return r;
  }

  private byte[] toBytes() {
    return toByteBuffer().array();
  }

  /**
   * Deserialize a node.
   *
   * @param b The bytes containing the node.
   * @return A newly created node.
   */
  private static Node fromBytes(byte[] b) {
    ByteBuffer buf = ByteBuffer.wrap(b);
    int id = buf.getInt();

    Node r = new Node(id);

    r.setWeight(buf.getDouble());

    int len = buf.getInt();
    for (int i = 0; i < len; i++) {
      r.out.add(buf.getInt());
    }

    len = buf.getInt();
    for (int i = 0; i < len; i++) {
      r.in.add(buf.getInt());
    }
    return r;
  }
}
