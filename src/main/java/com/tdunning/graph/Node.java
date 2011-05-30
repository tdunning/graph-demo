package com.tdunning.graph;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
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
  private String name;
  private int id;
  private Set<Integer> out = Sets.newHashSet();
  private Set<Integer> in = Sets.newHashSet();
  private double weight;
  private int version;

  /**
   * Private constructor called from factory method readNode.
   * @param name The name of the node.
   * @param id   The id.
   */
  private Node(String name, int id) {
    this.name = name;
    this.id = id;
  }

  public static void createNode(ZooKeeper zk, String root, int id, String name) throws InterruptedException, KeeperException {
    zk.multi(Lists.<Op>newArrayList(new Node(name, id).createOp(root)));
  }

  public static Node readNode(ZooKeeper zk, String root, int id) throws InterruptedException, KeeperException {
    Stat s = new Stat();
    Node r = Node.fromBytes(zk.getData(root + "/" + id, false, s));
    r.version = s.getVersion();
    return r;
  }

  /**
   * Set up a connection to another node.
   *
   * @param other The node to connect to.
   */
  public void connectTo(int other) {
    out.add(other);
  }

  /**
   * Set up a connection from another node.
   *
   * @param other The node to connect from.
   */
  public void connectFrom(int other) {
    in.add(other);
  }

  public Op updateOp(String root) {
    return Op.setData(path(root), toBytes(), version);
  }

  public Op checkOp(String root) {
    return Op.setData(path(root), toBytes(), version);
  }

  public Op createOp(String root) {
    return Op.create(path(root), toBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
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
    byte[] nameBytes = name.getBytes(Charsets.UTF_8);
    ByteBuffer r = ByteBuffer.allocate(4 + nameBytes.length + 4 + 8 + 4 + 4 * out.size() + 4 + 4 * in.size());
    r.putInt(nameBytes.length);
    r.put(nameBytes);
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
    int len = buf.getInt();
    byte[] nameBytes = new byte[len];
    buf.get(nameBytes);
    int id = buf.getInt();

    Node r = new Node(new String(nameBytes, Charsets.UTF_8), id);

    r.setWeight(buf.getDouble());

    len = buf.getInt();
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
