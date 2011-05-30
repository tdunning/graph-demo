package com.tdunning.graph;

import com.google.common.base.Charsets;
import com.google.common.collect.Sets;

import java.nio.ByteBuffer;
import java.util.Set;

/**
 * A graph consists of
 * <ul>
 * <li> A display name</li>
 * <li> An integer id</li>
 * <li> A list of node id's this node connects to</li>
 * <li> A list of node id's that connect to this node</li>
 * <li> A floating point "weight"</li>
 * </ul>
 *
 * This class includes simplistic serialization to allow it to be put into a Zookeeper znode.
 */
public class Node {
    private String name;
    private int id;
    private Set<Integer> out = Sets.newHashSet();
    private Set<Integer> in = Sets.newHashSet();
    private double weight;

    public Node(String name, int id) {
        this.name = name;
        this.id = id;
    }

    public ByteBuffer toBytes() {
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

    public static Node fromBytes(byte[] b) {
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

    public void connectTo(int other) {
        out.add(other);
    }

    public void connectFrom(int other) {
        in.add(other);
    }

    public Set<Integer> getIn() {
        return in;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public int getId() {
        return id;
    }
}
