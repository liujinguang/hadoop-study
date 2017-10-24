package com.jliu.mr.intro;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Edge implements WritableComparable<Edge> {
	private String departureNode;
	private String arrivalNode;

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(departureNode);
		out.writeUTF(arrivalNode);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		departureNode = in.readUTF();
		arrivalNode = in.readUTF();
	}

	@Override
	public int compareTo(Edge edge) {
		return (departureNode.compareTo(edge.departureNode) != 0) ? departureNode.compareTo(edge.departureNode)
				: arrivalNode.compareTo(edge.arrivalNode);
	}
	
	@Override
	public int hashCode() {
		final int prime = 47;
		return departureNode.hashCode() * 47 + arrivalNode.hashCode();
	}

}
