package infrastructure.gas;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FPMessage implements Writable {

	/*
	 * ID of the vertex sending the message.
	 */
	private int messageSourceVertexId;

	/*
	 * the number of hops it took from the source vertex in this iteration to
	 * the vertex which receives this fp message.
	 */
	private int hops;

	/*
	 * the number of packets in this message.
	 */
	private int packets;

	/*
	 * the distance at which the target vertex for this message is reached.
	 */
	private double distance;

	/**
	 * Default constructor.
	 */
	public FPMessage() {

	}

	/**
	 * @param messageSourceVertexId
	 * @param hops
	 * @param packets
	 * @param distance
	 */
	public FPMessage(int messageSourceVertexId, int hops, int packets,
			double distance) {
		super();
		this.messageSourceVertexId = messageSourceVertexId;
		this.hops = hops;
		this.packets = packets;
		this.distance = distance;
	}

	/**
	 * @return the messageSourceVertexId
	 */
	public int getMessageSourceVertexId() {
		return messageSourceVertexId;
	}

	/**
	 * @param messageSourceVertexId
	 *            the messageSourceVertexId to set
	 */
	public void setMessageSourceVertexId(int messageSourceVertexId) {
		this.messageSourceVertexId = messageSourceVertexId;
	}

	/**
	 * @return the hops
	 */
	public int getHops() {
		return hops;
	}

	/**
	 * @param hops
	 *            the hops to set
	 */
	public void setHops(int hops) {
		this.hops = hops;
	}

	/**
	 * @return the packets
	 */
	public int getPackets() {
		return packets;
	}

	/**
	 * @param packets
	 *            the packets to set
	 */
	public void setPackets(int packets) {
		this.packets = packets;
	}

	/**
	 * @return the distance
	 */
	public double getDistance() {
		return distance;
	}

	/**
	 * @param distance
	 *            the distance to set
	 */
	public void setDistance(double distance) {
		this.distance = distance;
	}

	/**
	 * This method reads the hadoop serialized version of the FPMessage Object
	 * and creates a new object out of it.
	 */
	@Override
	public void readFields(DataInput dataInputStream) throws IOException {
		this.messageSourceVertexId = dataInputStream.readInt();
		this.hops = dataInputStream.readInt();
		this.packets = dataInputStream.readInt();
		this.distance = dataInputStream.readDouble();
	}

	/**
	 * This method writes a hadoop serialized version of the FPMessage object.
	 * The message is written as follows : messageSourceId,hops,packets and
	 * distance.
	 */
	@Override
	public void write(DataOutput dataOutputStream) throws IOException {
		/*
		 * Writing the message source vertex id.
		 */
		dataOutputStream.writeInt(this.messageSourceVertexId);
		/*
		 * Writing the number of hops in the message.
		 */
		dataOutputStream.writeInt(this.hops);
		/*
		 * Writing the number of packets in the message.
		 */
		dataOutputStream.writeInt(this.packets);
		/*
		 * Writing the distance at which the new vertex was discovered.
		 */
		dataOutputStream.writeDouble(this.distance);
	}

}
