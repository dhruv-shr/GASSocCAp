package infrastructure.gas;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Writable;

import core.CommonConstants;

/**
 * Basic vertex value class.
 * 
 * @author dhruvsharma1
 * 
 */
public class SCVertexValue implements Writable {

	/**
	 * Maintains the least discovery distance for the current vertex.
	 * Initialized with maximum value (infinity).
	 */
	private double discoveryDistance = Double.MAX_VALUE;

	/**
	 * Maintains the Social Capital Value of the vertex. Initialized with 0.
	 */
	private double scv = 0;

	/**
	 * Maintains a map of the hops and the total packets for hops as a map. This
	 * will be extremely useful as we divide the credit equally amongst all the
	 * nodes participating in the shortest path.
	 */
	private Map<Integer, Integer> hopsPacketCountMap = new HashMap<Integer, Integer>(
			CommonConstants.maxHopThreshold);

	/**
	 * Will be useful during backward propagation as we will only send the
	 * backward propagation message along the edges which participated in
	 * discovery of the shortest paths in the forward propagation pahse.
	 */
	private Set<Integer> activeIncomingEdges = new HashSet<Integer>();

	/**
	 * method to flush the hopsPacketCountMap and the list of active incoming
	 * edges. Should be used when a shorter distance to an already discovered
	 * vertex is found.
	 */
	public void flushSCVertexValue() {
		this.hopsPacketCountMap.clear();
		this.activeIncomingEdges.clear();
	}

	public void addDiscoveryMessage(int messageSourceId, int hops, int packets,
			double distance) {
		/*
		 * Creating an entry in the hopsPacketCountMap for the new message.
		 */
		int v = packets;
		if (hopsPacketCountMap.containsKey(hops)) {
			v += hopsPacketCountMap.get(hops);
		}
		hopsPacketCountMap.put(hops, v);
		/*
		 * Creating an entry in the activeIncomingEdges List.
		 */
		activeIncomingEdges.add(messageSourceId);
	}

	/**
	 * Default constructor for SCVertexValue class.
	 */
	public SCVertexValue() {

	}

	/**
	 * @param discoveryDistance
	 * @param hopsPacketCountMap
	 * @param activeIncomingEdges
	 */
	public SCVertexValue(double discoveryDistance,
			Map<Integer, Integer> hopsPacketCountMap,
			Set<Integer> activeIncomingEdges) {
		super();
		this.discoveryDistance = discoveryDistance;
		this.hopsPacketCountMap = hopsPacketCountMap;
		this.activeIncomingEdges = activeIncomingEdges;
	}

	/**
	 * @return the discoveryDistance
	 */
	public double getDiscoveryDistance() {
		return discoveryDistance;
	}

	/**
	 * @param discoveryDistance
	 *            the discoveryDistance to set
	 */
	public void setDiscoveryDistance(double discoveryDistance) {
		this.discoveryDistance = discoveryDistance;
	}

	/**
	 * @return the hopsPacketCountMap
	 */
	public Map<Integer, Integer> getHopsPacketCountMap() {
		return hopsPacketCountMap;
	}

	/**
	 * @param hopsPacketCountMap
	 *            the hopsPacketCountMap to set
	 */
	public void setHopsPacketCountMap(Map<Integer, Integer> hopsPacketCountMap) {
		this.hopsPacketCountMap = hopsPacketCountMap;
	}

	/**
	 * @return the activeIncomingEdges
	 */
	public Set<Integer> getActiveIncomingEdges() {
		return activeIncomingEdges;
	}

	/**
	 * @param activeIncomingEdges
	 *            the activeIncomingEdges to set
	 */
	public void setActiveIncomingEdges(Set<Integer> activeIncomingEdges) {
		this.activeIncomingEdges = activeIncomingEdges;
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		/*
		 * Reading the discoveryDistance which is a double.
		 */
		this.discoveryDistance = dataInput.readDouble();
		/*
		 * Reading the scv value of the vertex as a double.
		 */
		this.scv = dataInput.readDouble();
		/*
		 * Reading the hops and packet count map as a String.
		 */
		String mapInString = dataInput.readUTF();
		String[] mapElements = mapInString.trim().split(CommonConstants.TAB);
		for (int i = 0; i < mapElements.length; i++) {
			String[] hopsPacketsPair = mapElements[i]
					.split(CommonConstants.COMMA);
			this.hopsPacketCountMap.put(Integer.parseInt(hopsPacketsPair[0]),
					Integer.parseInt(hopsPacketsPair[1]));
		}

		/*
		 * Reading the active incoming edges from the UTF String.
		 */
		String activeICEdgesInString = dataInput.readUTF();
		String[] activeEdges = activeICEdgesInString
				.split(CommonConstants.COMMA);
		for (int i = 0; i < activeEdges.length; i++) {
			this.activeIncomingEdges.add(Integer.parseInt(activeEdges[i]));
		}

		/* Object Fully Loaded from the data input stream */
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		/* serialized writing of the object for the hadoop framework to use */
		dataOutput.writeDouble(this.discoveryDistance);
		/* Writing scv of the vertex */
		dataOutput.writeDouble(this.scv);
		/*
		 * Writing the hops and packet counts map as a string, will need to read
		 * an decompose this string in the readFileds() method as well. The
		 * object will be written as hop1,count1<tab>hops-2,count2 and so on.
		 */
		StringBuffer buf = new StringBuffer();
		for (Iterator<Integer> hopsPacketCountMapIter = this.hopsPacketCountMap
				.keySet().iterator(); hopsPacketCountMapIter.hasNext();) {
			int hops = hopsPacketCountMapIter.next();
			int packets = this.hopsPacketCountMap.get(hops);
			buf.append(Integer.toString(hops));
			buf.append(CommonConstants.COMMA);
			buf.append(Integer.toString(packets));
			buf.append(CommonConstants.TAB);
		}
		dataOutput.writeUTF(buf.toString());

		/**
		 * Writing the list of active incoming edges as a string.
		 */
		StringBuffer buf2 = new StringBuffer();
		for (Iterator<Integer> activeincomingEdgeIter = this.activeIncomingEdges
				.iterator(); activeincomingEdgeIter.hasNext();) {
			buf2.append(Integer.toString(activeincomingEdgeIter.next()));
			buf2.append(CommonConstants.COMMA);
		}
		dataOutput.writeUTF(buf2.toString());

		/* Object fully written to the data output stream */
	}

}
