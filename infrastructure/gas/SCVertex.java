package infrastructure.gas;

import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;

/**
 * Core Vertex Class for the SCV program. This has the compute() method which
 * basically does all the work during the iterative computation. Extends the
 * Vertex class from org.apache.giraph.graph.Vertex
 * 
 * 
 * @author dhruvsharma1
 * 
 */
public class SCVertex extends
		Vertex<IntWritable, SCVertexValue, SCEdge, FPMessage> {

	@Override
	public void compute(Iterable<FPMessage> fpMessages) throws IOException {
		SCVertexValue currentVertex = getValue();
		int currentVertexId = getId().get();
		if (getSuperstep() == 0 && isSource()) {
			/*
			 * setting the discovery distance to 0
			 */
			currentVertex.setDiscoveryDistance(0);
			/*
			 * Iterating over all neighbor nodes.
			 */
			for (Iterator<Edge<IntWritable, SCEdge>> edgeIter = getEdges()
					.iterator(); edgeIter.hasNext();) {
				Edge<IntWritable, SCEdge> edge = edgeIter.next();
				/* vertex to send a message to */
				IntWritable targetVertexId = edge.getTargetVertexId();
				/* distance of the target vertex */
				double distance = 1.0 / edge.getValue().getValue().getWeight();
				/* hops from source vertex */
				int hops = 1;
				/* packets of the message */
				int packets = 1;
				/* Creating a FP Message to send */
				FPMessage fpmsg = new FPMessage(currentVertexId, hops, packets,
						distance);
				/* sending a message to all outgoing edges */
				sendMessage(targetVertexId, fpmsg);
			}
			voteToHalt();
		} else if (getSuperstep() == 0 && !isSource()) {
			/*
			 * if the current super step is 0 and the current vertex is not the
			 * source vertex simpy vote to halt.
			 */
			voteToHalt();
		} else {
			/*
			 * Iterating over each incoming message and then sendimg the useful
			 * ones on the outgoing edges.
			 */
			for (Iterator<FPMessage> fpmsgIter = fpMessages.iterator(); fpmsgIter
					.hasNext();) {
				FPMessage msg = fpmsgIter.next();
				double msgDist = msg.getDistance();
				if (msgDist < currentVertex.getDiscoveryDistance()) {
					/*
					 * The vertex has been discovered at a a shorter path than
					 * before. So we need to flush all the existing values of
					 * this vertex and set a new discovery distance.
					 */
					currentVertex.setDiscoveryDistance(msgDist);
					currentVertex.flushSCVertexValue();
				} else if (msgDist == currentVertex.getDiscoveryDistance()) {
					/**
					 * Do nothing the vertx has been discovered at the same
					 * distance but via another path. So no need to flush the
					 * vertex values.
					 */
				} else {
					continue;
				}
				/**
				 * Add a new message to the vertex for discovery via a shortest
				 * path.
				 */
				currentVertex.addDiscoveryMessage(msg
						.getMessageSourceVertexId(), msg.getHops() + 1, msg
						.getPackets(), msgDist);
			}
			for (Iterator<Edge<IntWritable, SCEdge>> edgeIter = getEdges()
					.iterator(); edgeIter.hasNext();) {
				Edge<IntWritable, SCEdge> edge = edgeIter.next();
				/* vertex to send a message to */
				IntWritable targetVertexId = edge.getTargetVertexId();
				for (Iterator<Integer> hopIter = currentVertex
						.getHopsPacketCountMap().keySet().iterator(); hopIter
						.hasNext();) {
					int hops = hopIter.next();
					/* distance of the target vertex */
					double distance = currentVertex.getDiscoveryDistance()
							+ 1.0 / edge.getValue().getValue().getWeight();
					int packets = currentVertex.getHopsPacketCountMap().get(
							hops);
					/*
					 * Adding a new message to all outgoing edges.
					 */
					FPMessage fpmsg = new FPMessage(currentVertexId, hops++,
							packets, distance);
					sendMessage(targetVertexId, fpmsg);
				}
			}
			/*
			 * Voting to halt for this super step.
			 */
			voteToHalt();
		}
	}

	/*
	 * method checks if the current vertex id is equal to the current iteration
	 * source id. If true then the current vertex is the source vertex.
	 */
	public boolean isSource() {
		return (getId().get() == ((IntWritable) getAggregatedValue("source_id"))
				.get());
	}

}
