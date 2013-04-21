package infrastructure.gas;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class SCEdge implements Edge<IntWritable, SCEdgeValue>, Writable {

	private int targetVertexId;

	private SCEdgeValue edgeVal;

	public void setTargetVertexId(int targetVertexId) {
		this.targetVertexId = targetVertexId;
	}

	public void setEdgeValue(double weight) {
		this.edgeVal = new SCEdgeValue(weight);
	}

	@Override
	public IntWritable getTargetVertexId() {
		return new IntWritable(targetVertexId);
	}

	@Override
	public SCEdgeValue getValue() {
		return this.edgeVal;
	}

	@Override
	public void readFields(DataInput dataInputStream) throws IOException {
		this.targetVertexId = dataInputStream.readInt();
		this.edgeVal = new SCEdgeValue(dataInputStream.readDouble());
	}

	@Override
	public void write(DataOutput dataOutputStream) throws IOException {
		dataOutputStream.writeInt(this.targetVertexId);
		dataOutputStream.writeDouble(this.edgeVal.getWeight());
	}

}
