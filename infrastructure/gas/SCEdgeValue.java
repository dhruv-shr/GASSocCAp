package infrastructure.gas;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class SCEdgeValue implements Writable {

	private double weight;

	/**
	 * @param weight
	 */
	public SCEdgeValue(double weight) {
		super();
		this.weight = weight;
	}

	/**
	 * @return the weight
	 */
	public double getWeight() {
		return weight;
	}

	/**
	 * @param weight
	 *            the weight to set
	 */
	public void setWeight(double weight) {
		this.weight = weight;
	}

	/**
	 * Default constructor.
	 */
	public SCEdgeValue() {

	}

	@Override
	public void readFields(DataInput dataInputStream) throws IOException {
		this.weight = dataInputStream.readDouble();
	}

	@Override
	public void write(DataOutput dataOutputStream) throws IOException {
		dataOutputStream.writeDouble(this.weight);
	}

}
