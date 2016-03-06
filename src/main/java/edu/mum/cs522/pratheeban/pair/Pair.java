package edu.mum.cs522.pratheeban.pair;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Pair implements WritableComparable<Pair> {
	private Text first;
	private Text second;

	public Pair() {
		this.first = new Text();
		this.second = new Text();
	}
	
	public Pair(String first, String second) {
		this.first = new Text(first);
		this.second = new Text(second);
	}

	public Text getFirst() {
		return first;
	}

	public Text getSecond() {
		return second;
	}

	public void readFields(DataInput dataInput) throws IOException {
		first.readFields(dataInput);
		second.readFields(dataInput);
	}

	public void write(DataOutput dataOuput) throws IOException {
		first.write(dataOuput);
		second.write(dataOuput);
	}

	public int compareTo(Pair pair) {

		int result = first.compareTo(pair.first);
		if (result != 0)
			return result;
		if (second.toString() == "*")
			return -1;
		else if (pair.second.toString() == "*")
			return 1;
		else
			return second.compareTo(pair.second);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((first == null) ? 0 : first.hashCode());
		result = prime * result + ((second == null) ? 0 : second.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object pair) {
		if (pair == null)
			return false;
		if (!(pair instanceof Pair))
			return false;
		Pair p = (Pair) pair;
		if (!this.first.equals(p.first))
			return false;
		else if (!second.equals(p.second))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "(" + first + "," + second + ")";
	}

}
