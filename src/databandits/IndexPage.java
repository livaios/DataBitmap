package databandits;

import java.io.Serializable;
import java.util.Vector;

@SuppressWarnings("serial")
public class IndexPage implements Serializable{
	private Vector<IndexTuple> tuples;

	public IndexPage(int indexCapacity) {
		
		this.tuples = new Vector<IndexTuple>(indexCapacity);
	}

	public Vector<IndexTuple> getTuples() {
		return tuples;
	}
	public void addToVector(IndexTuple add) {
		this.tuples.addElement(add);
		
	}
	public String toString() {
		String print="";
		for(int i = 0; i< tuples.size();i++) {
			print+=tuples.get(i);
		}
		return print;
	}
	public void setTuples(Vector<IndexTuple> tuples) {
		this.tuples = tuples;
	}
	
	
}
