package databandits;
import java.io.Serializable;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

@SuppressWarnings("serial")
public class Bitmap implements Serializable{
	 private String attribute;
	 private transient Hashtable<Object, BitArray> uniqueValues;
	 private Vector<IndexTuple> vector= new Vector<IndexTuple>();
	 
	 public String getAttribute() {
		return attribute;
	}

	public void setAttribute(String attribute) {
		this.attribute = attribute;
	}

	public Bitmap(String attribute) {
		 this.attribute=attribute;
		 this.uniqueValues = new Hashtable<Object, BitArray>();
	 }
	public void getIndexTuples() {
		Enumeration<Object> values =uniqueValues.keys();
		while(values.hasMoreElements()) {
			Object value = values.nextElement();
			BitArray bits = uniqueValues.get(value);
			this.vector.add(new IndexTuple(value, bits));
		}
	}
	 
	 public Hashtable<Object, BitArray> getUniqueValues(){
		 return uniqueValues;
	 }
	 
	 public Vector<IndexTuple> getVector() {
		return vector;
	}

	public void setVector(Vector<IndexTuple> vector) {
		this.vector = vector;
	}

	public void setUniqueValues(Hashtable<Object, BitArray> uniqueValues){
		 this.uniqueValues = uniqueValues;
	 }
	 public String toString() {
		 return "Bitmap of "  + attribute+" : \n"+ uniqueValues;
	 }
	 
	 
	
	

}
