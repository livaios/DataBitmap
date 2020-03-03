package databandits;
import java.io.Serializable;
import java.util.Vector;

@SuppressWarnings("serial")
public class Page implements Serializable {

	private transient String tableName;
	private transient int pageID;
	private transient String key; // clustering key
	private Vector<Tuple> tuples;

	public Page(int pageCapacity, int pageID, String key, String tableName) {
		this.tuples = new Vector<Tuple>(pageCapacity);
		this.setKey(key);
		this.setPageID(pageID);
		this.setTableName(tableName);
	}

	public int getPageID() {
		return pageID;
	}

	public void setPageID(int pageID) {
		this.pageID = pageID;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public Vector<Tuple> getTuples() {
		return tuples;
	}
	
	public String toString() {
		String print = "";
		for (int i = 0; i < tuples.size(); i++) {
			print += "tuple: [" + tuples.elementAt(i).toString() + "]\n";
		}
		return print;
	}
	
	public boolean isEmpty() {
		return tuples.size()==0;
	}
	
	public void deleteTuple(Tuple t) {
		tuples.remove(t);
	}

}
