package databandits;

import java.io.Serializable;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;

@SuppressWarnings({ "serial", "rawtypes" })
public class Tuple implements Serializable, Comparable {
	private String key; //ex. id
	private String keyType; //ex. int
	private Hashtable<String, Object> tupleValues;// <Column Name (key),Value>
	private Date touchDate; // update when insert and update

	public Tuple(Hashtable<String, Object> tupleValues) {
		this.tupleValues = tupleValues;
		this.touchDate = new Date();
	}
	
	public String getKey() {
		return key;
	}
	
	public void setTouchDate(Date touchDate) {
		this.touchDate = touchDate;
	}

	public Tuple(Hashtable<String, Object> tupleValues,String key,String keyType) {
		this.tupleValues = tupleValues;
		this.touchDate = new Date();
		this.key = key;
		this.keyType = keyType;
	}

	public Hashtable<String, Object> getTupleValues() {
		return tupleValues;
	}

	public void setTupleValues(Hashtable<String, Object> tupleValues) {
		this.tupleValues = tupleValues;
		this.touchDate = new Date();
	}

	public Boolean updateTupleValues(Hashtable<String, Object> updatedValues) {
		Enumeration<String> attr = updatedValues.keys();
		Boolean isKey = false;
		while (attr.hasMoreElements()) {
			String currentKey = (String) attr.nextElement();
			if(currentKey.equals(key)) {
				isKey=true;
			}
			tupleValues.replace(currentKey, updatedValues.get(currentKey));
		}
		this.touchDate = new Date();
		return isKey;
	}

	public Date getTouchDate() {
		return touchDate;
	}

	public Object get(String attribute) {
		return tupleValues.get(attribute);
	}

	public int compareTo(Object tuple) {
		// System.out.println(attribute);
		Tuple newTuple = (Tuple) tuple;
		
		if (keyType.equals("java.lang.Integer")) {
			return ((Integer) this.get(key)).compareTo((Integer) newTuple.get(key));
		} else if (keyType.equals("java.lang.String")) {
			return ((String) this.get(key)).compareTo((String) newTuple.get(key));
		} else if (keyType.equals("java.lang.Double")) {
			return ((Double) this.get(key)).compareTo((Double) newTuple.get(key));
		} else if (keyType.equals("java.lang.Boolean")) {
			return ((Boolean) this.get(key)).compareTo((Boolean) newTuple.get(key));
		} else if (keyType.equals("java.util.Date")) {
			return ((Date) this.get(key)).compareTo((Date) newTuple.get(key));
		}
		System.out.println("couldnt compare");
		return 1;
	}
	
	public String toString() {
		String print = "";
		Enumeration<String> attr = tupleValues.keys();
		while (attr.hasMoreElements()) {
			String currentKey = (String) attr.nextElement();
			String currentValue = "" + tupleValues.get(currentKey);
			print += currentKey + " : " + currentValue + " , ";

		}
		print += "touchDate : " + touchDate;
		return print;
	}

	public Boolean almostEqual(Hashtable<String, Object> htblColNameValue) {
		Enumeration<String> keys = htblColNameValue.keys();
		while(keys.hasMoreElements()) {
			String column = (String)keys.nextElement();
			Object tuplevalue = this.get(column);
			Object htblvalue = htblColNameValue.get(column);
			if(!tuplevalue.equals(htblvalue)) {
				return false;
			}
		}
		return true;
	}


}
