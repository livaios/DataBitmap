package databandits;

import java.io.Serializable;

@SuppressWarnings("serial")
public class IndexTuple implements Serializable{
	private Object value;
	private BitArray bits;
	public Object getValue() {
		return value;
	}
	public void setValue(Object value) {
		this.value = value;
	}
	public BitArray getBits() {
		return bits;
	}
	public void setBits(BitArray bits) {
		this.bits = bits;
	}
	public IndexTuple(Object value, BitArray bits) {
		super();
		this.value = value;
		this.bits = bits;
	}
	public String toString() {
		return value +":" +bits;
	}
}
