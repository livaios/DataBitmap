package databandits;
import java.io.Serializable;

@SuppressWarnings("serial")
public class BitIndex implements Serializable {
	private Boolean value;
	private int pageIndex;
	private int tupleIndex;

	public BitIndex(Boolean value, int pageIndex, int tupleIndex) {
		super();
		this.value = value;
		this.pageIndex = pageIndex;
		this.tupleIndex = tupleIndex;
	}

	public Boolean getValue() {
		return value;
	}

	public void setValue(Boolean value) {
		this.value = value;
	}

	public int getPageIndex() {
		return pageIndex;
	}

	public void setPageIndex(int pageIndex) {
		this.pageIndex = pageIndex;
	}

	public int getTupleIndex() {
		return tupleIndex;
	}

	public void setTupleIndex(int tupleIndex) {
		this.tupleIndex = tupleIndex;
	}

	public String toString() {
		if (value) {
			return "1";
		} else {
			return "0";
		}
	}

}
