package databandits;
import java.io.Serializable;
import java.util.ArrayList;

@SuppressWarnings("serial")
public class CompressedBit implements Serializable{
	private Boolean value;
	private int occur;
	private ArrayList<String> data;

	public ArrayList<String> getData(){
		return data;
	}
	public int getOccur() {
		return occur;
	}

	public void setOccur(int occur) {
		this.occur = occur;
	}

	public Boolean getValue() {
		return value;
	}

	public void setValue(Boolean value) {
		this.value = value;
	}

	public CompressedBit(Boolean value, int occur,ArrayList<String> data) {
		this.value = value;
		this.occur = occur;
		this.data = data;
	}

	public String toString() {
		if (value) {
			return   occur+":1" ;
		} else {
			return  occur+":0" ;
		}
	}

}
