package databandits;
import java.io.Serializable;
import java.util.ArrayList;

@SuppressWarnings("serial")
public class BitArray implements Serializable {
	private ArrayList<BitIndex> bits;
	private ArrayList<CompressedBit> compressedBits;
	private boolean compressed;

	public BitArray() {
		bits = new ArrayList<BitIndex>();
	}

	public ArrayList<?> getBits() {
		if (!compressed)
			return bits;
		else
			return compressedBits;
	}

	public void compress() {
		ArrayList<CompressedBit> newbits = new ArrayList<CompressedBit>();
		int n = bits.size();
		for (int i = 0; i < n; i++) {
			ArrayList<String> data = new ArrayList<String>();
			int count = 1;
			while (i < n - 1 && bits.get(i).getValue() == bits.get(i + 1).getValue()) {
				count++;
				data.add(bits.get(i).getPageIndex() + "," + bits.get(i).getTupleIndex());
				// System.out.println(data.get(i));
				i++;
			}
			data.add(bits.get(i).getPageIndex() + "," + bits.get(i).getTupleIndex());
			CompressedBit newB = new CompressedBit(bits.get(i).getValue(), count, data);
			newB.setOccur(count);
			newbits.add(newB);
		}
		this.compressedBits = newbits;
		this.setCompressed(true);
	}

	public void decompress() {
		ArrayList<BitIndex> newbits = new ArrayList<BitIndex>();
		for (int i = 0; i < compressedBits.size(); i++) {
			CompressedBit currBit = compressedBits.get(i);

			for (int j = 0; j < currBit.getOccur(); j++) {
				String[] pageTuple = currBit.getData().get(j).split(",");
				BitIndex newBit = new BitIndex(currBit.getValue(), Integer.parseInt(pageTuple[0]),
						Integer.parseInt(pageTuple[1]));
				newbits.add(newBit);
			}
		}
		this.bits = newbits;
		this.setCompressed(false);

	}

	public String toString() {
		String print = "[";
		for (int i = 0; i < getBits().size(); i++) {
			if (i == getBits().size() - 1)
				print += getBits().get(i)+"]";
			else
				print += getBits().get(i) + ",";

		}
		return print +"\n";
	}

	public boolean isCompressed() {
		return compressed;
	}

	public void setCompressed(boolean compressed) {
		this.compressed = compressed;
	}

	public void addBit(BitIndex bit) {
		bits.add(bit);

	}
}
