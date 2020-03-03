package databandits;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Scanner;
import java.util.Vector;

@SuppressWarnings("serial")
public class Table implements Serializable {

	private String tableName;
	private Hashtable<String, String> htblColNameType;
	private String key;
	private int pageNum = 0;
	private int indexNum = 0;
	private transient static StringBuilder metaData = new StringBuilder(
			"Table Name, Column Name, Column Type, Key, Indexed \n");;
	private transient Scanner validator;
	private int pageCapacity = 2;
	private int indexCapacity = 2;
	private ArrayList<String> indexedAtts = new ArrayList<String>();

	public Table(String tableName, Hashtable<String, String> htblColNameType, String strClusteringKeyColumn) {
		this.tableName = tableName;
		this.htblColNameType = htblColNameType;
		this.key = strClusteringKeyColumn;
		pageNum++;
		Page newPage = new Page(pageCapacity, pageNum, key, tableName);
		writePage(newPage, pageNum);
		updateMeta();
		writeMeta();
	}

	public String toString() {
		String print = "";
		for (int i = 1; i <= pageNum; i++) {
			print += "Page" + i + ": \n" + readPage(i).toString();
		}
		return print;
	}

	private Page readPage(int indexPage) {
		Page currP = null;
		try {
			ObjectInputStream is = new ObjectInputStream(
					new FileInputStream("./data/" + tableName + " Page" + indexPage + ".class"));
			currP = (Page) is.readObject();
			is.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return currP;
	}

	public void writePage(Page page, int indexPage) {
		try {
			FileOutputStream fileOut = new FileOutputStream("./data/" + tableName + " Page" + indexPage + ".class");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(page);
			out.close();
			fileOut.close();
			System.out.println("Serialized data is saved in " + tableName + " Page" + indexPage + ".class");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void validateTupleValues(Hashtable<String, Object> tupleValues) throws DBAppException {
		Enumeration<String> colName = tupleValues.keys();
		String currentValue = "";
		Boolean found = false;
		Object tblName;
		Object columnName;
		Object columnType = null;
		while (colName.hasMoreElements()) {
			currentValue = (String) colName.nextElement();
			try {
				validator = new Scanner(new File("./data/metadata.csv"));
				validator.useDelimiter("[,\n]");
				validator.next(); // removing
				validator.next(); // first
				validator.next(); // line
				validator.next(); // in
				validator.next(); // metadata
				found = false;
				while (validator.hasNext() && !found) {
					tblName = validator.next();
					columnName = validator.next();
					columnType = validator.next();
					validator.next(); // key
					validator.next(); // isindexed
					if (columnName.equals(currentValue) && tblName.equals(tableName)) {
						found = true;
					}
				}
				if (!("" + tupleValues.get(currentValue).getClass()).equals("class " + columnType)) {
					throw new DBAppException("Data entered in hashtable is of incorrect type");
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
	}

	private void writeMeta() {
		try (PrintWriter writer = new PrintWriter(new File("./data/metadata.csv"))) {
			writer.write(metaData.toString());
		} catch (FileNotFoundException e) {
			System.out.println(e.getMessage());
		}
	}

	private void updateMeta() {
		Enumeration<String> attributes = htblColNameType.keys();
		while (attributes.hasMoreElements()) {
			String elem = (String) attributes.nextElement();
			metaData.append(tableName);
			metaData.append(',');
			metaData.append(elem);
			metaData.append(',');
			metaData.append(htblColNameType.get(elem));
			metaData.append(',');
			metaData.append(elem == key);
			metaData.append(',');
			metaData.append("false");
			metaData.append('\n');
			System.out.println(htblColNameType);
		}
	}

	public String getTableName() {
		return tableName;
	}

	public void insert(Hashtable<String, Object> htblColNameValue) throws DBAppException {
		validateTupleValues(htblColNameValue);
		try {
			insertHelper2(htblColNameValue, 1, 0);
			for(int i = 0;i<indexedAtts.size();i++) {
				if(htblColNameValue.containsKey(indexedAtts.get(i))) {
					updateBitmap(indexedAtts.get(i));
				}
			}
			
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	public void insertHelper2(Hashtable<String, Object> htblColNameValue, int indexPage, int indexTuple)
			throws ClassNotFoundException, IOException, DBAppException {
		Tuple toBeInsert = new Tuple(htblColNameValue, key, htblColNameType.get(key));
		// System.out.println(toBeInsert);
		if (indexPage > pageNum) {
			pageNum++;
			Page newPage = new Page(pageCapacity, pageNum, key, tableName);
			newPage.getTuples().add(toBeInsert);
			Collections.sort(newPage.getTuples());
			writePage(newPage, indexPage);
		} else {
			Page currentPage = readPage(indexPage);
			if (indexTuple > pageCapacity - 2) {
				currentPage.getTuples().add(indexTuple, toBeInsert);

				Collections.sort(currentPage.getTuples());
				// System.out.println("SORTED \n" + currentPage);
				Hashtable<String, Object> lastTup = currentPage.getTuples().lastElement().getTupleValues();
				currentPage.getTuples().remove(currentPage.getTuples().lastElement());
				// System.out.println("last tuple: "+lastTup);
				writePage(currentPage, indexPage);
				insertHelper2(lastTup, ++indexPage, 0);

			} else {
				if (!(currentPage.getTuples().size() == 0)) { // When you finish a page jump to next page with index of
																// tuple
					Tuple currentTuple = currentPage.getTuples().get(indexTuple);
					if (currentTuple == (null)) {
						if (getNext(indexPage, indexTuple) == (null)) {
							currentPage.getTuples().add(indexTuple, toBeInsert);
							Collections.sort(currentPage.getTuples());
							writePage(currentPage, indexPage);
						} else if ((getNext(indexPage, indexTuple).compareTo(toBeInsert)) > 0) {
							// System.out.println("Next is greater and current place is empty");
							currentPage.getTuples().add(indexTuple, toBeInsert);
							Collections.sort(currentPage.getTuples());
							writePage(currentPage, indexPage);
						} else if ((getNext(indexPage, indexTuple).compareTo(toBeInsert)) < 0) {
							// System.out.println("Next is less than so skip");
							insertHelper2(htblColNameValue, indexPage, ++indexTuple);
						}
						// System.out.println("current was empty and i did nothing");
					} else {
						if (currentTuple.compareTo(toBeInsert) == 0) {
							throw new DBAppException("Identical Key");
						}
						if (getNext(indexPage, indexTuple) == (null)) {
							currentPage.getTuples().add(indexTuple, toBeInsert);
							Collections.sort(currentPage.getTuples());
							writePage(currentPage, indexPage);
						} else if ((getNext(indexPage, indexTuple).compareTo(toBeInsert)) < 0) {
							// System.out.println("Next is less than so skip");
							insertHelper2(htblColNameValue, indexPage, ++indexTuple);
						} else if ((getNext(indexPage, indexTuple).compareTo(toBeInsert)) > 0) {
							// System.out.println("Next is greater and current place is full");
							Hashtable<String, Object> currTup = currentPage.getTuples().get(indexTuple)
									.getTupleValues();
							currentPage.getTuples().remove(indexTuple);
							currentPage.getTuples().add(indexTuple, toBeInsert);
							Collections.sort(currentPage.getTuples());
							writePage(currentPage, indexPage);
							insertHelper2(currTup, indexPage, ++indexTuple);
						}

					}
				} else {
					currentPage.getTuples().add(indexTuple, toBeInsert);
					Collections.sort(currentPage.getTuples());
					writePage(currentPage, indexPage);
				}
			}
		}
	}

	public Tuple getNext(int currentPage, int currentIndex) {
		currentIndex++;
		if (currentIndex > pageCapacity) {
			currentIndex -= pageCapacity;
			currentPage++;
			Page page = readPage(currentPage);
			return page.getTuples().get(currentIndex);
		} else {
			Page page = readPage(currentPage);
			if (page.getTuples().size() <= currentIndex)
				return null;
			else
				return page.getTuples().get(currentIndex);
		}
	}

	public void update(String strKey, Hashtable<String, Object> htblColNameValue, int indexPage) throws DBAppException {
		validateTupleValues(htblColNameValue);
		if (indexPage <= pageNum) {
			Page currentPage = readPage(indexPage);
			Boolean found = false, updatedKey = false;
			for (int i = 0; i < currentPage.getTuples().size(); i++) {
				Tuple currentTuple = currentPage.getTuples().get(i);
				String currentTupleKey = "" + currentTuple.get(currentTuple.getKey());
				found = currentTupleKey.equals(strKey);
				if (found) {
					updatedKey = currentTuple.updateTupleValues(htblColNameValue);
					if (updatedKey) {
						currentTuple = currentPage.getTuples().get(i); // getting updated version of tuple
						// currentPage.deleteTuple(currentTuple);
						currentPage.getTuples().remove(i);
						writePage(currentPage, indexPage);
						this.insert(currentTuple.getTupleValues());
					} else {
						writePage(currentPage, indexPage);
					}

				}
			}
			indexPage++;
			update(strKey, htblColNameValue, indexPage);
		} else {
			// System.out.println("Tuple not found.");
			return;
		}
		for(int i = 0;i<indexedAtts.size();i++) {
			if(htblColNameValue.containsKey(indexedAtts.get(i))) {
				updateBitmap(indexedAtts.get(i));
			}
		}
	}

	public void delete(Hashtable<String, Object> htblColNameValue) throws DBAppException {
		deleteHelper2(htblColNameValue, 1);
		for(int i = 0;i<indexedAtts.size();i++) {
			if(htblColNameValue.containsKey(indexedAtts.get(i))) {
				updateBitmap(indexedAtts.get(i));
			}
		}
	}

	private void deleteHelper2(Hashtable<String, Object> htblColNameValue, int indexPage) {
		if (indexPage <= pageNum) {
			Page currentPage = readPage(indexPage);
			for (int i = 0; i < currentPage.getTuples().size(); i++) {
				Tuple currentTuple = currentPage.getTuples().get(i);
				Boolean delete = currentTuple.almostEqual(htblColNameValue);
				if (delete) {
					currentPage.deleteTuple(currentTuple);
					writePage(currentPage, indexPage);
					if (currentPage.isEmpty()) {
						for (int j = indexPage + 1; j <= pageNum; j++) {
							currentPage = readPage(j);
							if (j != 1) {
								writePage(currentPage, j - 1);
							}
						}
						File filePath = new File("./data/" + tableName + " Page" + pageNum + ".class");
						filePath.delete();
						pageNum--;
					}
				}
			}
			indexPage++;
			deleteHelper2(htblColNameValue, indexPage);
		} else {
			return;
		}
	}

	public void createBitmap(String strColName) {
		// set indexed to true
		indexedAtts.add(strColName);
		updateBitmap(strColName);
	}
	
	public void updateBitmap(String strColName) {
		Bitmap bitmapIndex = new Bitmap(strColName);
		Hashtable<Object, BitArray> values = bitmapIndex.getUniqueValues();
		ArrayList<Object> tempVals = getValues(strColName);
		for (int i = 0; i < tempVals.size(); i++) {// loop on tempVals
			Object currValue = tempVals.get(i);
			BitArray bits = new BitArray();
			for (int j = 1; j <= pageNum; j++) {// loop on page
				Page currP = readPage(j);
				for (int k = 0; k < currP.getTuples().size(); k++) {
					Tuple currTuple = currP.getTuples().get(k);
					if (currValue.equals(currTuple.get(strColName))) {
						bits.addBit(new BitIndex(true, j, k));
					} else {
						bits.addBit(new BitIndex(false, j, k));
					}
				}
			}
			values.put(currValue, bits);

		}
		bitmapIndex.setUniqueValues(values);
		System.out.println("uncompress " + bitmapIndex);
		compress(bitmapIndex);
		System.out.println("compressed " + bitmapIndex);
		bitmapIndex.getIndexTuples();
		Vector<IndexTuple> vector = bitmapIndex.getVector();
		//System.out.println(vector);
		writeIndex(vector,strColName);
	}

	private void writeIndex(Vector<IndexTuple> vector,String attribute) {

		indexNum = 0;
		for (int j = 0; j < vector.size(); ) {
			indexNum++;
			IndexPage indexpage = new IndexPage(indexCapacity);
			for (int i = 0; i < indexCapacity; i++) {
				IndexTuple index = vector.get(j);
				indexpage.addToVector(index);
				j++;
				if(j>=vector.size())
					break;
			}
			writeIndex(indexpage,indexNum,attribute);
			System.out.println(indexpage + "IndexPage =" + indexNum);
		}

	}

	public void compress(Bitmap bitmapIndex) {
		Hashtable<Object, BitArray> values = bitmapIndex.getUniqueValues();
		Enumeration<Object> uniques = values.keys();
		while (uniques.hasMoreElements()) {
			Object currentvalue = (Object) uniques.nextElement();
			BitArray bits = values.get(currentvalue);
			bits.compress();
			values.replace(currentvalue, bits);
		}
		Bitmap returned = new Bitmap(bitmapIndex.getAttribute());
		returned.setUniqueValues(values);
		// return returned;
	}

	public ArrayList<Object> getValues(String strColName) {
		ArrayList<Object> values = new ArrayList<Object>();
		int j = 0, i;
		for (i = 1; i <= pageNum; i++) {
			Page page = readPage(i);
			for (j = 0; j < page.getTuples().size(); j++) {
				if (!(values.contains(page.getTuples().get(j).get(strColName))))
					values.add(page.getTuples().get(j).get(strColName));
			}

		}
		System.out.println(values);
		return values;

	}

	public void writeIndex(IndexPage page, int indexPage, String attribute) {
		try {
			FileOutputStream fileOut = new FileOutputStream(
					"./data/" + tableName + " " + attribute + " Index" + indexPage + ".class");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(page);
			out.close();
			fileOut.close();
			System.out.println(
					"Serialized data is saved in " + tableName + " " + attribute + " Index" + indexPage + ".class");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unused")
	private Page readIndex(int indexPage, String attribute) {
		Page currP = null;
		try {
			ObjectInputStream is = new ObjectInputStream(
					new FileInputStream(tableName + " " + attribute + " Index" + indexPage + ".class"));
			currP = (Page) is.readObject();
			is.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return currP;
	}

	public Iterator<Object> searchTable(SQLTerm query1, SQLTerm query2, String strarrOperators) throws DBAppException{
		ArrayList<Object> result1 = processQuery(query1);
		ArrayList<Object> result2 = processQuery(query2);
		switch(strarrOperators) {
		case "AND":return and(result1,result2);
		case "OR":return or(result1,result2);
		case "XOR":return xor(result1,result2);
		default: throw new DBAppException("Do not support this operation");
		}
		
	}
	
	private Iterator<Object> and(ArrayList<Object> result1, ArrayList<Object> result2) {
		// TODO Auto-generated method stub
		return null;
	}

	private Iterator<Object> or(ArrayList<Object> result1, ArrayList<Object> result2) {
		// TODO Auto-generated method stub
		return null;
	}

	private Iterator<Object> xor(ArrayList<Object> result1, ArrayList<Object> result2) {
		// TODO Auto-generated method stub
		return null;
	}

	public ArrayList<Object> processQuery(SQLTerm query){
		for(int i = 0;i<indexedAtts.size();i++) {
			if(query.get_strColumnName().equals(indexedAtts.get(i))) 
				return binSearch(query);
		}
		return linSearch(query);
	}

	private ArrayList<Object> linSearch(SQLTerm query) {
		ArrayList<Object> foundTuples = new ArrayList<Object>();
		return foundTuples;
	}

	private ArrayList<Object> binSearch(SQLTerm query) {
		ArrayList<Object> foundTuples = new ArrayList<Object>();
		
		return foundTuples;
	}

}
