package databandits;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.ParseException;
import java.util.Hashtable;
import java.util.Iterator;

public class DBApp {

	public static void main(String[] args) throws ParseException {
		DBApp db = new DBApp();
		String strTableName = "Student";
		Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.Double");
		try {
			db.createTable(strTableName, "id", htblColNameType);
		} catch (DBAppException e) {
			e.printStackTrace();
		}
		try {

			// db.createBitmapIndex("Doctor", "name");
			Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
			htblColNameValue.put("id", new Integer(5));
			htblColNameValue.put("name", new String("Ahmed Noor"));
			htblColNameValue.put("gpa", new Double(0.95));
			db.insertIntoTable(strTableName, htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(4));
			htblColNameValue.put("name", new String("Ahmed Noor"));
			htblColNameValue.put("gpa", new Double(0.95));
			db.insertIntoTable(strTableName, htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(3));
			htblColNameValue.put("name", new String("Dalia Noor"));
			htblColNameValue.put("gpa", new Double(1.25));
			db.insertIntoTable(strTableName, htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(2));
			htblColNameValue.put("name", new String("John Noor"));
			htblColNameValue.put("gpa", new Double(1.5));
			db.insertIntoTable(strTableName, htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(1));
			htblColNameValue.put("name", new String("Zaky Noor"));
			htblColNameValue.put("gpa", new Double(0.88));
			db.insertIntoTable(strTableName, htblColNameValue);

			// Doctor Table
			strTableName = "Doctor";
			htblColNameType.clear();
			htblColNameType.put("number", "java.lang.Integer");
			htblColNameType.put("name", "java.lang.String");
			htblColNameType.put("hot", "java.lang.Double");
			try {
				db.createTable(strTableName, "number", htblColNameType);
			} catch (DBAppException e) {
				e.printStackTrace();
			}
			htblColNameValue.clear();
			htblColNameValue.put("number", new Integer(5));
			htblColNameValue.put("name", new String("Ahmed Noor"));
			htblColNameValue.put("hot", new Double(0.95));
			db.insertIntoTable(strTableName, htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("number", new Integer(4));
			htblColNameValue.put("name", new String("Ahmed Noor"));
			htblColNameValue.put("hot", new Double(0.95));
			db.insertIntoTable(strTableName, htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("number", new Integer(3));
			htblColNameValue.put("name", new String("Dalia Noor"));
			htblColNameValue.put("hot", new Double(1.25));
			db.insertIntoTable(strTableName, htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("number", new Integer(2));
			htblColNameValue.put("name", new String("Lina"));
			htblColNameValue.put("hot", new Double(1.5));
			db.insertIntoTable(strTableName, htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("number", new Integer(1));
			htblColNameValue.put("name", new String("Lina"));
			htblColNameValue.put("hot", new Double(0.88));
			db.insertIntoTable(strTableName, htblColNameValue);

		} catch (DBAppException e) {
			e.printStackTrace();
		}
		try {
			db.createBitmapIndex("Student", "id");
			Hashtable<String, Object> deleteItems = new Hashtable<String, Object>();
			deleteItems.put("id", new Integer(5));
			deleteItems.put("gpa", new Double(0.95));
			db.deleteFromTable("Student", deleteItems);
			System.out.println("deleted tuples");
			Hashtable<String, Object> updateItems = new Hashtable<String, Object>();
			updateItems.put("gpa", new Double(3.00));
			updateItems.put("id", new Integer(3)); 
			db.updateTable("Student", "" + 2, updateItems);
			System.out.println("updated tuples");
		} catch (DBAppException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
//		SQLTerm[] arrSQLTerms;
//		arrSQLTerms = new SQLTerm[2];
//		arrSQLTerms[0].set_strTableName("Student");
//		arrSQLTerms[0].set_strColumnName("name");
//		arrSQLTerms[0].set_strOperator("=");
//		arrSQLTerms[0].set_objValue("John Noor");
//		arrSQLTerms[1].set_strTableName("Student");
//		arrSQLTerms[1].set_strColumnName("gpa");
//		arrSQLTerms[1].set_strOperator("=");
//		arrSQLTerms[1].set_objValue(new Double(1.5));
//		String[] strarrOperators = new String[1];
//		strarrOperators[0] = "OR";

		// select * from Student where name = "John Noor" or gpa = 1.5; 
		//Iterator resultSet = selectFromTable(arrSQLTerms , strarrOperators);
	}

	public void writeTable(Table table) {
		try {
			FileOutputStream fileOut = new FileOutputStream("./classes/databandits/" + table.getTableName() + ".class");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(table);
			out.close();
			fileOut.close();
			System.out.println("Serialized data is saved in " + table.getTableName() + ".class");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private Table readTable(String tableName) {
		Table currT = null;
		try {
			ObjectInputStream is = new ObjectInputStream(
					new FileInputStream("./classes/databandits/" + tableName + ".class"));
			currT = (Table) is.readObject();
			is.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return currT;
	}

	public void init() {
		// this does whatever initialization you would like
		// or leave it empty if
		// there is no code you want to
		// execute at application startup

	}

	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException {
		Table newTable = new Table(strTableName, htblColNameType, strClusteringKeyColumn);
		writeTable(newTable);
		System.out.println(newTable);

	}

	public void createBitmapIndex(String strTableName, String strColName) throws DBAppException {
		Table currentTable = readTable(strTableName);
		currentTable.createBitmap(strColName);
		System.out.println(currentTable);
		writeTable(currentTable);
	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		Table currentTable = readTable(strTableName);
		currentTable.insert(htblColNameValue);
		System.out.println(currentTable);
		writeTable(currentTable);
	}

	public void updateTable(String strTableName, String strKey, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, ParseException {
		Table currentTable = readTable(strTableName);
		currentTable.update(strKey, htblColNameValue, 1);
		System.out.println(currentTable);
		writeTable(currentTable);
	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		Table currentTable = readTable(strTableName);
		currentTable.delete(htblColNameValue);
		System.out.println(currentTable);
		writeTable(currentTable);
	}

	public Iterator<Object> selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
		String strTableName = "";
		SQLTerm query1;
		SQLTerm query2;

		strTableName = arrSQLTerms[0].get_strTableName();
		query1 = arrSQLTerms[0];
		query2 = arrSQLTerms[1];

		Table currentTable = readTable(strTableName);
		return currentTable.searchTable(query1, query2, strarrOperators[0]);
	}
}
