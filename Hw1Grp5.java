import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Arrays;
import java.util.*;

/**
 * The work of ³ÂÍ¨±¦ 
 * Created by tongbaochen on 2018/4/13. 
 * E-mail: tongbaochenchn@163.com
 * The new class that sort based distinct.
 */

public class Hw1Grp5 {
	/**
	 * Function : Return the comparison operator.
	 * 
	 * @param operator
	 *            The comparison operator got from CommandLine.
	 */

	public String getOperator(String operator) {
		switch (operator) {
		case "gt":
			return ">";
		case "ge":
			return ">=";
		case "eq":
			return "==";
		case "ne":
			return "!=";
		case "le":
			return "<=";
		case "lt":
			return "<";
		}
		return operator;

	}

	/**
	 * Function: Return all the rows required by the threshold in a form of
	 * ArrayList.
	 * 
	 * @param array
	 *            The file got from the HDFS
	 * @param operator
	 *            The comparison operator got from CommandLine.
	 * @param columnNameSelected
	 *            The selected column name got from CommandLine.
	 * @param threshold
	 *            The threshold got form the CommandLine.
	 * 
	 */

	public ArrayList<String[]> selectDataRecords(ArrayList<String[]> array, String operator, int columnNameSelected,
			double threshold) {
		ArrayList<String[]> arrayCopy = new ArrayList<String[]>();
		Iterator<String[]> iterators = array.iterator();

		// Return all the rows required by the threshold in a form of ArrayList
		if (operator.equals(">=")) {
			while (iterators.hasNext()) {

				String[] entry = iterators.next();

				if (Double.parseDouble(entry[columnNameSelected]) >= threshold) {
					arrayCopy.add(entry);
				}

			}
			return arrayCopy;
		}

		if (operator.equals("<=")) {
			while (iterators.hasNext()) {

				String[] entry = iterators.next();

				if (Double.parseDouble(entry[columnNameSelected]) <= threshold) {
					arrayCopy.add(entry);
				}

			}
			return arrayCopy;
		}

		if (operator.equals(">")) {
			while (iterators.hasNext()) {

				String[] entry = iterators.next();

				if (Double.parseDouble(entry[columnNameSelected]) > threshold) {
					arrayCopy.add(entry);
				}

			}
			return arrayCopy;
		}

		if (operator.equals("<")) {
			while (iterators.hasNext()) {

				String[] entry = iterators.next();

				if (Double.parseDouble(entry[columnNameSelected]) < threshold) {
					arrayCopy.add(entry);
				}

			}
			return arrayCopy;
		}

		if (operator.equals("==")) {
			while (iterators.hasNext()) {

				String[] entry = iterators.next();

				if (Double.parseDouble(entry[columnNameSelected]) == threshold) {
					arrayCopy.add(entry);
				}

			}
			return arrayCopy;
		}

		if (operator.equals("!=")) {
			while (iterators.hasNext()) {

				String[] entry = iterators.next();

				if (Double.parseDouble(entry[columnNameSelected]) != threshold) {
					arrayCopy.add(entry);
				}

			}
			return arrayCopy;
		}
		return null;

	}

	/**
	 * Function: Return all the rows that sort based distinct in a form of
	 * ArrayList.
	 * 
	 * @param dataRecords
	 *            The dataRecords returned by function selectDataRecords().
	 * @param key
	 *            The distinct key got form the CommandLine.
	 * 
	 */

	public ArrayList<String[]> distinctBySort(ArrayList<String[]> dataRecords, String[] key) {
		Iterator<String[]> iterators = dataRecords.iterator();
		ArrayList<String[]> recordsDistinct = new ArrayList<String[]>();

		ArrayList<String> midToStr = new ArrayList<String>();
		ArrayList<String> records = new ArrayList<String>();

		// For every row,get the result columns and connect them as a string
		while (iterators.hasNext()) {
			ArrayList<String> middle = new ArrayList<String>();
			String[] entry = iterators.next();
			for (int i = 0; i < key.length; i++) {
				middle.add(entry[Integer.parseInt(key[i])]);
			}
			String temp = (middle.toString().replace("[", "")).replace("]", "");
			midToStr.add(temp);
//			System.out.println(temp);
		}

		// Sort according to the distinct key
		Collections.sort(midToStr);

		// Distinct the dataRecords
		records.add(midToStr.get(0));
		for (int i = 1; i < midToStr.size(); i++) {
			if (!midToStr.get(i).equals(records.get(records.size() - 1))) {
				records.add(midToStr.get(i));
			}

		}
		// ArrayList to array
		for (Iterator<String> list = records.iterator(); list.hasNext();) {
			recordsDistinct.add((list.next()).split(","));
		}
		return recordsDistinct;
	}

	/**
	 * Function: Create a table named Result and put the result of sort based
	 * distinct into it.
	 * 
	 * @param dataSet
	 *            DataRecords return by function distinctBySort() .
	 * @param columnKey
	 *            The distinct key required by CommandLine.
	 * 
	 */
	public void setHbaseTable(ArrayList<String[]> dataSet, String[] columnkey) {

		try {
			// create table descriptor
			String tableName = "Result";
			HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
			// create column descriptor
			HColumnDescriptor cf = new HColumnDescriptor("res");
			htd.addFamily(cf);
			// configure HBase
			Configuration configuration = HBaseConfiguration.create();
			HBaseAdmin hAdmin = new HBaseAdmin(configuration);
			// If the Result exits then delete it and create a new one
			if (hAdmin.tableExists(tableName)) {
				hAdmin.disableTable(tableName);
				hAdmin.deleteTable(tableName);
			}
			System.out.println("Creating the table................");
			hAdmin.createTable(htd);
			System.out.println("Table \'" + tableName + "\' created successfully......................");
			hAdmin.close();

			HTable table = new HTable(configuration, tableName);
			List<Put> lists = new ArrayList<Put>();
			// Put the result of sort based distinct into table Result.
			for (int i = 0; i < dataSet.size(); i++) {
				String rowkey = Integer.toString(i);
				Put put = new Put(rowkey.getBytes());
				String[] entry = (dataSet.get(i)).clone();
				for (int j = 0; j < entry.length; j++) {
					put.add("res".getBytes(), ("R" + columnkey[j]).getBytes(), entry[j].getBytes());
				}
				lists.add(put);

			}
			table.put(lists);
			table.close();
//			System.out.println();

		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println("put successfully");

	}

	/*
	 * java Hw1Grp5 R=/hw1/lineitem.tbl select:R4,lt,5 distinct:R13,R14,R8,R9
	 */

	public static void main(String[] args) throws IOException, URISyntaxException {

		// Parsing the CommanLine.
		System.out.println("Parsing the command................");
		String filePath = null;
		if (args[0].startsWith("R")) {
			filePath = args[0].replace("R=", "");
		}
		// selected column.
		int columSelected = 0;
		String operation = null;
		double threshold = 0;
		if (args[1].startsWith("select")) {
			String comparison[] = args[1].replace("select:", "").split(",");
			columSelected = Integer.parseInt(comparison[0].replaceFirst("R", ""));
			operation = comparison[1];
			threshold = Double.parseDouble(comparison[2]);
		}

		// distinct columns
		String distinct[] = args[2].replace("distinct:", "").split(",");
		ArrayList<String> key = new ArrayList<String>();
		for (int i = 0; i < distinct.length; i++) {
			if (distinct[i].startsWith("R")) {
				key.add(distinct[i].replaceAll("[^0-9]", ""));
			}
		}

		// ArrayList to array
		String[] sortKey = new String[key.size()];
		for (int i = 0; i < key.size(); i++)
			sortKey[i] = key.get(i);

		String file = "hdfs://localhost:9000" + filePath;
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(file), conf);
		Path path = new Path(file);
		FSDataInputStream in_stream = fs.open(path);
		BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));
		String s;

		ArrayList<String[]> array = new ArrayList<String[]>();
		String str = null;
		while ((str = in.readLine()) != null) {
			String[] st = str.split("\\|");
			array.add(st);
		}

		Hw1Grp5 htfs = new Hw1Grp5();
		ArrayList<String[]> test = new ArrayList<String[]>(
				htfs.selectDataRecords(array, htfs.getOperator(operation), columSelected, threshold));
		ArrayList<String[]> result = new ArrayList<String[]>(htfs.distinctBySort(test, sortKey));

		// put the result into hbase
		htfs.setHbaseTable(result, sortKey);
		in.close();
		fs.close();
	}

}
