package com.serendio.Importtsv;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.csvreader.CsvReader;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

public class Importtsv {
	final static String DEFAULT_COL_SEPRATOR = ",";
	final static String DEAULT_SEPRATOR = ":";

	static class Flush{
		private final  byte[][] families;
		private final byte[][] qualifiers;
		private final ArrayList<String>columnStrings;
		private final String[] datatypes ;
		private final String tableName;
		private final String columnsSpecification;
		private byte[] row_key ;
		private byte[] cell_value;
		private boolean row_key_flag = true;
		private int rowKeyColumnIndex;

		public static String ROWKEY_COLUMN_SPEC="HBASE_ROW_KEY";


		public  Flush(String[] args) throws IOException{
			tableName = args[0];
			columnsSpecification = args[1];
			columnStrings = Lists.newArrayList(Splitter.on(',').
					trimResults().split(columnsSpecification));
			families = new byte[columnStrings.size()][];
			qualifiers = new byte[columnStrings.size()][];
			datatypes = new String[columnStrings.size()];
			for(int i =0;i<columnStrings.size();i++){
				String str = columnStrings.get(i);
				String[] parts = str.split(":");
				if (parts.length != 3) {
					System.out.println("Error in specifying columns");
				} else if (row_key_flag) {
					if(parts[0].split("=").length==2){
						String[] subparts = parts[0].split("=");
						if(subparts[0].equals(ROWKEY_COLUMN_SPEC)){
							rowKeyColumnIndex = i;
							families[i] = subparts[1].getBytes();
							qualifiers[i] = parts[1].getBytes();
							datatypes[i] = parts[2];
						}else{
							System.out.println("Error while specifying the ROWKEY_COLUMN_SPEC");
							System.exit(-1);
						}
					}else{
						families[i] = parts[0].getBytes();
						qualifiers[i] = parts[1].getBytes();
						datatypes[i] = parts[2];
					}
				}
			}
		}

		public void FlushFile(String FileName) throws IOException{
			Configuration config = HBaseConfiguration.create();
			HTable table = new HTable(config, tableName);
			CsvReader items = new CsvReader(FileName);
			items.readHeaders();
			while (items.readRecord()) {
				row_key = changeDataype(items.get(rowKeyColumnIndex),datatypes[rowKeyColumnIndex]);
				Put p = new Put(row_key);
				for (int j = 0 ; j < columnStrings.size(); j++) {
					byte[] cell_value = changeDataype(items.get(j), datatypes[j]);
					p.add(families[j], qualifiers[j], cell_value);
				}
				table.put(p);
			}
		}


		public byte[] changeDataype(String value,String datatype)
		{
			try {
				switch (datatype) {
				case "Boolean":
					return Bytes.toBytes(Boolean.parseBoolean(value));
				case "Double":
					return Bytes.toBytes(Double.parseDouble(value));
				case "Float":
					return Bytes.toBytes(Float.parseFloat(value));
				case "Integer":
					return Bytes.toBytes(Integer.parseInt(value));
				case "Long":
					return Bytes.toBytes(Long.parseLong(value));
				case "String":
					return Bytes.toBytes(value);
				case "Short":
					return Bytes.toBytes(Short.parseShort(value));
				default:
					return Bytes.toBytes(value);
				}
			} catch (Exception e) {
				return Bytes.toBytes("NA");
			}

		}

		public int getRowKeyColumnIndex() {
			return rowKeyColumnIndex;
		}
		public byte[] getFamily(int idx) {
			return families[idx];
		}
		public byte[] getQualifier(int idx) {
			return qualifiers[idx];
		}
	}
	private static void usage(final String errorMsg) {
		if (errorMsg != null && errorMsg.length() > 0) {
			System.err.println("ERROR: " + errorMsg);
		}

	}



	public static void main(String[] args) throws IOException {
		if (args.length < 2) {
			usage("Wrong number of arguments: " + args.length);
			System.exit(-1);
		} 
		String columns[] = args[1].split(DEFAULT_COL_SEPRATOR);
		if (columns == null) {
			usage("No columns specified.");
			System.exit(-1);
		}
		// Make sure one or more columns are specified
		if (columns.length < 2) {
			usage("One or more columns in addition to the row key are required");
			System.exit(-1);
		}
		int rowkeysFound=0;
		for (String col : columns) {
			if (col.contains(Flush.ROWKEY_COLUMN_SPEC))  rowkeysFound++;

		}
		if (rowkeysFound != 1) {
			usage("Must specify exactly one column as " + Flush.ROWKEY_COLUMN_SPEC);
			System.exit(-1);
		}
		Flush f =new Flush(args);
		f.FlushFile(args[2]);
	}
}

