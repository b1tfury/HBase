package com.serendio.Importtsv;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;

public class Exporttsv {

	final static String DEFAULT_COL_SEPRATOR = ",";
	final static String DEAULT_SEPRATOR = ":";

	static class Dump {		
		private final  byte[][] families;
		private final byte[][] qualifiers;
		private final String[] datatypes ;
		private byte[] row_key ;
		private byte[] cell_value;


		public  Dump(String[] args) throws IOException
		{
			final String tbname = args[0];
			final String columnsSpecification = args[1];
			FileWriter filewriter = new FileWriter(args[2]+".csv");
			PrintWriter out = new PrintWriter(filewriter);
			Configuration conf = HBaseConfiguration.create();
			HTable table = new HTable(conf, tbname);
			Scan s = new Scan();
			ResultScanner scanner = table.getScanner(s);
			ArrayList<String> columnStrings = Lists.newArrayList(Splitter.on(',').
					trimResults().split(columnsSpecification));

			families = new byte[columnStrings.size()][];
			qualifiers = new byte[columnStrings.size()][];
			datatypes = new String[columnStrings.size()];

			for(int i =0;i<columnStrings.size();i++){
				String str = columnStrings.get(i);
				String[] parts = str.split(":");
				if (parts.length != 3) {
					System.out.println("Error in specifying columns");
				} else {
					families[i] = parts[0].getBytes();
					qualifiers[i] = parts[1].getBytes();
					datatypes[i] = parts[2];
				}

			}
			try
			{
				for(Result rr = scanner.next(); rr != null; rr = scanner.next()){
					for(int i =0;i<columnStrings.size();i++){
						cell_value = rr.getValue(families[i], qualifiers[i]);
						try{
							switch (datatypes[i]){
							case "Boolean":
								out.print(Bytes.toBoolean(cell_value)+",");
								break;
							case "Double":
								out.print(Bytes.toDouble(cell_value)+",");
								break;
							case "Float":
								out.print(Bytes.toFloat(cell_value)+",");
								break;
							case "Integer":
								out.print(Bytes.toInt(cell_value)+",");
								break;
							case "Long":
								out.print(Bytes.toLong(cell_value)+",");
								break;
							case "String":
								out.print(Bytes.toString(cell_value)+",");
								break;
							case "Short":
								out.print(Bytes.toShort(cell_value)+",");
								break;
							default:
								out.print(Bytes.toString(cell_value)+",");
								break;
							}
						}
						catch(Exception e){
							e.printStackTrace();
							out.print(Bytes.toString(cell_value)+",");
						}
					}
					out.println();
					out.flush();
				}
				out.close();
			}
			finally{
				System.out.println("File Dumping is complete.");
				scanner.close();
			}
		}
		public byte[] getFamily(int idx) {
			return families[idx];
		}
		public byte[] getQualifier(int idx) {
			return qualifiers[idx];
		}
	}
	/**
	 * 'usage' message prints the error while passing the arguments 
	 **/


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

		Dump d = new Dump(args);
	}
}


