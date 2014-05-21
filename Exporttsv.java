import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.FileWriter;
import java.io.PrintWriter;





public class Exporttsv {
	public static void main(String[] args) throws IOException {
	
		if(args.length!=3)
		{
			System.out.println("Please provide the correct details in a order described below\n <table_name> <all columns which want to dump to csv in comma separated format <coloumn_family:column_qualfier:Output datatype>,..> <output_directory> ");
			return ;
		}
		else
		{	
			String tbname = args[0];
			String [] tokens = args[1].split(",");
			FileWriter fw = new FileWriter(args[2]+".csv");
			PrintWriter out = new PrintWriter(fw);
			int rowcount = 0;
			Configuration config = HBaseConfiguration.create();//conf obj created for reading from hbase-site.xml
			HTable table = new HTable(config, tbname);
			//scanning all rows
			  Scan s = new Scan();
			  ResultScanner scanner = table.getScanner(s);
			try
			{
				for(Result rr = scanner.next(); rr != null; rr = scanner.next())
				{
					++rowcount;
				}
			}finally
			{
				   scanner.close();

			}
			for(int i =1;i<=rowcount;i++)
			{
				for(int j =0;j<tokens.length;j++)
					{
						String [] val= tokens[j].split(":");
//						System.out.println(val[0]+" "+val[1]+" "+val[2]);
						Get g = new Get(Bytes.toBytes(String.valueOf(i)));
						Result r = table.get(g);
						byte[] value = r.getValue(Bytes.toBytes(val[0]), Bytes.toBytes(val[1]));
						String valueStr = Bytes.toString(value);
				//		System.out.println(key+" "+val.substring(0, val.indexOf(':'))+" "+val.substring(val.indexOf(':')+1, val.length()));
						switch (val[2]) {
						case "Boolean":
						//	System.out.println(valueStr);
							System.out.println("adding input to file"+Boolean.parseBoolean(valueStr));
							out.print(Boolean.parseBoolean(valueStr));
							out.print(",");
							break;
						case "Double":
						//	System.out.println(valueStr);
							System.out.println("adding input to file "+ Double.parseDouble(valueStr));
							out.print(Double.parseDouble(valueStr));
							out.print(",");
							break;
						case "Float":
						//	System.out.println(valueStr);
							System.out.println("adding input to file "+ Float.parseFloat(valueStr));
							out.print(Float.parseFloat(valueStr));
							out.print(",");
							break;
						case "Integer":
						//	System.out.println(valueStr);
							System.out.println("adding input to file "+ Integer.parseInt(valueStr));
							out.print(Integer.parseInt(valueStr));
							out.print(",");
							break;
						case "Long":
						//	System.out.println(valueStr);
							System.out.println("adding input to file "+ Long.parseLong(valueStr));
							out.print(Long.parseLong(valueStr));
							out.print(",");
							break;
						case "String":
						//	System.out.println(valueStr);
							System.out.println("adding input to file "+ valueStr);
							out.print(valueStr);
							out.print(",");
							break;
						case "Short":
						//	System.out.println(valueStr);
							System.out.println("adding input to file "+ Short.parseShort(valueStr));
							out.print(Short.parseShort(valueStr));
							out.print(",");
							break;
						default:
							out.print(valueStr);
							out.print(",");
							break;
						}
						
					}
				out.println();
				out.flush();
					}
				out.close();
			}
			
	}

}
