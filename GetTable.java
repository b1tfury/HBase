import java.io.IOException;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class GetTable {
public static void main(String[] args) throws IOException {
Scanner in = new Scanner(System.in);
String tbname,row_name,col_name,cf,val;
Configuration config = HBaseConfiguration.create();
System.out.println("enter the table name to be searched");
tbname = in.next();
HTable table = new HTable(config, tbname);
System.out.println("enter the row name ");
row_name = in.next();
Get g = new Get(Bytes.toBytes(row_name));
Result r = table.get(g);
System.out.println("Enter the col family and col qualifier");
cf  = in.next();
col_name = in.next();
byte[] value = r.getValue(Bytes.toBytes(cf), Bytes.toBytes(col_name));
String valueStr = Bytes.toString(value);
System.out.println("GET: " + valueStr);
Scan s = new Scan();
s.addColumn(Bytes.toBytes(cf), Bytes.toBytes(col_name));
ResultScanner scanner = table.getScanner(s);
try {
for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
System.out.println("Found row: " + rr);
}

 for (Result rr : scanner) {
 System.out.println("Found row: " + rr);
 }
} finally {
scanner.close();
}
}
}
