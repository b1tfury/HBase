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

public class UpdateTable {
public static void main(String[] args) throws IOException {

        Configuration config = HBaseConfiguration.create();//conf obj created for reading from hbase-site.xml
        Scanner in = new Scanner(System.in);
        String tbname,cf,val,row_num,col_num;
        System.out.println("Enter the table name to be connected");
        tbname = in.next();
        HTable table = new HTable(config,tbname); // instantiate HTable object and connec with the table specified

        System.out.println("Enter the row number ");
        row_num = in.next();
        Put p = new Put(Bytes.toBytes(row_num)); // Put constructor is used to add a row

        System.out.println("Enter the col family and col number and cell value");
        cf = in.next();
        col_num = in.next();
        val  = in.next();
        p.add(Bytes.toBytes(cf), Bytes.toBytes(col_num),Bytes.toBytes(val)); //update the values columnfamily columnqualifier and value

        System.out.println("Flushing values to Hbase");

}
}
