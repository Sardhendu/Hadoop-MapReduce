package Hadoop_StudMarks_Writable;
import java.io.IOException;
import java.io.PrintStream;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class HSMW_reducer
  extends Reducer<HSMW_writable, NullWritable, Text, NullWritable>
{
  protected void reduce(HSMW_writable key, Iterable<NullWritable> value, Reducer<HSMW_writable, NullWritable, Text, NullWritable>.Context context)
    throws IOException, InterruptedException
  {
    System.out.println("I am inside the reducer aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    
    String a = key.getname();
    int b = key.getmarks();
    
    System.out.println("the name is " + a);
    System.out.println("the marks is " + b);
    
    String fin_key = a + " " + b;
    System.out.println(a);
    System.out.println(b);
    context.write(new Text(fin_key), null);
  }
}
