package NLine_Delimiter;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class NL_reducer
  extends MapReduceBase
  implements Reducer<Text, IntWritable, Text, IntWritable>
{
  public void reduce(Text key, Iterator<IntWritable> value, OutputCollector<Text, IntWritable> output, Reporter reporter)
    throws IOException
  {
    IntWritable value_1 = null;
    while (value.hasNext()) {
      value_1 = (IntWritable)value.next();
    }
    System.out.println("Inside the reducer function");
    
    output.collect(key, value_1);
  }
}
