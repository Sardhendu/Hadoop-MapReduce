package MultiLine_InfutFormatter;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MLI_reducer
  extends MapReduceBase
  implements Reducer<Text, IntWritable, Text, IntWritable>
{
  public void reduce(Text key, Iterator<IntWritable> value, OutputCollector<Text, IntWritable> output, Reporter reporter)
    throws IOException
  {
    System.out.println("Satrting the reduce work");
    System.out.println(key.toString());
    
    output.collect(key, (IntWritable)value.next());
  }
}
