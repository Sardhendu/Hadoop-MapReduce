package NLine_Delimiter;

import java.io.IOException;
import java.io.PrintStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class NL_mapper
  extends MapReduceBase
  implements Mapper<Text, IntWritable, Text, IntWritable>
{
  public void map(Text key, IntWritable value, OutputCollector<Text, IntWritable> output, Reporter reporter)
    throws IOException
  {
    System.out.println("Inside the mapper class");
    System.out.println("the key is " + key.toString());
    System.out.println("the value is " + Integer.parseInt(value.toString()));
    output.collect(key, value);
  }
}
