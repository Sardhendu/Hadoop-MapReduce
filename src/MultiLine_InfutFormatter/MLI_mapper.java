package MultiLine_InfutFormatter;

import java.io.IOException;
import java.io.PrintStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MLI_mapper
  extends MapReduceBase
  implements Mapper<IntWritable, Text, Text, IntWritable>
{
  public void map(IntWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
    throws IOException
  {
    System.out.println("Inside the mapper class");
    System.out.println("the value is " + value.toString());
    
    output.collect(value, new IntWritable(1));
  }
}
