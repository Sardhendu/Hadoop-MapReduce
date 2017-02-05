package Hadoop_StudMarks_Writable;

import java.io.IOException;
import java.io.PrintStream;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class HSMW_mapper
  extends Mapper<LongWritable, Text, HSMW_writable, NullWritable>
{
  NullWritable null_val = NullWritable.get();
  HSMW_writable composite_val = new HSMW_writable();
  
  protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, HSMW_writable, NullWritable>.Context context)
    throws IOException, InterruptedException
  {
    String[] tokens = value.toString().split(",");
    
    String name = tokens[0];
    
    int marks = Integer.parseInt(tokens[2]);
    
    System.out.println("name is " + name + " and a.marks is " + marks);
    this.composite_val.setnamemarks(name, marks);
    
    context.write(this.composite_val, this.null_val);
  }
}
