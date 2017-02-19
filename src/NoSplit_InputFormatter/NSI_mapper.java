package NoSplit_InputFormatter;

import java.io.IOException;
import java.io.PrintStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class NSI_mapper
  extends MapReduceBase
  implements Mapper<NullWritable, Text, Text, NullWritable>
{
  NullWritable null_val = NullWritable.get();
  
  public void map(NullWritable key, Text value, OutputCollector<Text, NullWritable> output, Reporter reporter)
    throws IOException
  {
    System.out.println("Inside the mapper class");
    
    String[] tokens = value.toString().split("> ");
    
    StringBuilder line_string = new StringBuilder();
    for (int i = 0; i < tokens.length; i++)
    {
      int cntr_line = i + 1;
      System.out.println("the word is aaaaaaaaaaaaaaaaaaaaaaaaaaaaa  ");
      System.out.println(tokens[i]);
      Pattern pattern = Pattern.compile("\\<(.+)", 32);
      Matcher matcher = pattern.matcher(tokens[i]);
      while (matcher.find())
      {
        if (((matcher.group(1).contains("2014")) || (matcher.group(1).contains("2013"))) && ((matcher.group(1).contains("AM")) || (matcher.group(1).contains("PM"))) && (matcher.group(1).contains("CST")) && (cntr_line != 1))
        {
          System.out.println("The complete String Buffer output is bbbbbbbbbbbbbbbbbbb");
          System.out.println(line_string.toString());
          output.collect(new Text(line_string.toString().substring(0, line_string.length() - 1)), this.null_val);
          line_string.setLength(0);
          System.out.println("The the string builder is empty is it ???? " + line_string);
        }
        line_string.append(matcher.group(1) + "*");
      }
    }
  }
}