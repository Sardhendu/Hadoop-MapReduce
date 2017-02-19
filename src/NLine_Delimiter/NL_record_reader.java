package NLine_Delimiter;

import java.io.IOException;
import java.io.PrintStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;

public class NL_record_reader
  implements RecordReader<Text, IntWritable>
{
  private LineRecordReader lineReader;
  private LongWritable lineKey;
  private Text lineValue;
  
  public NL_record_reader(JobConf job_run, FileSplit input)
    throws IOException
  {
    System.out.println("I am inside the NL_record_reader constructor");
    this.lineReader = new LineRecordReader(job_run, input);
    this.lineKey = this.lineReader.createKey();
    this.lineValue = this.lineReader.createValue();
    System.out.println("lineReader is " + this.lineReader);
    System.out.println("lineKey is " + this.lineKey.toString());
    System.out.println("lineValue is " + this.lineValue.toString());
  }
  
  public boolean next(Text key, IntWritable value)
    throws IOException
  {
    System.out.println("Inside the next method of NL_record_reader");
    if (!this.lineReader.next(this.lineKey, this.lineValue)) {
      return false;
    }
    System.out.println("The initial value in is " + this.lineValue.toString());
    String[] tokens = this.lineValue.toString().split(",");
    key.set(tokens[0].trim());
    value.set(Integer.parseInt(tokens[1].trim()));
    System.out.println("The key is " + tokens[0].trim());
    System.out.println("The Value is " + Integer.parseInt(tokens[1].trim()));
    
    return true;
  }
  
  public Text createKey()
  {
    System.out.println("Inside the cretaeKey method of NL_record_reader");
    
    return new Text("");
  }
  
  public IntWritable createValue()
  {
    System.out.println("Inside the createValue method of NL_record_reader");
    
    return new IntWritable();
  }
  
  public long getPos()
    throws IOException
  {
    System.out.println("Inside the getPos method of NL_record_reader");
    return this.lineReader.getPos();
  }
  
  public void close()
    throws IOException
  {
    System.out.println("Inside the close method of NL_record_reader");
    this.lineReader.close();
  }
  
  public float getProgress()
    throws IOException
  {
    System.out.println("Inside the getProgress method of NL_record_reader");
    return this.lineReader.getProgress();
  }
}
}
