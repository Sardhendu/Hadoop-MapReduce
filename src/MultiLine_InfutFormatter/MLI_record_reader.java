package MultiLine_InfutFormatter;


import java.io.IOException;
import java.io.PrintStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;

public class MLI_record_reader
  implements RecordReader<IntWritable, Text>
{
  private LineRecordReader linereader;
  private LongWritable linekey;
  private Text linevalue;
  
  public MLI_record_reader(JobConf job_run, FileSplit split)
    throws IOException
  {
    System.out.println("Inside the MLI_record_reader constructor");
    this.linereader = new LineRecordReader(job_run, split);
  }
  
  public boolean next(IntWritable key, Text value)
    throws IOException, NullPointerException
  {
    System.out.println("Inside the next function");
    StringBuilder string_value = new StringBuilder();
    int line = 0;
    for (int i = 0; i < 2; i++)
    {
      System.out.println("The value of i is " + i);
      System.out.println("The value of i is " + i);
      System.out.println("inside the for loop");
      
      System.out.println("satart creation");
      this.linekey = this.linereader.createKey();
      System.out.println("created key");
      this.linevalue = this.linereader.createValue();
      System.out.println("created valuerrrr " + this.linevalue.toString());
      if (!this.linereader.next(this.linekey, this.linevalue))
      {
        System.out.println("Inside to return false");
        
        return false;
      }
      string_value.append(this.linevalue.toString());
      System.out.println("The string After one release is " + string_value.toString());
      line = i + 1;
    }
    System.out.println("the original string_value is " + string_value.toString());
    key.set(line);
    value.set(string_value.toString());
    return true;
  }
  
  public void close()
    throws IOException
  {
    this.linereader.close();
  }
  
  public IntWritable createKey()
  {
    System.out.println("Inside the create key");
    return new IntWritable();
  }
  
  public Text createValue()
  {
    System.out.println("Inside the create value");
    return new Text(" ");
  }
  
  public long getPos()
    throws IOException
  {
    return this.linereader.getPos();
  }
  
  public float getProgress()
    throws IOException
  {
    return this.linereader.getProgress();
  }
}
