package NLine_Delimiter;

import java.io.IOException;
import java.io.PrintStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class NL_inputformatter
  extends FileInputFormat<Text, IntWritable>
{
  public RecordReader<Text, IntWritable> getRecordReader(InputSplit input, JobConf job_run, Reporter reporter)
    throws IOException
  {
    System.out.println("I am Inside the NL_inputformatter class");
    reporter.setStatus(input.toString());
    return new NL_record_reader(job_run, (FileSplit)input);
  }
}
