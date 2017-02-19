package MultiLine_InfutFormatter;

import java.io.IOException;
import java.io.PrintStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class MLI_inputformatter
  extends FileInputFormat<IntWritable, Text>
{
  public boolean isSplitable(FileSystem fs, Path filename)
  {
    System.out.println("Inside the isSplitable Method of NSI_inputformatter");
    return false;
  }
  
  public RecordReader<IntWritable, Text> getRecordReader(InputSplit split, JobConf job_run, Reporter reporter)
    throws IOException
  {
    reporter.setStatus(split.toString());
    return new MLI_record_reader(job_run, (FileSplit)split);
  }
}
