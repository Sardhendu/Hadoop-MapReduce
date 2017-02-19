package NoSplit_InputFormatter;

import java.io.IOException;
import java.io.PrintStream;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

public class NSI_record_reader
  implements RecordReader<NullWritable, Text>
{
  FileSplit split;
  JobConf job_run;
  String text;
  public boolean processed = false;
  
  public NSI_record_reader(JobConf job_run, FileSplit split)
  {
    this.split = split;
    this.job_run = job_run;
  }
  
  /* Error */
  public boolean next(NullWritable key, Text value)
    throws IOException
  {
    // Byte code:
    //   0: aload_0
    //   1: getfield 21	com/nosplit_inptfrmtr/NSI_record_reader:processed	Z
    //   4: ifne +119 -> 123
    //   7: aload_0
    //   8: getfield 23	com/nosplit_inptfrmtr/NSI_record_reader:split	Lorg/apache/hadoop/mapred/FileSplit;
    //   11: invokevirtual 36	org/apache/hadoop/mapred/FileSplit:getLength	()J
    //   14: l2i
    //   15: newarray <illegal type>
    //   17: astore_3
    //   18: aload_0
    //   19: getfield 23	com/nosplit_inptfrmtr/NSI_record_reader:split	Lorg/apache/hadoop/mapred/FileSplit;
    //   22: invokevirtual 42	org/apache/hadoop/mapred/FileSplit:getPath	()Lorg/apache/hadoop/fs/Path;
    //   25: astore 4
    //   27: aload 4
    //   29: aload_0
    //   30: getfield 25	com/nosplit_inptfrmtr/NSI_record_reader:job_run	Lorg/apache/hadoop/mapred/JobConf;
    //   33: invokevirtual 46	org/apache/hadoop/fs/Path:getFileSystem	(Lorg/apache/hadoop/conf/Configuration;)Lorg/apache/hadoop/fs/FileSystem;
    //   36: astore 5
    //   38: aconst_null
    //   39: astore 6
    //   41: aload 5
    //   43: aload 4
    //   45: invokevirtual 52	org/apache/hadoop/fs/FileSystem:open	(Lorg/apache/hadoop/fs/Path;)Lorg/apache/hadoop/fs/FSDataInputStream;
    //   48: astore 6
    //   50: getstatic 58	java/lang/System:out	Ljava/io/PrintStream;
    //   53: new 64	java/lang/StringBuilder
    //   56: dup
    //   57: ldc 66
    //   59: invokespecial 68	java/lang/StringBuilder:<init>	(Ljava/lang/String;)V
    //   62: aload 6
    //   64: invokevirtual 71	java/lang/StringBuilder:append	(Ljava/lang/Object;)Ljava/lang/StringBuilder;
    //   67: aload 6
    //   69: invokevirtual 75	java/lang/Object:toString	()Ljava/lang/String;
    //   72: invokevirtual 79	java/lang/StringBuilder:append	(Ljava/lang/String;)Ljava/lang/StringBuilder;
    //   75: invokevirtual 82	java/lang/StringBuilder:toString	()Ljava/lang/String;
    //   78: invokevirtual 83	java/io/PrintStream:println	(Ljava/lang/String;)V
    //   81: aload 6
    //   83: aload_3
    //   84: iconst_0
    //   85: aload_3
    //   86: arraylength
    //   87: invokestatic 88	org/apache/hadoop/io/IOUtils:readFully	(Ljava/io/InputStream;[BII)V
    //   90: aload_2
    //   91: aload_3
    //   92: iconst_0
    //   93: aload_3
    //   94: arraylength
    //   95: invokevirtual 94	org/apache/hadoop/io/Text:set	([BII)V
    //   98: goto +13 -> 111
    //   101: astore 7
    //   103: aload 6
    //   105: invokestatic 100	org/apache/hadoop/io/IOUtils:closeStream	(Ljava/io/Closeable;)V
    //   108: aload 7
    //   110: athrow
    //   111: aload 6
    //   113: invokestatic 100	org/apache/hadoop/io/IOUtils:closeStream	(Ljava/io/Closeable;)V
    //   116: aload_0
    //   117: iconst_1
    //   118: putfield 21	com/nosplit_inptfrmtr/NSI_record_reader:processed	Z
    //   121: iconst_1
    //   122: ireturn
    //   123: iconst_0
    //   124: ireturn
    // Line number table:
    //   Java source line #33	-> byte code offset #0
    //   Java source line #35	-> byte code offset #7
    //   Java source line #36	-> byte code offset #18
    //   Java source line #37	-> byte code offset #27
    //   Java source line #38	-> byte code offset #38
    //   Java source line #43	-> byte code offset #41
    //   Java source line #44	-> byte code offset #50
    //   Java source line #45	-> byte code offset #81
    //   Java source line #46	-> byte code offset #90
    //   Java source line #47	-> byte code offset #98
    //   Java source line #49	-> byte code offset #101
    //   Java source line #50	-> byte code offset #103
    //   Java source line #52	-> byte code offset #108
    //   Java source line #50	-> byte code offset #111
    //   Java source line #53	-> byte code offset #116
    //   Java source line #54	-> byte code offset #121
    //   Java source line #57	-> byte code offset #123
    // Local variable table:
    //   start	length	slot	name	signature
    //   0	125	0	this	NSI_record_reader
    //   0	125	1	key	NullWritable
    //   0	125	2	value	Text
    //   17	77	3	content_add	byte[]
    //   25	19	4	file	org.apache.hadoop.fs.Path
    //   36	6	5	fs	org.apache.hadoop.fs.FileSystem
    //   39	73	6	input	org.apache.hadoop.fs.FSDataInputStream
    //   101	8	7	localObject	Object
    // Exception table:
    //   from	to	target	type
    //   41	101	101	finally
  }
  
  public void close()
    throws IOException
  {}
  
  public NullWritable createKey()
  {
    System.out.println("Inside createkey() mrthod of NSI_record_reader");
    
    return NullWritable.get();
  }
  
  public Text createValue()
  {
    System.out.println("Inside createValue() mrthod of NSI_record_reader");
    
    return new Text();
  }
  
  public long getPos()
    throws IOException
  {
    System.out.println("Inside getPs() mrthod of NSI_record_reader");
    return this.processed ? this.split.getLength() : 0L;
  }
  
  public float getProgress()
    throws IOException
  {
    System.out.println("Inside getProgress() mrthod of NSI_record_reader");
    return this.processed ? 1.0F : 0.0F;
  }
}
