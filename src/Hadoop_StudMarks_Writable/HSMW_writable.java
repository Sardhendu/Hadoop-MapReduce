package Hadoop_StudMarks_Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import org.apache.hadoop.io.WritableComparable;

public class HSMW_writable
  implements WritableComparable<HSMW_writable>
{
  private int marks;
  private String name;
  
  public void setnamemarks(String name, int marks)
  {
    System.out.println("setname method of writable class the name is     " + name);
    this.name = name.toString();
    this.marks = marks;
  }
  
  public String getname()
  {
    System.out.println("returning the string name");
    return this.name;
  }
  
  public int getmarks()
  {
    System.out.println("returning the int marks");
    return this.marks;
  }
  
  public void readFields(DataInput input)
    throws IOException
  {
    this.marks = input.readInt();
    this.name = input.readUTF();
    System.out.println("readfields method of writable class and the name is   " + this.name);
    System.out.println("readfields method of writable class and the marks is   " + this.marks);
  }
  
  public void write(DataOutput output)
    throws IOException
  {
    output.writeInt(this.marks);
    output.writeUTF(this.name);
    System.out.println("write method of writable class and the name is    " + this.name);
    System.out.println("write method of writable class and the marks is    " + this.marks);
  }
  
  public int compareTo(HSMW_writable a)
  {
    System.out.println("Inside the compare_to method of HSMW_writable");
    System.out.println("this.name is " + this.name + " and a.name is " + a.getname());
    int val = this.name.compareTo(a.getname());
    System.out.println("The val after comparision is " + val);
    if (val == 0)
    {
      System.out.println("this.name is " + this.name + " and a.name is " + a.getname());
      if (a.getmarks() < this.marks)
      {
        System.out.println("this.marks is " + this.marks + " and a.marks is " + a.getmarks());
        return 1;
      }
      if (a.getmarks() > this.marks)
      {
        System.out.println("this.marks is " + this.marks + " and a.marks is " + a.getmarks());
        return -1;
      }
      return 0;
    }
    return val;
  }
}
