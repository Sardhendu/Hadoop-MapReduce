package NoSplit_InputFormatter;



import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;

public class Read_data {
  public static void main(String[] args)
    throws IOException
  {
    String name = "sammmy*";
    
    System.out.println(name.substring(0, name.length() - 1));
    
    String value = "abcd 2013 ndnd23 AM akaksk CST";
    String value_1 = "aascdfghjkjhgfdscvbnkiuytrd";
    if (((value.contains("2014")) || (value.contains("2013"))) && (value.contains("AM")) && (value.contains("CST"))) {
      System.out.println("the value is " + value);
    }
    if (value.matches(".*[2014 AM CST].*")) {
      System.out.println(value);
    } else {
      System.out.println("nope");
    }
    BufferedReader br = new BufferedReader(new FileReader("/home/hduser/in_sort"));
    System.out.println("ballu");
    StringBuilder sb = new StringBuilder();
    System.out.println("gallu");
    String line = br.readLine();
    System.out.println("jallu");
    int i = 0;
    while (line != null)
    {
      System.out.println(i + 1);
      sb.append(line);
      line = br.readLine();
    }
    System.out.println(sb.toString());
  }
}
