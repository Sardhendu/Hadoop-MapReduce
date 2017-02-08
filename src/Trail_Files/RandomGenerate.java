package Trail_Files;

import java.awt.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;

public class RandomGenerate {
	
	private static class hitme {
		
		private int totalNum = 0;
		private TreeMap<Integer, String> recordHolder = new TreeMap<Integer, String>();
		
		public void sample(){
			Random rands = new Random();
			
			for (int i=0; i<=111; i++){
//				System.out.println(rands.nextInt());
				int randnum = rands.nextInt();
				recordHolder.put(randnum, "sam");
				totalNum += 1;
			}
		}
		
		protected void example(){
			int sampleNum = (int) (totalNum * 0.1);
			int indexNum = 0;
			for(Entry<Integer, String> entry : recordHolder.entrySet()) {
				// Using the if condition we only let the first 10% go through the MapOutput
				if (indexNum<sampleNum){
					System.out.println(entry.getKey());
					System.out.println(entry.getValue());
				}
				else{
					break;
				}
				indexNum += 1;
		                
		    }
		}
	}
	
	public static void main (String[] args){
		hitme obj = new hitme();
		obj.sample();
		obj.example();
		
	}
}

