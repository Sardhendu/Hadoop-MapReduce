package Trail_Files;

import java.util.Arrays;

public class PRAC1 {
	public static final String value = "Sam,1,4,3,2";
	
//	@SuppressWarnings("null")
	public static void main(String args[]){
//		String fnlToken = new String();
		String token_arr = new String(value.toString());
		String[] token_split = token_arr.split(",");
		int[] fields = new int[token_split.length -1];
		String name = token_split[0];
		
		System.out.print(name);
		
		int i = 0;
		for (String tkn: token_split){
			if (i!=0){
				int field_value = Integer.parseInt(tkn);
				fields[i-1] = field_value;
			}
			i+=1;
		}
		
		if (fields[0] < fields[2] && fields[1] > fields[3]){
			System.out.println(Arrays.toString(fields));
//			String.join(",", fields);
		}
		
//		for 
//		for (String token: ){
//			System.out.println(token);
//			System.out.println(i);
//			if (i==0){
//				String name
//			}
//			else
//			i+=1;
//		}
//		
	}

}


