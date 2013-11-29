import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

import org.apache.hadoop.io.Writable;


public class CombinedValue implements Writable, Comparable<CombinedValue>{

	private int article_id;
	private int count;

	public CombinedValue(){
		super();
	}
	
	public CombinedValue(int a, int c){
		super();
		this.article_id = a;
		this.count = c;
	}
	
	public int getArticle_id() {
		return article_id;
	}

	public void setArticle_id(int article_id) {
		this.article_id = article_id;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		count = in.readInt();
		article_id = in.readInt();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(count);
        out.writeLong(article_id);
		
	}

	@Override
	public int compareTo(CombinedValue arg0) {
//		int compareCount = ((CombinedValue) arg0).getCount(); 
//		 
//		//ascending order
//		int cmp =  this.count - compareCount;
//		if (cmp!=0){
//			return cmp;
//		}
//		long compareArt = ((CombinedValue) arg0).getArticle_id();
//		return (int) (this.article_id - compareArt);
//		//descending order
//		//return compareQuantity - this.quantity;
		if (this.count > arg0.getCount()) 
			return 1;
		if (this.count < arg0.getCount()) 
			return -1;
	    if (this.article_id > arg0.getArticle_id())
	    	return 1;
	    return -1;
	    	
	}
	
	public static Comparator<CombinedValue> CombinedValueComparator 
    = new Comparator<CombinedValue>() {

		public int compare(CombinedValue c1, CombinedValue c2) {
//
//			String fruitName1 = fruit1.getFruitName().toUpperCase();
//			String fruitName2 = fruit2.getFruitName().toUpperCase();

			//ascending order
			return c1.compareTo(c2);

			//descending order
			//return fruitName2.compareTo(fruitName1);
		}
	};
	
	@Override
	public String toString(){
		return (article_id + " " + count);
	}

}
