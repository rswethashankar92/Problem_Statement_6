import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TitleBrandMap extends Mapper<LongWritable,Text,Text,NullWritable> {
	
	public void setup(Context context){
		try {
			context.write(new Text("title"), NullWritable.get());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void map(LongWritable key,Text value,Context context){
		if(Long.parseLong(key.toString()) == 0)
			return;
		else{
			String[] val = value.toString().split(",");
			String title = val[0];
			String brand = val[1];
			String store = val[2];
			String in_stock = val[4];
			
			if(in_stock.equals("false") && ((Integer.parseInt(brand) == 5) || (Integer.parseInt(store)== 2))){
				try {
					context.write(new Text(title), NullWritable.get());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}


}
