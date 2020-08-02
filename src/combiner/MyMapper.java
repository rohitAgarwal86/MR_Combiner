package combiner;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	Text outkey = new Text();
	IntWritable outvalue = new IntWritable();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] words = value.toString().split(",");
		for (String a : words) {
			outkey.set(a);
			outvalue.set(1);
			context.write(outkey, outvalue);
		}
	}

}
