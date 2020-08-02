package combiner;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	IntWritable outvalue = new IntWritable();

	public void reduce(Text key, Iterable<IntWritable> value, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : value) {
			sum = sum + val.get();
		}
		outvalue.set(sum);
		context.write(key, outvalue);

	}

}
