package avgTemp;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgTemperatureReducer
extends Reducer<Text, IntWritable, Text, IntWritable> {

@Override
public void reduce(Text key, Iterable<IntWritable> values,
   Context context)
   throws IOException, InterruptedException {
	int tempSum = 0;
	int count = 0;
	int tempAverage = 0;
	
	for (IntWritable value : values) {
		tempSum += value.get();
        count++;
    }
	
	tempAverage = tempSum / count;
    context.write(key, new IntWritable(tempAverage));
}
}

