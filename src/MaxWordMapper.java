import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by aldrichj on 3/31/16.
 */
public class MaxWordMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        //(word,author),(count)
        String[] toks = value.toString().split("#");

        //author,(word,count)
        context.write(new Text(toks[1]), new Text(toks[0] + "#" +toks[2]));

    }
}
