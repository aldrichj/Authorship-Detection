import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by jeremy on 4/1/16.
 */
public class CosineMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // (word,author),(Summed numerator,rooted author denominator)


        String[] line = value.toString().split("#");

        String word = line[0];
        String author = line[1];
        String numerator = line[2];
        String author_denom = line[3];




        // (word),(author,summed numerator,rooted auth denominator)
        context.write(new Text(word), new Text(author + "#" + numerator + "#" + author_denom));




    }
}
