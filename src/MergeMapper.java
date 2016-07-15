import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by jeremy on 4/1/16.
 */
public class MergeMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // (word,author),(TF,IDF)
        String[] line = value.toString().split("#");

        String word = line[0];
        String author = line[1];
        String TF = line[2];
        String IDF = line[3];

        // (word),(author,TF,IDF)
        context.write(new Text(word), new Text(author+"#"+TF+"#"+IDF));

    }
}
