import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by jeremy on 4/1/16.
 */
public class CalcMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // (word,author),(TFIDF,numerator)

        String[] line = value.toString().split("#");

        String word = line[0];
        String author = line[1];
        String TFIDF = line[2];
        String numerator = line[3];



        // (author),(word,TFIDF,numerator)
        context.write(new Text(author),new Text(word+"#"+TFIDF+"#"+numerator));


    }
}
