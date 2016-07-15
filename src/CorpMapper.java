import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by aldrichj on 3/31/16.
 */
public class CorpMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // (word,author),(count,max)

        String[] line = value.toString().split("#");
        String word = line[0];
        String author = line[1];
        String count = line[2];
        String max = line[3];



        // (word),(author,count,max)
        context.write(new Text(word),new Text(author+"#"+count+"#"+max));




    }
}
