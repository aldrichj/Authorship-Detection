import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by aldrichj on 3/31/16.
 */
public class NGramReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values,
                       Context context)
            throws IOException, InterruptedException {

        int sum = 0;

        for (Text value : values) {

            sum += Integer.parseInt(value.toString());
        }

        //(word, author),(count)
        context.write(new Text(key), new Text("#"+Integer.toString(sum)));

    }


}
