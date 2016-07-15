import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by aldrichj on 3/31/16.
 */
public class MaxWordReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {


        ArrayList<String> data = new ArrayList<String>();
        // number of documents a word appears in
        int maxCount = 0;

        //author,(word,count)
        for (Text value : values) {
            String[] token = value.toString().split("#");

            String word = token[0];
            // word occurrences for author
            String count = token[1];


            if (maxCount < Integer.parseInt(count)) {
                maxCount = Integer.parseInt(count);
            }

            data.add(word + "#" + count);



        }

        for (String line : data) {
            String[] toks = line.split("#");
            String word = toks[0];
            String totalCount = toks[1];


            //author,(word,count,max)
            context.write(new Text(word + "#" + key), new Text("#"+totalCount + "#" + maxCount));
        }


    }
}
