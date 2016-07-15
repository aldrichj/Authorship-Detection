import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by jeremy on 4/1/16.
 */
public class CosineReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        String unknownAuth = conf.get("uAuthor");


        boolean flag = false;
        double denomUnknown = -3.0;

        ArrayList<String> data = new ArrayList<>();

        Map<String,String> uniqueAuthor = new HashMap<>();


        // (word),(author,summed numerator,rooted auth denominator)
        for(Text value:values){
            String[] line = value.toString().split("#");

            String author = line[0];
            String numerator = line[1];
            double denomAuthor = Double.parseDouble(line[2]);


            if(!flag && unknownAuth.equals(author.trim())){
                denomUnknown = denomAuthor;


                flag = true;
            }

            //m


            data.add(author + "#" + numerator + "#" + denomAuthor);

        }
        // removes words that aren't in the unknown authors list
        if(flag) {
            for (String entry : data) {
                String[] item = entry.split("#");
                String author = item[0];


                if (!unknownAuth.equals(author.trim()) ) {

                    double numerator = Double.parseDouble(item[1]);
                    double denomAuthor = Double.parseDouble(item[2]);
                    double denominator = denomAuthor * denomUnknown;
                    double cosine = numerator / denominator;

                    //System.out.println(numerator+" "+denomAuthor+" "+denomUnknown+" "+denominator);

                    // (word,author),(numerator,denominator)

                    context.write(new Text(author), new Text("#" + cosine));


                }
            }
        }
    }
}
