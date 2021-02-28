import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.*;





public class QueryStat extends Configured implements Tool {
    public static Path qid_path = new Path("/user/vakulenko.yulia/data/queries.tsv");

    public static class QueryStatMapper extends Mapper<LongWritable, Text, Text, Text> {
        // url --> id
        public static Map<String, String> url_id = new HashMap<>();
        public static Map<String, String> id_url = new HashMap<>();
        // query text --> query id
        public static Map<String, String> t_qid = new HashMap<>();
        public static Map<String, String> qid_t = new HashMap<>();

        public static void query_extractor()
        {
            try {
                FileSystem fs = FileSystem.get(new Configuration());
                FileStatus[] statuses = fs.globStatus(qid_path);
                for (FileStatus status : statuses)
                {
                    BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status.getPath()), StandardCharsets.UTF_8));
                    String line;
                    line=br.readLine();
                    while (line != null){
                        String[] args = line.split("\t");
                        String id = args[0];
                        t_qid.put(args[1],id);
                        qid_t.put(id, args[1]);
                        line=br.readLine();
                    }
                }
            }
            catch (Exception ignored){}
        }

        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            query_extractor();
        }
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            // query
            String[] query_temp = line[0].split("@");
            // query text
            String query_text = t_qid.getOrDefault(query_temp[0], "-1");
            if (query_text.equals("-1")){
                return;
            }
            // click map
            HashMap<String,Integer> cmap = new HashMap<>();
            // link map
            HashMap<String,Integer> lmap = new HashMap<>();
            // shown links
            String[] rawlinks = line[1].split(",");
            // clicked links
            List<String> rawclicks = Arrays.asList(line[2].split(","));
            // list with links
            List<String> llinks = new LinkedList<>();
            // list with clicked links
            List<String> lclicked = new LinkedList<>();
            // raw time stamps
            String[] rawts = line[3].split(",");
            // list with time differences between clicks
            List<Double> lts = new LinkedList<>();

            for (int i = 1; i < rawts.length ; i++){
                lts.add((double) ((Long.parseLong(rawts[i].substring(5)) - Long.parseLong(rawts[i-1].substring(5)))/1000));
            }
            lts.add(300.0);

            for (String rawlink : rawlinks) {

                String temp = rawlink.startsWith("http://") ? rawlink.substring(7) : rawlink;
                temp = temp.startsWith("www.") ? temp.substring(4) : temp;
                llinks.add(temp);

            }

            for(int i = 0; i < rawclicks.size(); i++){
                
                String temp = rawclicks.get(i).startsWith("http://") ? rawclicks.get(i).substring(7) : rawclicks.get(i);
                temp = temp.startsWith("www.") ? temp.substring(4) : temp;

                lclicked.add(temp);

            }
            // shown links
            String[] links = new String[llinks.size()];
            for(int i = 0; i < llinks.size(); i++){
                links[i] = llinks.get(i);
                lmap.put(links[i],i);
            }
            // clicked links
            String[] clicked = new String[lclicked.size()];
            for(int i = 0; i < lclicked.size(); i++){
                clicked[i] = lclicked.get(i);
                cmap.put(clicked[i],i);
            }
            // time stamps
            Double[] ts = new Double[lts.size()];
            for(int i = 0; i < lts.size(); i++){
                ts[i] = lts.get(i);
            }
            // out string
            StringBuilder out = new StringBuilder();
            // num of links
            out.append(links.length).append("\t");
            // num of clicks
            out.append(clicked.length).append("\t");
            // ts diff
            if(ts.length > 0) {
                double maxts = Collections.max(lts);
                double mints = Collections.min(lts);
                out.append(maxts - mints + 300).append("\t");
            }else{
                out.append(0).append("\t");
            }
            // clicks at pos from 1 to 10
            Integer[] click_pos_ar = new Integer[10];
            Arrays.fill(click_pos_ar,0);
            for (int i = 0; i < 10 && i< links.length; i++){
                if(cmap.containsKey(links[i])){
                    int pos  = cmap.get(links[i]);
                    if(pos < 10) {
                        click_pos_ar[pos] += 1;
                    }
                }
            }
            for(int i = 0; i < 10; i++){
                out.append(click_pos_ar[i]).append("\t");
            }
            // last clicked and first clicked
            if (clicked.length > 0) {
                out.append(lmap.get(clicked[0])).append("\t");
                out.append(lmap.get(clicked[clicked.length - 1])).append("\t");
            }else {
                out.append(0).append("\t");
                out.append(0).append("\t");
            }
            // average ts diff
            double avg_tsdif = 0.0;
            if (ts.length > 1) {
                for (int i = 1; i < ts.length; i++) {
                    avg_tsdif += ts[i] - ts[i - 1];
                }
                out.append(avg_tsdif / (ts.length - 1));
            }else{
                out.append(0);
            }
            context.write(new Text(query_text), new Text(out.toString()));
        }

    }

    public static class QueryStatReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            
            Double[] stats = new Double[9];
            Arrays.fill(stats,0.0);
            Double[] clicks_in_pos = new Double[10];
            Arrays.fill(clicks_in_pos, 0.0);
            //num of links, num of clicks, ts diff, clicks at pos 1 -10, first clicked, last clicked, average ts
            for(Text value: values){
                String[] line = value.toString().split("\t");
                stats[0] += 1;
                // num of links
                stats[1] += Double.parseDouble(line[0]);
                // num of clicked links
                stats[2] += Double.parseDouble(line[1]);
                // ts diff
                stats[3] += Double.parseDouble(line[2]);
                for(int i = 0; i<10; i++){
                    clicks_in_pos[i] += Double.parseDouble(line[i+3]);
                }
                // first clicked
                stats[4] += Double.parseDouble(line[13]);
                // last clicked
                stats[5] += Double.parseDouble(line[14]);
                // average ts
                stats[6] += Double.parseDouble(line[15]);
                // no clicks
                if(Double.parseDouble(line[2]) == 0){
                    stats[7] += 1;
                }
                // one click
                if(Double.parseDouble(line[2]) == 1){
                    stats[8] += 1;
                }
            }
            StringBuilder out = new StringBuilder();
            // how many times any links were clicked
            double click_any_times = (stats[0] - stats[7]);
            if(click_any_times == 0){
                // appearence of query
                out.append(stats[0]).append("\t");
                // mean links shown
                out.append(stats[1]/stats[0]).append("\t");
                // mean links clicked
                out.append(stats[2]/stats[0]).append("\t");
                out.append(0).append("\t");//
                out.append(0).append("\t");
                // num of cliks at pos 1-10
                for(int i = 0; i < 10; i++){
                    out.append(0).append("\t");
                }
                out.append(0).append("\t");
                out.append(0).append("\t");
                out.append(stats[6]/click_any_times).append("\t");
                out.append(stats[7]).append("\t");
                out.append(stats[7]/stats[0]).append("\t");
                out.append(stats[8]).append("\t");
                out.append(stats[8]/stats[0]).append("\t");
                out.append(0);
                context.write(key, new Text(out.toString()));
                return;
            }
            // appearence of query
            out.append(stats[0]).append("\t");
            // mean links shown
            out.append(stats[1]/stats[0]).append("\t");
            // mean links clicked
            out.append(stats[2]/stats[0]).append("\t");
            // mean links clicked
            out.append(stats[2]/click_any_times).append("\t");
            // mean ts diff
            out.append(stats[3]/click_any_times).append("\t");
            // mean first click pos
            for(int i = 0; i < 10; i++){
                out.append(clicks_in_pos[i]/click_any_times).append("\t");
            }
            // mean first click pos
            out.append(stats[4]/click_any_times).append("\t");
            // mean first click pos
            out.append(stats[5]/click_any_times).append("\t");
            // mean average ts
            out.append(stats[6]/click_any_times).append("\t");
            // no click
            out.append(stats[7]).append("\t");
            out.append(stats[7]/stats[0]).append("\t");
            // one click
            out.append(stats[8]).append("\t");
            out.append(stats[8]/stats[0]).append("\t");
            out.append(stats[8]/click_any_times);
            context.write(key, new Text(out.toString()));
        }
    }
    private Job getJobConf(final String input, final String output) throws IOException {

        final Job job = Job.getInstance(getConf());
        job.setJarByClass(QueryStat.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setJobName(QueryStat.class.getCanonicalName());
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(output));
        FileInputFormat.addInputPath(job, new Path(input));
        job.setMapperClass(QueryStatMapper.class);
        job.setReducerClass(QueryStatReducer.class);
        job.setNumReduceTasks(1);
        return job;
    }

    @Override
    public int run(final String[] args) throws Exception {
        FileSystem fs = FileSystem.get(getConf());
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }
        final Job job = getJobConf(args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(final String[] args) throws Exception {
        final int ret = ToolRunner.run(new QueryStat(), args);
        System.exit(ret);
    }
}

