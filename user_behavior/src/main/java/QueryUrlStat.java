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
import java.nio.charset.StandardCharsets;
import java.util.*;





public class QueryUrlStat extends Configured implements Tool {
    public static Path url_path = new Path("/user/vakulenko.yulia/data/url.data");
    public static Path qid_path = new Path("/user/vakulenko.yulia/data/queries.tsv");


    public static class QueryUrlStatMapper extends Mapper<LongWritable, Text, Text, Text> {
        // url --> id
        public static Map<String, String> url_id = new HashMap<>();
        public static Map<String, String> id_url = new HashMap<>();
        // query text --> query id
        public static Map<String, String> t_qid = new HashMap<>();
        public static Map<String, String> qid_t = new HashMap<>();



        public static void url_extractor()
        {
            try {
                FileSystem fs = FileSystem.get(new Configuration());
                FileStatus[] statuses = fs.globStatus(url_path);
                for (FileStatus status : statuses)
                {
                    BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status.getPath()), StandardCharsets.UTF_8));
                    String line;
                    line=br.readLine();
                    while (line != null){
                        String[] args = line.split("\t");
                        String id = args[0];
                        String url = args[1].charAt(args[1].length()-1)=='/' ? args[1].substring(0, args[1].length()-1) : args[1];
                        url = url.startsWith("http://") ? url.substring(7) : url;
                        url = url.startsWith("www.") ? url.substring(4) : url;

                        url_id.put(url,id);
                        id_url.put(id, url);
                        line=br.readLine();
                    }
                }
            }
            catch (Exception ignored){}
        }

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
            url_extractor();
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
            HashMap<String,Integer> cmap = new HashMap<>();
            // shown links
            String[] raw_links = line[1].split(",");
            // clicked links
            List<String> raw_clicks = Arrays.asList(line[2].split(","));
            // list with shown links
            List<String> llinks = new LinkedList<>();
            // list with clicked links
            List<String> lclicked = new LinkedList<>();
            // raw time stamps
            String[] rawts = line[3].split(",");
            // list with time differences between clicks
            List<Double> lts = new LinkedList<>();

            for (int i = 1; i < rawts.length ; i++){
                lts.add((double) ((Long.parseLong(rawts[i]) - Long.parseLong(rawts[i-1]))/1000));
            }
            lts.add(300.0);
            
            for (String rawlink : raw_links) {

                String temp = rawlink.startsWith("http://") ? rawlink.substring(7) : rawlink;
                temp = temp.startsWith("www.") ? temp.substring(4) : temp;

                llinks.add(temp);
            }

            for(int i = 0; i < raw_clicks.size(); i++){

                String temp = raw_clicks.get(i).startsWith("http://") ? raw_clicks.get(i).substring(7) : raw_clicks.get(i);
                temp = temp.startsWith("www.") ? temp.substring(4) : temp;

                lclicked.add(temp);
            }
            // shown links
            String[] shown_links = new String[llinks.size()];
            for(int i = 0; i < llinks.size(); i++){
                shown_links[i] = llinks.get(i);
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
            for (int i = 0; i < shown_links.length; i++){
                if (url_id.containsKey(shown_links[i])) {
                    // out string
                    StringBuilder out = new StringBuilder();
                    // query
                    out.append(query_text).append("\t");
                    // pos of link
                    out.append(i + 1).append("\t");
                    // pos from the end
                    out.append(shown_links.length - i);
                    if (cmap.containsKey(shown_links[i])) {
                        // pos if clicked
                        out.append(cmap.get(shown_links[i]) + 1).append("\t");
                        // pos from the end
                        out.append(clicked.length - cmap.get(shown_links[i])).append("\t");
                        int ind = 0;
                        //
                        if(cmap.get(shown_links[i]) >= ts.length){
                            ind = ts.length - 1;
                        } else{
                            ind = cmap.get(shown_links[i]);
                        }
                        try{
                            // time
                            out.append(ts[ind]).append("\t");
                            // time beore
                            out.append(ind != 0 ? ts[ind - 1] : 0).append("\t");
                            // time after
                            out.append(ind != ts.length - 1 ? ts[ind + 1] : 0);
                        }catch (Exception e){
                            out.append("300\t300\t300");
                        }
                    }
                    context.write(new Text(url_id.get(shown_links[i])), new Text(out.toString()));
                }
            }
        }

    }

    public static class QueryUrlStatReducer extends Reducer<Text, Text, Text, Text> {

        private MultipleOutputs multipleOutputs;

        public void setup(Context context) {
            multipleOutputs = new MultipleOutputs(context);

        }
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            Map<String, Double[]> qu_stats = new HashMap<>();
            Double[] clicked_positions = new Double[10];
            Double[] shown_positions = new Double[10];
            Double[] shows_if_has_cl = new Double[10];
            Arrays.fill(clicked_positions,0.0);
            Arrays.fill(shown_positions,0.0);
            Arrays.fill(shows_if_has_cl,0.0);
            for (Text value : values) {
                String[] vals = value.toString().split("\t");
                Double[] temp = new Double[13];
                if(qu_stats.containsKey(vals[0])){
                    temp = qu_stats.get(vals[0]);
                } else{
                    Arrays.fill(temp,0.0);
                }
                if (vals.length == 3){
                    // num of appearances
                    temp[0] += 1;
                    // pos of link
                    temp[1] += Double.parseDouble(vals[1]);
                    // pos from the end
                    temp[2] += Double.parseDouble(vals[2]);
                    // first clicked
                    if(Double.parseDouble(vals[1]) == 1){
                        temp[7] += 1;
                    }
                    // last clicked
                    if(Double.parseDouble(vals[2]) == 1){
                        temp[8] += 1;
                    }
                    // link at pos 1-10
                    if(Integer.parseInt(vals[1]) - 1 < 10) {
                        shown_positions[Integer.parseInt(vals[1]) - 1] += 1;
                    }
                }else{
                    // num of appearances
                    temp[0] += 1;
                    // pos of link
                    temp[1] += Double.parseDouble(vals[1]);
                     // pos from the end
                    temp[2] += Double.parseDouble(vals[2]);
                    // num of clicks
                    temp[3] += 1;
                    // pos if clicked
                    temp[4] += Double.parseDouble(vals[3]);
                    // pos if clicked from the end
                    temp[5] += Double.parseDouble(vals[4]);
                    try {
                        temp[6] += Double.parseDouble(vals[5]);
                    } catch(Exception ignored){}
                    // shown at first pos
                    if(Double.parseDouble(vals[1]) == 1){
                        temp[7] += 1;
                    }
                    // shown at last pos
                    if(Double.parseDouble(vals[2]) == 1){
                        temp[8] += 1;
                    }
                    // clicked at first pos
                    if(Double.parseDouble(vals[3]) == 1){
                        temp[9] += 1;
                    }
                    // clicked at last pos
                    if(Double.parseDouble(vals[4]) == 1){
                        temp[10] += 1;
                    }
                    // shown pos 1-10
                    if(Integer.parseInt(vals[1]) - 1 < 10) {
                        shown_positions[Integer.parseInt(vals[1]) - 1] += 1;
                        shows_if_has_cl[Integer.parseInt(vals[1]) - 1] += 1;
                    }
                    // clicked pos 1-10
                    if(Integer.parseInt(vals[3]) - 1 < 10) {
                        clicked_positions[Integer.parseInt(vals[3]) - 1] += 1;
                    }
                    // ts
                    try {
                        // time before
                        temp[11] += Double.parseDouble(vals[6]);
                    } catch(Exception ignored){}
                    try {
                        // time after
                        temp[12] += Double.parseDouble(vals[7]);
                    } catch (Exception ignored){}
                }
                // query text
                qu_stats.put(vals[0],temp);
            }
            Double[] url_stats = new Double[13];
            Arrays.fill(url_stats,0.0);
            for (String q: qu_stats.keySet()){
                if(q.equals("-1")){
                    continue;
                }
                
                Double[] temp  = qu_stats.get(q);
                // sum of temp values over url
                for (int i = 0; i < url_stats.length; i++){
                    url_stats[i] += temp[i];
                }
                double shows = temp[0] + 0.00001;
                double clicks = temp[3] + 0.00001;
                StringBuilder out = new StringBuilder();
                // num of qu apperances
                out.append(temp[0]).append("\t");
                // mean pos of url
                out.append(temp[1]/shows).append("\t");
                // mean pos from the end url
                out.append(temp[2]/shows).append("\t");
                // num of clicks
                out.append(temp[3]).append("\t");
                // mean pos if clicked
                out.append(temp[4]/clicks).append("\t");
                // mean pos if clicked from the end
                out.append(temp[5]/clicks).append("\t");
                // log  time
                out.append(Math.log1p(temp[6]/clicks)).append("\t");
                // mean shows at first pos
                out.append(temp[7]/shows).append("\t");
                // mean shows at last pos
                out.append(temp[8]/shows).append("\t");
                // mean click at first pos
                out.append(temp[9]/clicks).append("\t");
                // mean click at last pos
                out.append(temp[10]/clicks).append("\t");
                // mean time before click
                out.append(Math.log1p(temp[11]/clicks)).append("\t");
                // mean time after click
                out.append(Math.log1p(temp[12]/clicks)).append("\t");
                // show on pos 1-10
                for (Double sposition : shown_positions) {
                    out.append(sposition).append("\t");
                }
                // ctr on pos 1-10
                for(int i = 0; i < clicked_positions.length; i++){
                    out.append(clicked_positions[i]/(shown_positions[i] + 0.00001)).append("\t");
                }
                // shows if has clicks on pos 1-10
                for (Double aDouble : shows_if_has_cl) {
                    out.append(aDouble).append("\t");
                }
                multipleOutputs.write(new Text(q + "\t" + key.toString()), new Text(out.toString()), "./uq_features");
            }
            // url features
            double shows = url_stats[0] + 0.00001;
            double clicks = url_stats[3] + 0.00001;
            StringBuilder out = new StringBuilder();
            // appearances of url
            out.append(url_stats[0]).append("\t");
            // mean show pos of url
            out.append(url_stats[1]/shows).append("\t");
            // mean show pos of url from the end
            out.append(url_stats[2]/shows).append("\t");
            // num of clicks
            out.append(url_stats[3]).append("\t");
            // mean pos if clicked
            out.append(url_stats[4]/clicks).append("\t");
            // mean pos if clicked from the end
            out.append(url_stats[5]/clicks).append("\t");
            // mean time
            out.append(url_stats[6]/clicks).append("\t");
            // mean shows at first pos
            out.append(url_stats[7]/shows).append("\t");
            // mean shows at last pos
            out.append(url_stats[8]/shows).append("\t");
            // mean click at first pos
            out.append(url_stats[9]/clicks).append("\t");
            // mean click at last pos
            out.append(url_stats[10]/clicks).append("\t");
            // mean time before click
            out.append(url_stats[11]/clicks).append("\t");
            // mean time after click
            out.append(url_stats[12]/clicks).append("\t");
            multipleOutputs.write(key, new Text(out.toString()), "./url_features");
        }
    }

    private Job getJobConf(final String input, final String output) throws IOException {

        final Job job = Job.getInstance(getConf());
        job.setJarByClass(QueryUrlStat.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setJobName(QueryUrlStat.class.getCanonicalName());
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(output));
        FileInputFormat.addInputPath(job, new Path(input));
        job.setMapperClass(QueryUrlStatMapper.class);
        job.setReducerClass(QueryUrlStatReducer.class);
        job.setNumReduceTasks(11);
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
        final int ret = ToolRunner.run(new QueryUrlStat(), args);
        System.exit(ret);
    }
}

