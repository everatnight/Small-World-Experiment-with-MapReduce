/*
 *
 * CS61C Spring 2013 Project 2: Small World
 *
 * Partner 1 Name:
 * Partner 1 Login:
 *
 * Partner 2 Name:
 * Partner 2 Login:
 *
 * REMINDERS: 
 *
 * 1) YOU MUST COMPLETE THIS PROJECT WITH A PARTNER.
 * 
 * 2) DO NOT SHARE CODE WITH ANYONE EXCEPT YOUR PARTNER.
 * EVEN FOR DEBUGGING. THIS MEANS YOU.
 *
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.Math;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class SmallWorld {
    // Maximum depth for any breadth-first search
    public static final int MAX_ITERATIONS = 20;

    // Example writable type
    public static class Node implements Writable {

        public long id; //id for the node
        public long parent = -1;
        public long[] edges; //example array of longs
        public boolean isStart = false;
        public long starter = 0;
        public HashMap<Long, long[]> map = new HashMap<Long, long[]>();

        public Node(long nodeId) {
            this.id = nodeId;
        }

        public Node() {
            // does nothing
        }

        public long getId() {
            return this.id;
        }

        public long getParent() {
            return this.parent;
        }

        public void setParent(int parent) {
            this.parent = parent;
        }

        public long getDistance(long id) {
            return map.get(new Long(id))[1];
        }

        public void setDistance(long id, long distance) {
            if (map.containsKey(id)) {
                map.get(id)[1] = distance;
            } else {
                long[] tmp = {0, distance};
                map.put(id, tmp);
            }
        }

        public long getColor(long id) {
            return this.map.get(id)[0];
        }

        public void setColor(long id, long color) {
            if (map.containsKey(id)) {
                map.get(id)[0] = color;
            } else {
                long[] tmp = {color, -1};
                map.put(id, tmp);
            }
        }

        public long[] getEdges() {
            return this.edges;
        }

        public void setEdges(long[] vertices) {
            this.edges = vertices;
        }

        public void setStart(boolean start) {
            this.isStart = start;
        }

        public boolean isStart() {
            return this.isStart;
        }

        public long getStarter() {
            return this.starter;
        }

        public void setStarter(long id) {
            this.starter = id;
        }
        public void setMap(HashMap<Long,long[]> maps) {
            this.map = maps;
        }
        public HashMap<Long, long[]> getMap() {
            return this.map;
        }
        public Node get() {
            return this;
        }


        // Serializes object - needed for Writable
        public void write(DataOutput out) throws IOException {
            out.writeLong(id);

            // Example of serializing an array:
            
            // It's a good idea to store the length explicitly
            int length = 0;

            if (edges != null){
                length = edges.length;
            }

           
            // always write the length, since we need to know
            // even when it's zero
            out.writeInt(length);
            out.writeBoolean(this.isStart);
            out.writeLong(this.starter);
            // now write each long in the array
            for (int i = 0; i < length; i++){
                out.writeLong(edges[i]);
            }

            long keylength = 0;
            keylength = map.keySet().size();
            out.writeLong(keylength);
            for (Map.Entry<Long, long[]> entry : map.entrySet()) {
                out.writeLong((long)entry.getKey());
                out.writeLong(entry.getValue()[0]);
                out.writeLong(entry.getValue()[1]);
            }
        }

        // Deserializes object - needed for Writable
        public void readFields(DataInput in) throws IOException {
            // example reading an int from the serialized object
            this.id = in.readLong();

            // example reading length from the serialized object
            int length = in.readInt();
            this.isStart = in.readBoolean();
            this.starter = in.readLong();
            // Example of rebuilding the array from the serialized object
            this.edges = new long[length];
            
            for(int i = 0; i < length; i++){
                edges[i] = in.readLong();
            }

            HashMap<Long, long[]> newmap = new HashMap<Long, long[]>();

            long keylength = in.readLong();
            for (int i = 0; i < keylength; i++) {
                long key = in.readLong();
                long color = in.readLong();
                long distance = in.readLong();
                long[] tmp = {color, distance};
                newmap.put(key, tmp);
            }
            this.map = newmap;
        }

        public String toString() {
            // We highly recommend implementing this for easy testing and
            // debugging. This version just returns an empty string.
            StringBuffer sb = new StringBuffer();
            sb.append("the node id is: \t" + this.id + "\nthe node isStart is:\t" + this.isStart
                + "\nthe node starter is:\t" + this.starter + "\nthe edges is:\t");
            if (edges != null) {
                for (long edge : this.edges) {
                    sb.append(edge + ", ");
                }
            }
            sb.append("\nthe map is:\t");
            for (Map.Entry<Long, long[]> entry : map.entrySet()) {
                sb.append("\nstarter:\t" + entry.getKey());
                sb.append("\ncolor:\t" + entry.getValue()[0]);
                sb.append("\ndistance:\t" + entry.getValue()[1]);
            }
            return sb.toString();
        }

    }


    /* The first mapper. Part of the graph loading process, currently just an 
     * identity function. Modify as you wish. */
    public static class LoaderMap extends Mapper<LongWritable, LongWritable, 
        LongWritable, LongWritable> {

        @Override
        public void map(LongWritable key, LongWritable value, Context context)
                throws IOException, InterruptedException {

            // example of getting value passed from main
            int inputValue = Integer.parseInt(context.getConfiguration().get("inputValue"));
            context.write(key, value);
        }
    }


    /* The first reducer. This is also currently an identity function (although it
     * does break the input Iterable back into individual values). Modify it
     * as you wish. In this reducer, you'll also find an example of loading
     * and using the denom field.  
     */
    public static class LoaderReduce extends Reducer<LongWritable, LongWritable, 
        LongWritable, Node> {

        public long denom;

        public boolean isStart(long denom) {
            if (Math.random() < (1.0 / denom)) {
                return true;
            }
            return false;
        }

        public void reduce(LongWritable key, Iterable<LongWritable> values, 
            Context context) throws IOException, InterruptedException {
            // We can grab the denom field from context: 
            denom = Long.parseLong(context.getConfiguration().get("denom"));

            // You can print it out by uncommenting the following line:
            // System.out.println(denom);

            //the edges ArrayList.
            ArrayList<Long> edges = new ArrayList<Long>();
            Node node = new Node(key.get());
            
            int i = 0;
            for (LongWritable value : values) {
                edges.add(value.get());
            }

            long[] arrayEdges = new long[edges.size()];

            for (Long edge : edges) {
                arrayEdges[i] = (long) edge;
                i++;
            }

            //choose start 
            node.setEdges(arrayEdges);
            if (isStart(denom)) {
                node.setStarter(key.get());
                HashMap<Long, long[]> map = new HashMap<Long, long[]>();
                long[] tmp = {1, 0};
                map.put(key.get(), tmp);
                node.setMap(map);
                node.setStart(true);
            }

            System.out.println("============LoaderReduce================");
            System.out.println(node.toString());
            System.out.println("============================");
            context.write(key, node);
        }
    }

    /* The first mapper. Part of the graph loading process, currently just an 
     * identity function. Modify as you wish. */
    public static class SearchMap extends Mapper<LongWritable, Node, 
        LongWritable, Node> {

        @Override
        public void map(LongWritable key, Node value, Context context)
                throws IOException, InterruptedException {
            Node node = value.get();
            System.out.println("========search map beginning============:\n" + node.toString());
            System.out.println("\n%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
            for (long startkey : node.getMap().keySet()) {
                if (node.getColor(startkey) == 1) {
                    if (node.getEdges() != null) {
                        for (long v : node.getEdges()) {
                            Node vnode = new Node(v);
                            System.out.println("======v==========\nthe v is:\t" + v + "\n");
                            vnode.setStarter(startkey);
                            HashMap<Long, long[]> map = new HashMap<Long, long[]>();
                            long[] tmp = {1, node.getDistance(startkey) + 1};
                            map.put(startkey, tmp);
                            vnode.setMap(map);
                            System.out.println("=========vnode==========\n" + vnode.toString());
                            context.write(new LongWritable(v), vnode);
                        }
                    }
                    node.setColor(startkey, 2);
                }
            }
            
            context.write(key, value);
        }
    }

    /* The first reducer. This is also currently an identity function (although it
     * does break the input Iterable back into individual values). Modify it
     * as you wish. In this reducer, you'll also find an example of loading
     * and using the denom field.  
     */
    public static class SearchReduce extends Reducer<LongWritable, Node, 
        LongWritable, Node> {

        public long[] findMinDark(HashSet<long[]> hs) {
            long[] result = {0L, Long.MAX_VALUE};
            for (long[] elem : hs) {
                result[0] = Math.max(elem[0], result[0]);
                result[1] = Math.min(elem[1], result[1]);
            }
            return result;
        }

        public void reduce(LongWritable key, Iterable<Node> values, 
            Context context) throws IOException, InterruptedException {
            long edges[] = null;
            HashMap<Long, long[]> map = new HashMap<Long, long[]>();
            HashMap<Long, HashSet<long[]>> tmpMap = new HashMap<Long, HashSet<long[]>>();
            for (Node u : values) {
                System.out.println("=============In Reduce===========\n" + u.toString());
                if (u.getEdges().length > 0) {
                    edges = u.getEdges();
                }
                for (Long startKey : u.getMap().keySet()) {
                    System.out.println("=====start key======\n" + startKey + "\n" + u.getMap().get(startKey) + "\n=============");
                    if (tmpMap.get(startKey) != null) {
                        if (u.getMap().size() != 0) {
                            tmpMap.get(startKey).add(u.getMap().get(startKey));    
                        }
                    } else {
                        HashSet<long[]> tmp = new HashSet<long[]>();
                        tmp.add(u.getMap().get(startKey));
                        tmpMap.put(startKey, tmp);
                    }
                }
            }

            for (Map.Entry<Long, HashSet<long[]>> t : tmpMap.entrySet()) {
                System.out.println("=====HashSet<long[]>======\n");
                for (long[] i : t.getValue()) {
                    for (long z : i) {
                        System.out.println(z + ", ");
                    }
                    System.out.println("\n");
                }
                map.put(t.getKey(), findMinDark(t.getValue()));
            }

            System.out.println("============Reduce result=======\n"
                + "the node " + key.get() + ":\n");
            System.out.println("edges:\t");
            for (long e : edges) {
                System.out.println(e + ", ");;
            }
            System.out.println("\n");
            for (Map.Entry<Long, long[]> entry : map.entrySet()) {
                System.out.println("\tstarter:" + entry.getKey() + "\n");
                System.out.println("\tcolor:" + entry.getValue()[0] + "\n");
                System.out.println("\tdistance:" + entry.getValue()[1] + "\n");
            }

            Node node = new Node(key.get());
            node.setEdges(edges);
            node.setMap(map);
            context.write(key, node);
        }
    }

     /* The first mapper. Part of the graph loading process, currently just an 
     * identity function. Modify as you wish. */
    public static class HistoMap extends Mapper<LongWritable, Node, 
        LongWritable, LongWritable> {

        @Override
        public void map(LongWritable key, Node value, Context context)
                throws IOException, InterruptedException {
            Node node = value.get();
            HashMap<Long, long[]> map = node.getMap();
            for (Map.Entry<Long, long[]> entry : map.entrySet()) {
                context.write(new LongWritable(entry.getValue()[1]), new LongWritable(1L));
            }
        }
    }

      /* The first reducer. This is also currently an identity function (although it
     * does break the input Iterable back into individual values). Modify it
     * as you wish. In this reducer, you'll also find an example of loading
     * and using the denom field.  
     */
    public static class HistoReduce extends Reducer<LongWritable, LongWritable, 
        LongWritable, LongWritable> {

        public void reduce(LongWritable key, Iterable<LongWritable> values, 
            Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(new LongWritable(key.get()), new LongWritable(sum));
        }
    }

    public static void main(String[] rawArgs) throws Exception {
        GenericOptionsParser parser = new GenericOptionsParser(rawArgs);
        Configuration conf = parser.getConfiguration();
        String[] args = parser.getRemainingArgs();

        // Pass in denom command line arg:
        conf.set("denom", args[2]);

        // Sample of passing value from main into Mappers/Reducers using
        // conf. You might want to use something like this in the BFS phase:
        // See LoaderMap for an example of how to access this value
        conf.set("inputValue", (new Integer(5)).toString());

        // Setting up mapreduce job to load in graph
        Job job = new Job(conf, "load graph");

        job.setJarByClass(SmallWorld.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Node.class);

        job.setMapperClass(LoaderMap.class);
        job.setReducerClass(LoaderReduce.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // Input from command-line argument, output to predictable place
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("bfs-0-out"));

        // Actually starts job, and waits for it to finish
        job.waitForCompletion(true);

        // Repeats your BFS mapreduce
        int i = 0;
        while (i < MAX_ITERATIONS) {
            job = new Job(conf, "bfs" + i);
            job.setJarByClass(SmallWorld.class);

            // Feel free to modify these four lines as necessary:
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Node.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Node.class);

            // You'll want to modify the following based on what you call
            // your mapper and reducer classes for the BFS phase.
            job.setMapperClass(SearchMap.class); // currently the default Mapper
            job.setReducerClass(SearchReduce.class); // currently the default Reducer

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            // Notice how each mapreduce job gets gets its own output dir
            FileInputFormat.addInputPath(job, new Path("bfs-" + i + "-out"));
            FileOutputFormat.setOutputPath(job, new Path("bfs-"+ (i+1) +"-out"));

            job.waitForCompletion(true);
            i++;
        }

        // Mapreduce config for histogram computation
        job = new Job(conf, "hist");
        job.setJarByClass(SmallWorld.class);

        // Feel free to modify these two lines as necessary:
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Node.class);

        // DO NOT MODIFY THE FOLLOWING TWO LINES OF CODE:
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        // You'll want to modify the following based on what you call your
        // mapper and reducer classes for the Histogram Phase
        job.setMapperClass(HistoMap.class); // currently the default Mapper
        job.setReducerClass(HistoReduce.class); // currently the default Reducer

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // By declaring i above outside of loop conditions, can use it
        // here to get last bfs output to be input to histogram
        FileInputFormat.addInputPath(job, new Path("bfs-"+ i +"-out"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
