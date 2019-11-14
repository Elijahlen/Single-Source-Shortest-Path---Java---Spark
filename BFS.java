import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.Tuple3;

class Path implements Serializable{
	public List<String> nodes;
	public Path(List<String> Nodes) {
		nodes = Nodes;
	}
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();	
		String prefix = "";
		for (String e : nodes) {
			sb.append(prefix);
			prefix = "-";
			sb.append(e);
		}		
		return sb.toString();
	}
	
}

public class BFS {
	public static int transfer(String x) {
		return Integer.parseInt(x.substring(x.length() - 1));
	}
	
	public static JavaPairRDD<String, Tuple3<Integer, Path, HashMap<String, Integer>>> 
	BfsCore(JavaPairRDD<String, Tuple3<Integer, Path, HashMap<String, Integer>>> inputRdd) {
		
		//for each parent node, emit all the child nodes (node in the adjacent list)
		//in the same format  Key ,          Value
		//					NodeID,<new_dist, new_path,emptyMap>	
		//at the same time
		//for parent node itself emit itself without any change foe keeping the adjacent list
		JavaPairRDD <String, Tuple3<Integer, Path, HashMap<String, Integer>>> map1 = 
		inputRdd.flatMapToPair(line -> {
			//initial the list for flatMapToPair result to add
			List<Tuple2<String, Tuple3<Integer, Path, HashMap<String, Integer>>>> result = new ArrayList<>();	
			//emit child nodes
			for ( Map.Entry e: line._2._3().entrySet()) {
				String childNode = e.getKey().toString();  // nodeID of childNode to be eimit
				int newDist = line._2._1() + (int) e.getValue();  // dist(child) = dist(parent) + weight
				if (line._2._1() == Integer.MAX_VALUE)
					newDist = Integer.MAX_VALUE;
				List<String> newPath = new ArrayList<>();
				newPath.addAll(line._2._2().nodes); // the path to childNode is the Path to its parent add itself
				newPath.add(e.getKey().toString());
//				System.out.println(newPath);
				HashMap<String, Integer> emptyMap = new HashMap<String, Integer>(); // for child node, it does not need to emit the adjacent list
				result.add(new Tuple2<>(childNode,                                  // it will get from when this node as a parent node anyway
			               new Tuple3<>(newDist, 
			                		    new Path(newPath),
			                		    emptyMap)));
			}
			//emit parent nodes
			result.add(line);		
			return result.iterator();
		});
		// GroupBykey
		JavaPairRDD <String, Iterable<Tuple3<Integer, Path, HashMap<String, Integer>>>> interRDD = map1.groupByKey();
		// for each node as Key find the minimum dist and its Path combine with its AdjacentList
		JavaPairRDD <String, Tuple3<Integer, Path, HashMap<String, Integer>>> reduce1 = 
		 interRDD.mapToPair(line -> {
			HashMap<String, Integer> adjacentList = new HashMap<String, Integer>();
			int minDist = Integer.MAX_VALUE;
			Path shortestPath = new Path(new ArrayList<String>());
			for(Tuple3<Integer, Path, HashMap<String, Integer>> e: line._2) {
				if(!e._3().isEmpty())
					adjacentList = e._3();
				if(e._1()<minDist) {
					minDist = e._1();
					List<String> l = e._2().nodes;
					shortestPath = new Path(l);
				}
			}
			return new Tuple2<>(line._1, new Tuple3<>(minDist, shortestPath, adjacentList));
		});
		// return the function
		return reduce1;
	}
	// Comparator class for sort By Dist
	public static class IntegerComparator implements Comparator<Integer>, Serializable{

		@Override
		public int compare(Integer a, Integer b) {
			return a-b;
		}
		
	}
	public static void main(String[] args) {
		
	SparkConf conf = new SparkConf().setMaster("local").setAppName("Ass2");
	
	// for delet output file if it exists
	conf.set("spark.hadoop.validateOutputSpecs", "false");
	
	// get rid of info worning
	Logger.getLogger("org").setLevel(Level.OFF);
	Logger.getLogger("akka").setLevel(Level.OFF);
	
	JavaSparkContext context = new JavaSparkContext(conf);
	
	//getting StartNode as integer
	String StartNode = args[0];
	System.out.println("Start Node is " + StartNode);
	
	JavaRDD<String> inputfile = context.textFile(args[1]);
	System.out.println("-------------originInputfile--------------");
	inputfile.collect().forEach(System.out::println);
	
	JavaPairRDD<String, Tuple2<String, Integer>> input_1 = inputfile.mapToPair(line -> {
		String[] words = line.split(",");
		return new Tuple2<>(words[0], new Tuple2<>(words[1], Integer.valueOf(words[2])));
	});
	System.out.println("-------------inputToPair--------------");
	input_1.collect().forEach(System.out::println);
		
	JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> input_2 = input_1.groupByKey();
	int numNodes = (int) input_2.count();
	
	System.out.println("-------------inputGourpByKey--------------");
	input_2.collect().forEach(System.out::println);
	System.out.println("Total nodes: " + numNodes);
	
	
	JavaPairRDD<String, HashMap<String, Integer>> inputWithMap = input_2.mapToPair(line ->{
		HashMap<String, Integer> ChilrdWeightMap = new HashMap<String, Integer>();
		for(Tuple2<String, Integer> e :line._2) {
			ChilrdWeightMap.put(e._1, e._2);
		}
		return new Tuple2<>(line._1, ChilrdWeightMap);
	});
	
	System.out.println("-------------inputWithMapAsAdjacentList--------------");
	inputWithMap.collect().forEach(System.out::println);

	JavaPairRDD<String, Tuple3<Integer, Path, HashMap<String, Integer>>> finalInput = 
			inputWithMap.mapToPair(line -> {
				
				//initial dist to Infinity for all nodes
				Integer dist = Integer.MAX_VALUE;
				//if its the source node set dist to zero
				if (line._1.equals(args[0]))
					dist = 0;
				
				//initial the path for all nodes
				List<String> l = new ArrayList<String>();
				l.add(line._1);
				Path path = new Path(l);
				
				return new Tuple2<>(line._1, new Tuple3<>(dist, path, line._2));
				
			});
	System.out.println("-------------finalInput-------------");
	finalInput.collect().forEach(System.out::println);

	JavaPairRDD <String, Tuple3<Integer, Path, HashMap<String, Integer>>> inter = finalInput;
	// start iteration for num_node - 1 times (worst case)
	System.out.println("-------------StartBFS--------------");
	for (int i = 1; i < numNodes ; i++ ) {
		inter = BfsCore(inter);
		System.out.println("-------------After Iteration" + i + "-------------");
		inter.collect().forEach(System.out::println);
	}
	//clean up the result as output format to save
	//first get rid of the source Node 
	//then get rid of the adjacentList, at the same time, if there is a Infinity dist, change it to -1
	//then sortBy distance
	//then combine each part so that when output it has no parenthesis
	JavaRDD<String> finalResult = inter.filter(line -> !line._1.equals(StartNode)).mapToPair(line->{
		int modiDist = line._2._1();
		if(modiDist == Integer.MAX_VALUE)
			modiDist = -1;
		return new Tuple2<>(modiDist, new Tuple2<>(line._1, line._2._2()));   //swap the nodeID and dist for sorting
	}).sortByKey(new IntegerComparator()).map(x -> x._2._1 + "," + x._1 + "," + x._2._2);
	System.out.println("-------------finalResult--------------");
	finalResult.collect().forEach(System.out::println);
	// save to file
	finalResult.saveAsTextFile(args[2]);
	
	
	
	
	
	
	
	
	}
}
