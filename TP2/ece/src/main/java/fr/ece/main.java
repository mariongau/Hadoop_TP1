package fr.ece;

import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.Job; 

public class main { 

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
	
		// Verification le nombre d'argument doit Ítre Ègal ‡ deux
		if (args.length != 2) { 
		  System.out.printf("Format de la ligne de commande : Pivot <input dir> <output dir>\n"); 
		  System.exit(-1); 
		}
		
		Job job = new Job();
		job.setJarByClass(main.class); 
		
		job.setJobName("Pivot d'un CSV"); // Attribution d'un nom au job -- pour les logs 
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));  // chemin des fichiers en entrÈe
		FileOutputFormat.setOutputPath(job, new Path(args[1])); // chemin des fichiers en sorties
		
		job.setMapperClass(map.class);
		job.setReducerClass(reduce.class);
		
		job.setMapOutputKeyClass(IntWritable.class); 
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(IntWritable.class); 
		job.setOutputValueClass(Text.class);
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);

  }

}

