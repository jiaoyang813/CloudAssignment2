/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.Spark;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import scala.Tuple2;

import com.google.common.collect.Iterables;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL         neighbor URL
 * URL         neighbor URL
 * URL         neighbor URL
 * ...
 * where URL and their neighbors are separated by space(s).
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to org.apache.spark.graphx.lib.PageRank
 *
 * Example Usage:
 * <pre>
 * bin/run-example JavaPageRank data/mllib/pagerank_data.txt 10
 * </pre>
 */
public final class PageRankSpark {
  
  private static class AddDouble implements Function2<Double, Double, Double> {
	@Override
    public Double call(Double a, Double b) {
      return a + b;
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: PageRankSpark <file> <number_of_iterations>");
      System.exit(1);
    }
    SparkSession spark = SparkSession
      .builder()
      .appName("PageRankSpark")
      .getOrCreate();

    // Loads in input file. It should be in format of:
    //     URL         neighbor URL
    //     URL         neighbor URL
    //     URL         neighbor URL
    //     ...
    JavaRDD<String> articles = spark.read().textFile(args[0]).javaRDD();
    System.out.println("Load raw articles " + articles.count());
    
    JavaRDD<String> urlPairs = articles.flatMap(new FlatMapFunction<String, String>(){
		@Override
		public Iterator<String> call(String article) throws Exception {
			ArrayList<String> urls = XmlParser(article);
			return urls.iterator();
		}});
    
    System.out.println("Parsed raw URLs " + urlPairs.count());
    
    
    // Loads all URLs from input file and initialize their neighbors.
    JavaPairRDD<String, Iterable<String>> links = urlPairs.mapToPair(s -> {
        String[] parts = s.split("\t");
        return new Tuple2<>(parts[0], parts[1]);
      }).distinct().groupByKey().cache();

    // Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    JavaPairRDD<String, Double> ranks = links.mapValues(rs -> 1.0);

    System.out.println("Init Ranks to 1.0 " + ranks.count());
    
    // Calculates and updates URL ranks continuously using PageRank algorithm.
    for (int current = 0; current < Integer.parseInt(args[1]); current++) {
      // Calculates URL contributions to the rank of other URLs.
      JavaPairRDD<String, Double> contribs = links.join(ranks).values()
        .flatMapToPair(s -> {
          int urlCount = Iterables.size(s._1());
          List<Tuple2<String, Double>> results = new ArrayList<>();
          for (String n : s._1) {
            results.add(new Tuple2<>(n, s._2() / urlCount));
          }
          return results.iterator();
        });

      // Re-calculates URL ranks based on neighbor contributions.
      ranks = contribs.reduceByKey(new AddDouble()).mapValues(sum -> 0.15 + sum * 0.85);
    }

    System.out.println("*************Output***********************");
    // Collects all URL ranks and dump them to console.
    List<Tuple2<String, Double>> output = ranks.collect();
    
    System.out.println("Output record " + output.size());
    
    for (Tuple2<?,?> tuple : output) {
      
      System.out.println(tuple._1() + " has rank: " + tuple._2() + ".");
    }
    System.out.println("*************Output***********************");
    
    spark.stop();
  }
  
  private static ArrayList<String> XmlParser(String article)
  {
    String[] parts = article.split("\t");
    String xmlfile = parts[3];
    String currentUrl = "http://en.wikipedia.org/wiki/" + parts[1];
	ArrayList<String> urls = new ArrayList<String>();
	try {
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(new ByteArrayInputStream(xmlfile.getBytes()));
		doc.getDocumentElement().normalize();

//		System.out.println("Root element :" + doc.getDocumentElement().getNodeName());

		NodeList nodeList = doc.getElementsByTagName("param");
		
		for (int count = 0; count < nodeList.getLength(); count++) {

			Node tempNode = nodeList.item(count);

			// make sure it's element node.
			if (tempNode.getNodeType() == Node.ELEMENT_NODE) {

				// get node name and value
				if (tempNode.hasAttributes()) {
					// get attributes names and values
					NamedNodeMap nodeMap = tempNode.getAttributes();

					for (int i = 0; i < nodeMap.getLength(); i++) {
						Node node = nodeMap.item(i);
						if(node.getNodeValue().equals("url"))
						{
							String linkUrl = tempNode.getTextContent();
						    urls.add(currentUrl + "\t" + linkUrl);
						    break;
						}
					}

				}
			}
		}	
	}catch(Exception ex)
	{	
	}
	return urls;
  }
}
