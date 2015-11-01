package index;

import org.apache.commons.io.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

public class CompareAlgorithms {
	public static void main(String args[]) throws IOException, ParseException{
		//location of index file
		String indexLocation="E:/MS/UNIV/indiana/IR/assignment2/index";
		//location of query file
		File queryLocation = new File("E:/MS/UNIV/indiana/IR/topics.51-100");	
		String file = FileUtils.readFileToString(queryLocation);		
		String[] topics = StringUtils.substringsBetween(file, "<top>", "</top>");		

		HashMap<Integer, List<String>> qMap = new HashMap<>();

		for (String topic: topics){
			List<String> titleAndDescriptionList = new ArrayList<>();
			String queryId = StringUtils.substringBetween(topic, "<num> Number:", "<dom>");
			if (queryId == null){
				//due to format of file
				queryId = StringUtils.substringBetween(topic, "<num>  Number:", "<dom>");
			}		

			String title = StringUtils.substringBetween(topic, "<title> Topic:", "<desc>");
			if (title == null){
				title = StringUtils.substringBetween(topic, "<title>  Topic:", "<desc>");
			}	
			title=title.replace("/", " ");	
			titleAndDescriptionList.add(title.trim());

			String description = StringUtils.substringBetween(topic, "<desc> Description:", "<smry>");
			if (description == null){
				description = StringUtils.substringBetween(topic, "<desc>  Description:", "<smry>");
			}	
			description=description.replace("/", " ");	
			titleAndDescriptionList.add(description.trim());

			qMap.put(Integer.parseInt(queryId.trim()), titleAndDescriptionList);		
		}
		calculatingTheRelevance(indexLocation,qMap);
	}

	private static void calculatingTheRelevance(String indexLocation,HashMap<Integer, List<String>> qMap) throws IOException, ParseException {
		IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexLocation)));
		IndexSearcher searcher = new IndexSearcher(reader);
		Analyzer analyzer = new StandardAnalyzer();
		QueryParser parser = new QueryParser("TEXT", analyzer);
		for(int i=0;i<4;i++){
			if(i==0){
				//Vector Space Model
				searcher.setSimilarity(new DefaultSimilarity());
				PrintWriter shortQueryOutput = new PrintWriter("E:/MS/UNIV/indiana/IR/assignment2OP/shortQueryOutputDefault.txt", "UTF-8");
				PrintWriter longQueryOutput = new PrintWriter("E:/MS/UNIV/indiana/IR/assignment2OP/longQueryOutputDefault.txt", "UTF-8");
				otherRankMethods(qMap,parser,searcher,shortQueryOutput, longQueryOutput);
			}
			if(i==1){
				searcher.setSimilarity(new BM25Similarity());
				PrintWriter shortQueryOutput = new PrintWriter("E:/MS/UNIV/indiana/IR/assignment2OP/shortQueryOutputBM25.txt", "UTF-8");
				PrintWriter longQueryOutput = new PrintWriter("E:/MS/UNIV/indiana/IR/assignment2OP/longQueryOutputBM25.txt", "UTF-8");
				otherRankMethods(qMap,parser,searcher,shortQueryOutput, longQueryOutput);				
			}
			if(i==2){
				searcher.setSimilarity(new LMDirichletSimilarity());
				PrintWriter shortQueryOutput = new PrintWriter("E:/MS/UNIV/indiana/IR/assignment2OP/shortQueryOutputDirichelet.txt", "UTF-8");
				PrintWriter longQueryOutput = new PrintWriter("E:/MS/UNIV/indiana/IR/assignment2OP/longQueryOutputDirichelet.txt", "UTF-8");
				otherRankMethods(qMap,parser,searcher,shortQueryOutput, longQueryOutput);				
			}
			if(i==3){
				searcher.setSimilarity(new LMJelinekMercerSimilarity((float) 0.7));
				PrintWriter shortQueryOutput = new PrintWriter("E:/MS/UNIV/indiana/IR/assignment2OP/shortQueryOutputMercer.txt", "UTF-8");
				PrintWriter longQueryOutput = new PrintWriter("E:/MS/UNIV/indiana/IR/assignment2OP/longQueryOutputMercer.txt", "UTF-8");
				otherRankMethods(qMap,parser,searcher,shortQueryOutput, longQueryOutput);				
			}
		}
		
	}

	private static void otherRankMethods(HashMap<Integer, List<String>> qMap,QueryParser parser,IndexSearcher searcher,PrintWriter shortQueryOutput,PrintWriter longQueryOutput) throws IOException, ParseException {
		for (Map.Entry<Integer, List<String>> entry: qMap.entrySet()){		
			//iterating over first short query then long query
			for (int k=0; k < entry.getValue().size() ; k++){				
				String queryString = entry.getValue().get(k);
				// Get the preprocessed query terms
				Query query = parser.parse(QueryParser.escape(queryString));
				TopDocs results = searcher.search(query, 1000); 
				  //Print number of hits
				int numTotalHits = results.totalHits; 
				System. out .println(numTotalHits + " total matching documents"); 
				//Print retrieved results 
				ScoreDoc[] hits = results.scoreDocs;  
		
						
				for (int i = 0; i < hits.length; i++) {				
					Document doc = searcher.doc(hits[i].doc);
					// 	k==0 means short query else long query
					if (k==0){
						shortQueryOutput.format("%-6s \t Q0 \t %-15s \t %-5s \t %f \t run-1\n", entry.getKey(), doc.get("DOCNO"), (i+1), hits[i].score);
					}
					else{
						longQueryOutput.format("%-6s \t Q0 \t %-15s \t %-5s \t %f \t run-1\n", entry.getKey(), doc.get("DOCNO"), (i+1), hits[i].score);
					}				
					}			
				}
			
		}
		shortQueryOutput.close();
		longQueryOutput.close();
	}

	
	}
