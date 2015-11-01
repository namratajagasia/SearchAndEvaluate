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
import org.apache.lucene.search.similarities.DefaultSimilarity;
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

public class SearchTRECTopics{

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
		PrintWriter shortQueryOutput = new PrintWriter("E:/MS/UNIV/indiana/IR/assignment2OP/shortQueryOutput.txt", "UTF-8");
		PrintWriter longQueryOutput = new PrintWriter("E:/MS/UNIV/indiana/IR/assignment2OP/longQueryOutput.txt", "UTF-8");
		System.out.println(indexLocation);
		HashMap<String, Double> docNoToQueryRelv = new HashMap<String, Double>();
		
		IndexSearcher searcher = new IndexSearcher(reader);

		/**
		 * Get document length and term frequency
		 */
		// Use DefaultSimilarity.decodeNormValue(…) to decode normalized
		// document length
		DefaultSimilarity dSimi = new DefaultSimilarity();
		// Get the segments of the index
		List<LeafReaderContext> leafContexts = reader.getContext().reader()
				.leaves();
		int N = reader.maxDoc();
		//System.out.println("Total number of docs in corpus"+N);

		Analyzer analyzer = new StandardAnalyzer();
		QueryParser parser = new QueryParser("TEXT", analyzer);
		for (Map.Entry<Integer, List<String>> entry: qMap.entrySet()){
			//iterating over first short query then long query
			for (int k=0; k < entry.getValue().size() ; k++){
				String queryString = entry.getValue().get(k);
				// Get the preprocessed query terms

				Query query = parser.parse(queryString);
				Set<Term> queryTerms = new LinkedHashSet<Term>();
				searcher.createNormalizedWeight(query, false).extractTerms(queryTerms);
				//System.out.println("Terms in the query: ");
				//for (Term t : queryTerms) {
				//	System.out.println(t.text());
				//}
				//System.out.println();
				
				// Processing each segment
				for (int i = 0; i < leafContexts.size(); i++) {
					// Get document length
					LeafReaderContext leafContext = leafContexts.get(i);
					int startDocNo = leafContext.docBase;
					int numberOfDoc = leafContext.reader().maxDoc();
					
					for (int docId = startDocNo; docId < startDocNo+numberOfDoc; docId++) {
						
						// Get normalized length (1/sqrt(numOfTokens)) of the document
						float normDocLeng = dSimi.decodeNormValue(leafContext.reader().getNormValues("TEXT").get(docId-startDocNo));
						// Get length of the document
						float docLeng = 1 / (normDocLeng * normDocLeng);
						double relevanceScoreForQuery=0;
						for (Term t : queryTerms) {
							double relevanceScoreForTerm;
							int cQInDoc=0;
							int df=reader.docFreq(new Term("TEXT", t.text()));
							// Get frequency of the term from its postings for that document
							PostingsEnum de = MultiFields.getTermDocsEnum(leafContext.reader(),"TEXT", new BytesRef(t.text()));

							// count for the term in that document
							int doc;
							//System.out.println("printing postings");
								while (de != null && (doc = de.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
									//	System.out.println("\"police\" occurs " + de.freq()
									//		+ " time(s) in doc(" + (de.docID() + startDocNo)
									//	+ ")");
									
								//	System.out.print(doc);
									if(doc==(docId-startDocNo)){
										cQInDoc = de.freq();
										//System.out.println(cQInDoc);
									}							
								}
							

							//Term Freq
							double tf = cQInDoc/docLeng;
							//inv doc fref
							double idf =0;
							if(df!=0)
							{
							 idf = Math.log10(1+(N/df));
							}
							else{
								 idf=0;
							}
							relevanceScoreForTerm=tf*idf;
							//System.out.println("relevance for term "+t.text()+" is "+relevanceScoreForTerm);
							relevanceScoreForQuery=relevanceScoreForQuery+relevanceScoreForTerm;				
						}
						//System.out.println("relevance for term in doc"+searcher.doc(docId).get("DOCNO")+" is "+relevanceScoreForQuery);
					    //hmapDocTermRelvance.put(docId, relevanceScoreForQuery);		
						//DocIdToDocNo.put(docId, searcher.doc(docId).get("DOCNO"));
						if(relevanceScoreForQuery!=0)
						docNoToQueryRelv.put( searcher.doc(docId).get("DOCNO"), relevanceScoreForQuery);
					}//for doc
				}//for leaf contexts
				
				//sorting to get top 1000 ranks and populating two output files
				Map<String, Double> sortedDocNoToScore = sortByValue(docNoToQueryRelv);
				Integer hits = 1;				
				for (Map.Entry<String, Double> entity: sortedDocNoToScore.entrySet()){
					if (hits != 1001){
						// k==0 means short query else long query
						if (k==0){							
							shortQueryOutput.println(entry.getKey() + "\t\t\t Q0 \t\t" + ((entity.getKey())) + "\t\t\t\t" + hits + " \t\t\t\t" + entity.getValue() + "\t\t\tshortRun-1");
						}
						else{
							longQueryOutput.println(entry.getKey() + "\t\t\t Q0 \t\t" + ((entity.getKey())) + "\t\t\t\t" + hits + " \t\t\t\t" + entity.getValue() + "\t\t\tlongRun-1");
						}						
					hits = hits + 1;
					}
					else{
						break;
					}					
				}				
			}
			}//qmap
		shortQueryOutput.close();
		longQueryOutput.close();
		}

	private static Map<String, Double> sortByValue(HashMap<String, Double> docNoToQueryRelv) {
		// Convert Map to List
				List<Map.Entry<String, Double>> list = new LinkedList<Map.Entry<String, Double>>(docNoToQueryRelv.entrySet());
		 
				// Sort list with comparator, to compare the Map values
				Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
					public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
						return (o2.getValue()).compareTo(o1.getValue());
					}
				});
		 
				// Convert sorted map back to a Map
				Map<String, Double> sortedMap = new LinkedHashMap<String, Double>();
				for (Iterator<Map.Entry<String, Double>> it = list.iterator(); it.hasNext();) {
					Map.Entry<String, Double> entry = it.next();
					sortedMap.put(entry.getKey(), entry.getValue());
				}
				return sortedMap;
		
	}
}