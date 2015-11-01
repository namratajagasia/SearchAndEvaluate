package index;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

public class EasySearch {
	public static void main(String[] args) throws ParseException, IOException {
		String indexLocation="C:/Users/DELL/Downloads/index/index";
		calculatingTheRelevance(indexLocation);
	}

	private static void calculatingTheRelevance(String indexLocation) throws IOException, ParseException {
		
		//HashMap<Integer,String> DocIdToDocNo = new HashMap<Integer, String>();
	
		//HashMap<Integer,Double> hmapDocTermRelvance= new HashMap<Integer, Double>();
		HashMap<String, Double> docNoToQueryRelv = new HashMap<String, Double>();

		IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexLocation)));
		IndexSearcher searcher = new IndexSearcher(reader);

		/**
		 * Get query terms from the query string
		 */
		String queryString = "New York";

		// Get the preprocessed query terms
		Analyzer analyzer = new StandardAnalyzer();
		QueryParser parser = new QueryParser("TEXT", analyzer);
		Query query = parser.parse(queryString);
		Set<Term> queryTerms = new LinkedHashSet<Term>();
		searcher.createNormalizedWeight(query, false).extractTerms(queryTerms);
		System.out.println("Terms in the query: ");
		for (Term t : queryTerms) {
			System.out.println(t.text());
		}
		System.out.println();

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
		System.out.println("Total number of docs in corpus"+N);

		// Processing each segment
		for (int i = 0; i < leafContexts.size(); i++) {
			// Get document length
			LeafReaderContext leafContext = leafContexts.get(i);
			int startDocNo = leafContext.docBase;
			int numberOfDoc = leafContext.reader().maxDoc();
			
			for (int docId = startDocNo; docId < startDocNo+numberOfDoc; docId++) {
				System.out.println("Document "+docId);
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
						while ((doc = de.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
							//	System.out.println("\"police\" occurs " + de.freq()
							//		+ " time(s) in doc(" + (de.docID() + startDocNo)
							//	+ ")");
							
						//	System.out.print(doc);
							if(doc==(docId-startDocNo)){
								cQInDoc = de.freq();
								System.out.println(cQInDoc);
							}							
						}
					

					//TF
					double tf = cQInDoc/docLeng;
					double idf = Math.log10(1+(N/df));
					relevanceScoreForTerm=tf*idf;
					System.out.println("relevance for term "+t.text()+" is "+relevanceScoreForTerm);
					relevanceScoreForQuery=relevanceScoreForQuery+relevanceScoreForTerm;				
				}
				//System.out.println("relevance for term in doc"+searcher.doc(docId).get("DOCNO")+" is "+relevanceScoreForQuery);
			    //hmapDocTermRelvance.put(docId, relevanceScoreForQuery);		
				//DocIdToDocNo.put(docId, searcher.doc(docId).get("DOCNO"));
				docNoToQueryRelv.put( searcher.doc(docId).get("DOCNO"), relevanceScoreForQuery);

			}


		}//for
		printMap(docNoToQueryRelv);

	}

	private static void printMap(HashMap<String, Double> docNoToQueryRelv) {
		Set<Entry<String, Double>> set =docNoToQueryRelv.entrySet();
		Iterator iterator = set.iterator();
		while(iterator.hasNext()) {
			Map.Entry mentry = (Map.Entry)iterator.next();
			System.out.println("Doc is: "+ mentry.getKey() + " & Term Relevance is: "+mentry.getValue());
		}
	}
	
}



