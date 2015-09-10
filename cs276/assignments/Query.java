package cs276.assignments;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import cs276.util.Pair;

public class Query {

	// Term id -> position in index file
	private static Map<Integer, Long> posDict = new TreeMap<Integer, Long>();
	// Term id -> document frequency
	private static Map<Integer, Integer> freqDict = new TreeMap<Integer, Integer>();
	// Doc id -> doc name dictionary
	private static Map<Integer, String> docDict = new TreeMap<Integer, String>();
	// Term -> term id dictionary
	private static Map<String, Integer> termDict = new TreeMap<String, Integer>();
	// Index
	private static BaseIndex index = null;
	
	/* 
	 * Write a posting list with a given termID from the file 
	 * You should seek to the file position of this specific
	 * posting list and read it back.
	 * */
	private static PostingList readPosting(FileChannel fc, int termId)
			throws IOException {
		Long postingPosition = posDict.get(termId);
		fc.position(postingPosition);
		PostingList ps=index.readPosting(fc);
		return ps;
	}

	public static void main(String[] args) throws IOException {
		/* Parse command line */
		if (args.length != 2) {
			System.err.println("Usage: java Query [Basic|VB|Gamma] index_dir");
			return;
		}

		/* Get index */
		String className = "cs276.assignments." + args[0] + "Index";
		try {
			Class<?> indexClass = Class.forName(className);
			index = (BaseIndex) indexClass.newInstance();
		} catch (Exception e) {
			System.err
					.println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
			throw new RuntimeException(e);
		}

		/* Get index directory */
		String input = args[1];
		File inputdir = new File(input);
		if (!inputdir.exists() || !inputdir.isDirectory()) {
			System.err.println("Invalid index directory: " + input);
			return;
		}

		/* Index file */
		RandomAccessFile indexFile = new RandomAccessFile(new File(input,
				"corpus.index"), "r");

		String line = null;
		/* Term dictionary */
		BufferedReader termReader = new BufferedReader(new FileReader(new File(
				input, "term.dict")));
		while ((line = termReader.readLine()) != null) {
			String[] tokens = line.split("\t");
			termDict.put(tokens[0], Integer.parseInt(tokens[1]));
		}
		termReader.close();

		/* Doc dictionary */
		BufferedReader docReader = new BufferedReader(new FileReader(new File(
				input, "doc.dict")));
		while ((line = docReader.readLine()) != null) {
			String[] tokens = line.split("\t");
			docDict.put(Integer.parseInt(tokens[1]), tokens[0]);
		}
		docReader.close();

		/* Posting dictionary */
		BufferedReader postReader = new BufferedReader(new FileReader(new File(
				input, "posting.dict")));
		while ((line = postReader.readLine()) != null) {
			String[] tokens = line.split("\t");
			posDict.put(Integer.parseInt(tokens[0]), Long.parseLong(tokens[1]));
			freqDict.put(Integer.parseInt(tokens[0]),
					Integer.parseInt(tokens[2]));
		}
		postReader.close();

		/* Processing queries */
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

		/* For each query */
		while ((line = br.readLine()) != null) {
			String[] tokens = line.split(" ");
			// for each token in query, creating a list of corresponding doc freq, to intersect in increasing order of freq (shortest two first)
			List<Pair<Integer, Integer>> listQueryTermFreq = new ArrayList<Pair<Integer,Integer>>();
			for (String token : tokens) {
				if(termDict.containsKey(token)){
					int termId = termDict.get(token);
					int docFreq = freqDict.get(termId);
					listQueryTermFreq.add(Pair.make(docFreq, termId));
				}else{
					System.out.println("no results found");
					return;
				}
			}
			
			//  this list sorted according to freq and added to a list of posting lists in increasing order
			Collections.sort(listQueryTermFreq, new Comparator<Pair<Integer, Integer>>() {

				@Override
				public int compare(Pair<Integer, Integer> o1, Pair<Integer, Integer> o2) {
						return o1.getFirst().compareTo(o2.getFirst());
				}
			});
			
			List<PostingList> listPostingLists = new ArrayList<PostingList>();
			
			FileChannel inFc = indexFile.getChannel();
			
			for (Pair<Integer, Integer> freqTermIdPair : listQueryTermFreq) {
				PostingList pl = readPosting(inFc, freqTermIdPair.getSecond());
				listPostingLists.add(pl);
			}
			
			List<Integer> resultDocIDs = intersectQueryPostings(listPostingLists);
			String[] strDocs = new String[resultDocIDs.size()];
			if(resultDocIDs!=null && resultDocIDs.size()>0){
				for (int i = 0; i<resultDocIDs.size();i++) {
					strDocs[i] = docDict.get(resultDocIDs.get(i));
				}
			}else{
				System.out.println("no results found");
			}
			Arrays.sort(strDocs);
			for (String string : strDocs) {
				System.out.println(string);;
			}

		}
		br.close();
		indexFile.close();
	}
	
	  static Integer popNextOrNull(Iterator<Integer> p) {
		    if (p.hasNext()) {
		      return p.next();
		    } else {
		      return null;
		    }
		  }

	private static List<Integer> intersectQueryPostings(
			List<PostingList> listPostingLists) {
		
		List<Integer> intersectIDs;
		if(listPostingLists.isEmpty()){
			return null;
		}
		intersectIDs = listPostingLists.get(0).getList();
			
		for(int i=1 ; i<listPostingLists.size(); i++){
			PostingList p2 = listPostingLists.get(i);
			
			Iterator<Integer> iterP1postings = intersectIDs.iterator();
			Iterator<Integer> iterP2postings = p2.getList().iterator();
			
			List<Integer> tempIntersect = new LinkedList<Integer>();
			
			Integer pp1 = popNextOrNull(iterP1postings);
			Integer pp2 = popNextOrNull(iterP2postings);

			while((pp1!=null) && (pp2!=null)){
				if(pp1.equals(pp2)){
					tempIntersect.add(pp1);
					pp1 = popNextOrNull(iterP1postings);
					pp2= popNextOrNull(iterP2postings);
				}else if(pp1<pp2){
					pp1 = popNextOrNull(iterP1postings);
				}else{
					pp2= popNextOrNull(iterP2postings);
				}
			}
			if(tempIntersect.size()<1){
				break;
			}
			intersectIDs = tempIntersect;
		}
		return intersectIDs;
		
	}
}
