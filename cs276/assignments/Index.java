package cs276.assignments;

import cs276.util.Pair;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.SortedMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import java.util.Arrays;


public class Index {

	// Term id -> (position in index file, doc frequency) dictionary
	private static Map<Integer, Pair<Long, Integer>> postingDict 
		= new TreeMap<Integer, Pair<Long, Integer>>();
	// Doc name -> doc id dictionary
	private static Map<String, Integer> docDict
		= new TreeMap<String, Integer>();
	// Term -> term id dictionary
	private static Map<String, Integer> termDict
		= new TreeMap<String, Integer>();
	// Block queue
	private static LinkedList<File> blockQueue
		= new LinkedList<File>();

	// Total file counter
	private static int totalFileCount = 0;
	// Document counter
	private static int docIdCounter = 0;
	// Term counter
	private static int wordIdCounter = 0;
	// Index
	private static BaseIndex index = null;
    private static BasicIndex basicindex = new BasicIndex();
	
	/* 
	 * Write a posting list to the file 
	 * You should record the file position of this posting list
	 * so that you can read it back during retrieval
	 * 
	 * Boolean argument is to determine whether we are writing to final index file, since we only
	 * want to update postingDict when writing this final version of the inverted index.
	 * */
	private static void writePosting(FileChannel fc, PostingList posting, boolean temp)
			throws IOException {
		if (!temp) {
			postingDict.put(posting.getTermId(), new Pair(fc.position(), posting.getList().size()));
			index.writePosting(fc, posting);
		}else{
			basicindex.writePosting(fc, posting);
		}
	}

	static Integer popNextOrNull(Iterator<Integer> p) {
		 if (p.hasNext()) {
	      	return p.next();
	    } else {
	      	return null;
	    }
	}

	private static PostingList mergePostings(PostingList p1, PostingList p2) {
		Iterator<Integer> i1 = p1.getList().iterator();
		Iterator<Integer> i2 = p2.getList().iterator();
		List<Integer> postings = new ArrayList();
		Integer id1 = popNextOrNull(i1);
		Integer id2 = popNextOrNull(i2);
		while ((id1 != null) && (id2 != null)) {
			if (id1 < id2) {
				postings.add(id1);
				id1 = popNextOrNull(i1);
			} else {
				postings.add(id2);
				id2 = popNextOrNull(i2);
			}
		}

		while (id1 != null) {
			postings.add(id1);
			id1 = popNextOrNull(i1);
		} 

		while (id2 != null) {
			postings.add(id2);
			id2 = popNextOrNull(i2);
		} 
		
		return new PostingList(p1.getTermId(), postings);
	}

	public static void main(String[] args) throws IOException {
             
		/* Parse command line */
		if (args.length != 3) {
			System.err
					.println("Usage: java Index [Basic|VB|Gamma] data_dir output_dir");
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

		/* Get root directory */
		String root = args[1];
		File rootdir = new File(root);
		if (!rootdir.exists() || !rootdir.isDirectory()) {
			System.err.println("Invalid data directory: " + root);
			return;
		}

		/* Get output directory */
		String output = args[2];
		File outdir = new File(output);
		if (outdir.exists() && !outdir.isDirectory()) {
			System.err.println("Invalid output directory: " + output);
			return;
		}

		if (!outdir.exists()) {
			if (!outdir.mkdirs()) {
				System.err.println("Create output directory failure");
				return;
			}
		}

		/* BSBI indexing algorithm */
		File[] dirlist = rootdir.listFiles();

		/* For each block */
		for (File block : dirlist) {
			if(!block.isDirectory()) continue;
			File blockFile = new File(output, block.getName());
			blockQueue.add(blockFile);

			File blockDir = new File(root, block.getName());
			File[] filelist = blockDir.listFiles();
			
			//Map<Integer, Set<Integer>> blockPostings = new TreeMap<Integer, Set<Integer>>();
			Set<Long> pairs = new TreeSet<Long>(); 
			/* For each file */
			for (File file : filelist) {
				if (file.getName().equals(".DS_Store")) continue; //skip this file
				++totalFileCount;
				String fileName = block.getName() + "/" + file.getName();
				int docId = docIdCounter;
				docDict.put(fileName, docIdCounter++);
				
				BufferedReader reader = new BufferedReader(new FileReader(file));
				String line;
				while ((line = reader.readLine()) != null) {
					String[] tokens = line.trim().split("\\s+"); //in case terms seperated by more than one space
					for (String token : tokens) {
						if (!termDict.containsKey(token)) {
							termDict.put(token, wordIdCounter++);
						}
						long tid = (long)termDict.get(token);
						pairs.add((tid << 32) + docId);
					}
				}
				reader.close();
			}

			/* Sort and output */
			if (!blockFile.createNewFile()) {
				System.err.println("Create new block failure.");
				return;
			}
			
			RandomAccessFile bfc = new RandomAccessFile(blockFile, "rw");
			FileChannel fc = bfc.getChannel();
			int numEntries = 0;
            int i =0, prv=-1;
            List<Integer> docList = new ArrayList<Integer>();
            for ( long ipair: pairs){
            	int first = (int)(ipair >> 32);
            	int sec =  (int)((ipair << 32)>>32);
            	if ( i == 0){
            		prv = first;
            		docList.add(sec);
            	}else{ 
            		if ( first != prv){
            			writePosting(fc, new PostingList( prv, docList), true);
            			docList.clear();
            			prv = first;
            			docList.add(sec);
            		}else{
            			docList.add(sec);
            		}
            	}
            	i++;
			}
			writePosting(fc, new PostingList( prv, docList), true);
			bfc.close();
		}

		/* Required: output total number of files. */
		System.out.println(totalFileCount);

		/* Merge blocks */
		while (true) {
			if (blockQueue.size() <= 1)
				break;

			File b1 = blockQueue.removeFirst();
			File b2 = blockQueue.removeFirst();
			
			File combfile = new File(output, b1.getName() + "+" + b2.getName());
			if (!combfile.createNewFile()) {
				System.err.println("Create new block failure.");
				return;
			}

			RandomAccessFile bf1 = new RandomAccessFile(b1, "r");
			RandomAccessFile bf2 = new RandomAccessFile(b2, "r");
			RandomAccessFile mf = new RandomAccessFile(combfile, "rw");
			 
			FileChannel fc1 = bf1.getChannel();
			FileChannel fc2 = bf2.getChannel();
			FileChannel mfc = mf.getChannel();

			PostingList p1 = basicindex.readPosting(fc1);
			PostingList p2 = basicindex.readPosting(fc2);

			while ((p1 != null) && (p2 != null)) {
				int termId1 = p1.getTermId(), termId2 = p2.getTermId();
				if (termId1 == termId2) {
					PostingList merged = mergePostings(p1, p2);
					writePosting(mfc, merged, blockQueue.size() != 0);
					p1 = basicindex.readPosting(fc1);
					p2 = basicindex.readPosting(fc2);
				} else if (termId1 < termId2) {
					writePosting(mfc, p1, blockQueue.size() != 0);
					p1 = basicindex.readPosting(fc1);
				} else {
					writePosting(mfc, p2, blockQueue.size() != 0);
					p2 = basicindex.readPosting(fc2);
				}
			}

			while (p1 != null) { 
				writePosting(mfc, p1, blockQueue.size() != 0);
				p1 = basicindex.readPosting(fc1);
			}

			while (p2 != null) {
				writePosting(mfc, p2, blockQueue.size() != 0);
				p2 = basicindex.readPosting(fc2);
			}

			
			bf1.close();
			bf2.close();
			mf.close();
			b1.delete();
			b2.delete();
			blockQueue.add(combfile);
		}

		/* Dump constructed index back into file system */
		File indexFile = blockQueue.removeFirst();
		indexFile.renameTo(new File(output, "corpus.index"));

		BufferedWriter termWriter = new BufferedWriter(new FileWriter(new File(
				output, "term.dict")));
		for (String term : termDict.keySet()) {
			termWriter.write(term + "\t" + termDict.get(term) + "\n");
		}
		termWriter.close();

		BufferedWriter docWriter = new BufferedWriter(new FileWriter(new File(
				output, "doc.dict")));
		for (String doc : docDict.keySet()) {
			docWriter.write(doc + "\t" + docDict.get(doc) + "\n");
		}
		docWriter.close();

		BufferedWriter postWriter = new BufferedWriter(new FileWriter(new File(
				output, "posting.dict")));
		for (Integer termId : postingDict.keySet()) {
			postWriter.write(termId + "\t" + postingDict.get(termId).getFirst()
					+ "\t" + postingDict.get(termId).getSecond() + "\n");
		}
		postWriter.close();
	}

}
