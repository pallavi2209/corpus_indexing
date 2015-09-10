package cs276.assignments;

import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;



public class BasicIndex implements BaseIndex {

	private static final int INT_BYTES = Integer.SIZE / Byte.SIZE;

	@Override
	public PostingList readPosting(FileChannel fc) throws IOException {
		//read docFreq and termId for posting 
		ByteBuffer buf = ByteBuffer.allocate(INT_BYTES * 2);
		if (fc.read(buf) < 0) return null;
		buf.flip();
		int docFreq = buf.getInt();
		int termId = buf.getInt();
		
		//write list of docIds into buffer
		ByteBuffer postingsBuf = ByteBuffer.allocate(INT_BYTES * docFreq);
		List<Integer> postings = new ArrayList<Integer>();
		fc.read(postingsBuf);
		postingsBuf.flip(); //flip to begin reading from buffer
		
		//read docIds from buffer and construct list
		for (int i = 0; i < docFreq; i++) {
			postings.add(postingsBuf.getInt());
		
		}

		//return constructed postings list
		return new PostingList(termId, postings);
	}

	@Override
	public void writePosting(FileChannel fc, PostingList p) throws IOException {
		
		List<Integer> postings = p.getList();
		int docFreq = postings.size();
		
		//allocate buffer for docFreq, termId, and postings
		ByteBuffer buf = ByteBuffer.allocate(INT_BYTES * (docFreq + 2));
		
		//write posting into buffer
		buf.putInt(docFreq);
		buf.putInt(p.getTermId());
		for (int i = 0; i < docFreq; i++) {
			buf.putInt(postings.get(i));
		}
		
		buf.rewind(); //rewind to write to channel
		fc.write(buf);
	}
}
