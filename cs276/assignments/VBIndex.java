package cs276.assignments;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class VBIndex implements BaseIndex {
	
	private static final int INT_BYTES = Integer.SIZE / Byte.SIZE;
	public static final int INVALID_VBCODE = -1;
	
	
	int[] toIntArray(List<Integer> list)  {
	    int[] ret = new int[list.size()];
	    int i = 0;
	    for (Integer e : list)  
	        ret[i++] = e.intValue();
	    return ret;
	}
	
	List<Integer> toIntegerList(int[] array){
		List<Integer> list = new ArrayList<Integer>(array.length);
		for (int i : array) {
			list.add(i);
		}
		return list;
	}
	
	public static void GapEncode(int[] inputDocIdsOutputGaps, int numDocIds) {
		
		for(int i= numDocIds-1; i>0; i--){
			inputDocIdsOutputGaps[i] =inputDocIdsOutputGaps[i]- inputDocIdsOutputGaps[i-1];
		}
	}
	
	public static void GapDecode(int[] inputGapsOutputDocIds, int numGaps) {
		for(int i= 1; i<numGaps; i++){
			inputGapsOutputDocIds[i] =inputGapsOutputDocIds[i]+ inputGapsOutputDocIds[i-1];
		}
	}
	
	/**
	 * Encodes gap using a VB code.  The encoded bytes are placed in outputVBCode.  Returns the number
	 * bytes placed in outputVBCode.
	 * 
	 * @param gap            gap to be encoded.  Assumed to be greater than or equal to 0.
	 * @param outputVBCode   VB encoded bytes are placed here.  This byte array is assumed to be large
	 * 						 enough to hold the VB code for gap (e.g., Integer.SIZE/7 + 1).
	 * @return				 Number of bytes placed in outputVBCode.
	 */
	
	public static int VBEncodeInteger(int gap, byte[] outputVBCode) {
		int numBytes = 0;
		byte mask = 0x7f;
		do {
			byte vbCode = (byte) (mask & gap);
			if (numBytes == 0) {
				vbCode = (byte) (vbCode | 0x80);
			}
			for (int i = numBytes; i > 0; i--) {
				outputVBCode[i] = outputVBCode[i - 1];
			}
			outputVBCode[0] = vbCode;

			gap = gap >>> 7;
			numBytes++;
		} while (gap > 0);

		return numBytes;
	}
	
	/**
	 * Decodes the first integer encoded in inputVBCode starting at index startIndex.  The decoded
	 * number is placed in the first element of the numberEndIndex array and the index position
	 * immediately after the encoded value is placed in the second element of numberEndIndex.
	 * 
	 * @param inputVBCode     Byte array containing the VB encoded number starting at index startIndex.
	 * @param startIndex      Index in inputVBCode where the VB encoded number starts
	 * @param numberEndIndex  Outputs are placed in this array.  The first element is set to the
	 * 						  decoded number (or INVALID_VBCODE if there's a problem) and the second
	 * 						  element is set to the index of inputVBCode immediately after the end of
	 * 						  the VB encoded number.
	 */
	public static void VBDecodeInteger(byte[] inputVBCode, int startIndex,
			int[] numberEndIndex) {
		boolean lastByte = false;
		int index = startIndex;
		int result = 0;
		do {
			if ((inputVBCode[index] & 0x80) == 128) {
				inputVBCode[index] = (byte) (inputVBCode[index] & 0x7f);
				lastByte = true;
			}
			result = (result << 7) + inputVBCode[index];
			index++;
			if (index >= inputVBCode.length && !lastByte) {
				result = INVALID_VBCODE;
				index = 0;
				break;
			}
		} while (!lastByte);
		numberEndIndex[0] = result;
		numberEndIndex[1] = index;
	}

	@Override
	public PostingList readPosting(FileChannel fc) throws IOException {
		//read docFreq and termId for posting 
		ByteBuffer buf = ByteBuffer.allocate(INT_BYTES * 2);
		int numbytesread = fc.read(buf);
		if ( numbytesread < 8) return null;
		buf.flip();
		int docFreq = buf.getInt();
		int termId = buf.getInt();
		
		//write list of docIds into buffer
		ByteBuffer postingsBuf = ByteBuffer.allocate(INT_BYTES * docFreq);
		int[] postingGaps = new int[docFreq];
		fc.read(postingsBuf);
		postingsBuf.flip(); //flip to begin reading from buffer
		
		int startIndex = 0;
		//read docIds from buffer and construct list
		for (int i = 0; i < docFreq; i++) {
			int[] numberEndIndex = new int[2];;
			VBDecodeInteger(postingsBuf.array(), startIndex, numberEndIndex);
			postingGaps[i] = numberEndIndex[0];
			startIndex = numberEndIndex[1];
		}
		
		GapDecode(postingGaps, docFreq);
		List<Integer> postings = toIntegerList(postingGaps);
        // seek the file channel to  INT_BYTES * docFreq - startIndex
		long cp = fc.position();
		
		long up = cp - (INT_BYTES * docFreq) + startIndex;
		fc.position(up);
		
		return new PostingList(termId, postings);
	}

	@Override
	public void writePosting(FileChannel fc, PostingList p) throws IOException {
		
		List<Integer> postings = p.getList();
		int docFreq = postings.size();
		
		//allocate buffer for docFreq, termId, and postings
		ByteBuffer buf = ByteBuffer.allocate(INT_BYTES * (2));
		
		//write posting into buffer
		buf.putInt(docFreq);
		buf.putInt(p.getTermId());
		buf.rewind();
		fc.write(buf);
		
		int[] gapsFromDocIds = toIntArray(postings);
		GapEncode(gapsFromDocIds, docFreq);
		
		for (int gap : gapsFromDocIds) {
			byte encodedGap[] = new byte[Integer.SIZE / 7 + 1];
			int numBytes = VBEncodeInteger(gap, encodedGap);

			ByteBuffer encodedGapBuffer = ByteBuffer.allocate(numBytes);
			encodedGapBuffer.put(encodedGap, 0, numBytes);
			encodedGapBuffer.rewind(); // rewind to write to channel
			fc.write(encodedGapBuffer);
		}
		
	}
}
