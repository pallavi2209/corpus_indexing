package cs276.assignments;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

public class GammaIndex implements BaseIndex {

	private static final int INT_BYTES = Integer.SIZE / Byte.SIZE;
	
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
	
	public static BitSet fromByteArray(byte[] bytes) {
	    BitSet bits = new BitSet();
	    for (int i=0; i<bytes.length*8; i++) {
	        if ((bytes[i/8]&(1<<(i%8))) > 0) {
	            bits.set(i);
	        }
	    }
	    return bits;
	}

	
	public static byte[] toByteArray(BitSet bits, int lengthToRead) 
	{
	    byte[] bytes = new byte[(lengthToRead + 7) / 8];       
	    for (int i = 0; i < lengthToRead; i++) {
	        if (bits.get(i)) {
	            bytes[ i / 8 ] |= 1 << (i % 8);
	        }
	    }
	    return bytes;
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
	 * Encodes a number using unary code.  The unary code for the number is placed in the BitSet
	 * outputUnaryCode starting at index startIndex.  The method returns the BitSet index that
	 * immediately follows the end of the unary encoding.  Use startIndex = 0 to place the unary
	 * encoding at the beginning of the outputUnaryCode.
	 * <p>
	 * Examples:
	 * If number = 5, startIndex = 3, then unary code 111110 is placed in outputUnaryCode starting
	 * at the 4th bit position and the return value 9.
	 *  
	 * @param number           The number to be unary encoded
	 * @param outputUnaryCode  The unary code for number is placed into this BitSet
	 * @param startIndex       The unary code for number starts at this index position in outputUnaryCode
	 * @return                 The next index position in outputUnaryCode immediately following the unary code for number
	 */
	public static int UnaryEncodeInteger(int number, BitSet outputUnaryCode, int startIndex) {
		int nextIndex = startIndex;
		int endIndex = startIndex+number;
		for(int i = startIndex; i<(startIndex+number);i++){
			outputUnaryCode.set(i, true);
		}
		outputUnaryCode.set(endIndex, false);
		nextIndex = endIndex+1;
		return nextIndex;
	}

	/**
	 * Decodes the unary coded number in BitSet inputUnaryCode starting at (0-based) index startIndex.
	 * The decoded number is returned in numberEndIndex[0] and the index position immediately following
	 * the encoded value in inputUnaryCode is returned in numberEndIndex[1].
	 * 
	 * @param inputUnaryCode  BitSet containing the unary code
	 * @param startIndex      Unary code starts at this index position
	 * @param numberEndIndex  Return values: index 0 holds the decoded number; index 1 holds the index
	 *                        position in inputUnaryCode immediately following the unary code.
	 */
	public static void UnaryDecodeInteger(BitSet inputUnaryCode, int startIndex, int[] numberEndIndex) {
		int currIndex= startIndex;
		int lastUnaryIndex=0;
		while(true){
			if(!(inputUnaryCode.get(currIndex))){
				lastUnaryIndex = currIndex;
				break;
			}
			currIndex++;
		}
		numberEndIndex[0]=inputUnaryCode.get(startIndex, lastUnaryIndex).length();
		numberEndIndex[1]= lastUnaryIndex+1;
	}

	/**
	 * Gamma encodes number.  The encoded bits are placed in BitSet outputGammaCode starting at
	 * (0-based) index position startIndex.  Returns the index position immediately following the
	 * encoded bits.  If you try to gamma encode 0, then the return value should be startIndex (i.e.,
	 * it does nothing).
	 * 
	 * @param number            Number to be gamma encoded
	 * @param outputGammaCode   Gamma encoded bits are placed in this BitSet starting at startIndex
	 * @param startIndex        Encoded bits start at this index position in outputGammaCode
	 * @return                  Index position in outputGammaCode immediately following the encoded bits
	 */
	public static int GammaEncodeInteger(int number, BitSet outputGammaCode, int startIndex) {
		if(number == 0){
			return startIndex;
		}

		int nextIndex = startIndex;
		int indexFirstBit = -1;

		for(int n=31; n>=0; n--){
			int mask = 0x01 << n;
			if((number & mask) == mask){
				indexFirstBit = n;
				break;
			}
		}

		nextIndex = UnaryEncodeInteger(indexFirstBit, outputGammaCode, startIndex);
		for (int i = indexFirstBit - 1; i >= 0; i--) {
			int mask = 0x01 << i;
			if ((number & mask) == mask) {
				outputGammaCode.set(nextIndex, true);
			} else {
				outputGammaCode.set(nextIndex, false);
			}
			nextIndex++;
		}

		// Fill in your code here
		return nextIndex;
	}

	/**
	 * Decodes the Gamma encoded number in BitSet inputGammaCode starting at (0-based) index startIndex.
	 * The decoded number is returned in numberEndIndex[0] and the index position immediately following
	 * the encoded value in inputGammaCode is returned in numberEndIndex[1].
	 * 
	 * @param inputGammaCode  BitSet containing the gamma code
	 * @param startIndex      Gamma code starts at this index position
	 * @param numberEndIndex  Return values: index 0 holds the decoded number; index 1 holds the index
	 *                        position in inputGammaCode immediately following the gamma code.
	 */
	public static void GammaDecodeInteger(BitSet inputGammaCode, int startIndex, int[] numberEndIndex) {
		
		int[] numberEndIndexUnary = new int[2];
		UnaryDecodeInteger(inputGammaCode, startIndex, numberEndIndexUnary );
		int unaryNumber = numberEndIndexUnary[0];
		int nextIndex= numberEndIndexUnary[1];
		
		int decodedNumber= 1;
		for( int i = 0; i< unaryNumber; i++){
			  decodedNumber = (decodedNumber << 1) +  (inputGammaCode.get(nextIndex + i) ? 1 : 0);
		}
		numberEndIndex[0] = decodedNumber;
		numberEndIndex[1] = nextIndex + unaryNumber;
	}
		
	@Override
	public PostingList readPosting(FileChannel fc) throws IOException {
		//read docFreq and termId for posting 
		ByteBuffer buf = ByteBuffer.allocate(INT_BYTES * 3);
		int numbytesread = fc.read(buf);
		if ( numbytesread < 12) return null;
		buf.flip();
		int docFreq = buf.getInt();
		int termId = buf.getInt();
		int[] postingGaps = new int[docFreq];
		postingGaps[0] = buf.getInt();
		
		if ( docFreq > 1){
			//write list of docIds into buffer
			ByteBuffer postingsBuf = ByteBuffer.allocate(INT_BYTES * (docFreq-1));

			fc.read(postingsBuf);
			postingsBuf.flip(); //flip to begin reading from buffer

			int startIndex = 0;
			BitSet inputGammaCode = fromByteArray(postingsBuf.array());

			//read docIds from buffer and construct list
			for (int i = 1; i < docFreq; i++) {
				int[] numberEndIndex = new int[2];
				GammaDecodeInteger(inputGammaCode, startIndex, numberEndIndex );
				postingGaps[i] = numberEndIndex[0];
				startIndex = numberEndIndex[1];
			}


			// seek the file channel to  INT_BYTES * docFreq - startIndex
			long cp = fc.position();

			//int bytesRead = (int) Math.ceil(( (double) startIndex) / 8.0);
			int bytesRead = (startIndex + 7) / 8 ; 
			long up = cp - (INT_BYTES * (docFreq-1)) + bytesRead;
			fc.position(up);
		}
		
		GapDecode(postingGaps, docFreq);
		List<Integer> postings = toIntegerList(postingGaps);
		return new PostingList(termId, postings);
	}

	@Override
	public void writePosting(FileChannel fc, PostingList p) throws IOException {
		
		List<Integer> postings = p.getList();
		int docFreq = postings.size();
		
		//allocate buffer for docFreq, termId, and postings
		ByteBuffer buf = ByteBuffer.allocate(INT_BYTES * 3);
		
		//write posting into buffer
		buf.putInt(docFreq);
		buf.putInt(p.getTermId());
		buf.putInt(postings.get(0));
		buf.rewind();
		fc.write(buf);
		
		if ( docFreq > 1){
			int[] gapsFromDocIds = toIntArray(postings);
			GapEncode(gapsFromDocIds, docFreq);
			BitSet encodedGaps = new BitSet();
			int startIndex = 0;
			for (int i =1; i< gapsFromDocIds.length ; i++) {
				int gap = gapsFromDocIds[i];
				int endIndex = GammaEncodeInteger(gap, encodedGaps, startIndex);
				startIndex = endIndex;
			}
			
			byte[] encodedBytes = toByteArray(encodedGaps, startIndex);
			
			ByteBuffer encodedGapBuffer = ByteBuffer.allocate(encodedBytes.length);
			encodedGapBuffer.put(encodedBytes);
			encodedGapBuffer.rewind(); // rewind to write to channel
			fc.write(encodedGapBuffer);
		}
	}

}
