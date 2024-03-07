package iterator;

import BigT.*;
import btree.AddFileEntryException;
import btree.ConstructPageException;
import btree.ConvertException;
import btree.DeleteFileEntryException;
import btree.DeleteRecException;
import btree.FreePageException;
import btree.GetFileEntryException;
import btree.IndexInsertRecException;
import btree.IndexSearchException;
import btree.InsertException;
import btree.IteratorException;
import btree.KeyNotMatchException;
import btree.KeyTooLongException;
import btree.LeafDeleteException;
import btree.LeafInsertRecException;
import btree.NodeNotMatchException;
import btree.PinPageException;
import btree.UnpinPageException;
import global.*;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.SpaceNotAvailableException;
import heap.Tuple;
import diskmgr.*;
import bufmgr.*;
import index.*;
import java.io.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

/**
 * This file contains the interface for the sort_merg joins. This file contains
 * an implementation of the sort merge join for two bigTables It makes use of
 * the external sorting utility to generate runs, and then uses the iterator
 * interface to get successive tuples for the final merge.
 */
public class SortMergeMap extends MapIterator implements GlobalConst {
	private FileScanMap stream1;
	private FileScanMap stream2;
	bigt outputBT;
	private Map map1, map2;
	private Map TempMap1, TempMap2;
	private boolean done;
	private boolean get_from_s1, get_from_s2;
	private boolean process_next_block;
	private int _n_pages;
	private byte _bufs1[][], _bufs2[][];
	private IoBufMap io_buf1, io_buf2;
	private Heapfile temp_file_fd1, temp_file_fd2;
	private String bigtable1Name = "";
	private String bigtable2Name = "";
	private String outputBigTName = "";
	private int size = 82;

	public SortMergeMap(String bnames1, String bnames2, FileScanMap s1, FileScanMap s2, String outBigTableName)
			throws JoinNewFailed, JoinLowMemory, SortException,

			IOException, HFException, HFBufMgrException, HFDiskMgrException, GetFileEntryException,
			ConstructPageException, AddFileEntryException, IteratorException, UnpinPageException, FreePageException,
			DeleteFileEntryException, PinPageException, PageUnpinnedException, InvalidFrameNumberException,
			HashEntryNotFoundException, ReplacerException {

		this.bigtable1Name = bnames1;
		this.bigtable2Name = bnames2;
		this.outputBigTName = outBigTableName;
		this.stream1 = s1;
		this.stream2 = s2;
		this.outputBT = new bigt(outBigTableName, 1);

		this.get_from_s1 = true;
		this.get_from_s2 = true;

		// open io_bufs
		this.io_buf1 = new IoBufMap();
		this.io_buf2 = new IoBufMap();

		// value size = 20 (Same as that set in stream)
		this.TempMap1 = new Map();
		this.TempMap2 = new Map();
		this.map1 = new Map();
		this.map2 = new Map();

		if (io_buf1 == null || io_buf2 == null || TempMap1 == null || TempMap2 == null || map1 == null || map1 == null)
			throw new JoinNewFailed("SortMerge.java: allocate failed");

		this.process_next_block = true;
		this.done = false;
		// Two buffer pages to store equivalence classes
		// NOTE -- THESE PAGES ARE NOT OBTAINED FROM THE BUFFER POOL
		// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		this._n_pages = 5;
		this._bufs1 = new byte[_n_pages][MINIBASE_PAGESIZE];
		this._bufs2 = new byte[_n_pages][MINIBASE_PAGESIZE];

		this.temp_file_fd1 = null;
		this.temp_file_fd2 = null;
		try {
			temp_file_fd1 = new Heapfile("tempfile1");
			temp_file_fd2 = new Heapfile("tempfile2");
		} catch (Exception e) {
			throw new SortException(e, "Create heap file failed");
		}

	}

	/**
	 * The map is returned All this function has to do is to get 1 map from one of
	 * the Streams (from both initially), use the sorting order to determine which
	 * one gets sent up.) Hmmm it seems that some thing more has to be done in order
	 * to account for duplicates.... => I am following Raghu's 564 notes in order to
	 * obtain an algorithm for this merging. Some funda about "equivalence classes"
	 * 
	 * @return the joined map is returned
	 * @exception IOException             I/O errors
	 * @exception JoinsException          some join exception
	 * @exception IndexException          exception from super class
	 * @exception InvalidMapSizeException invalid tuple size
	 * @exception PageNotReadException    exception from lower layer
	 * @exception MapUtilsException       exception from using tuple utilities
	 * @exception SortException           sort exception
	 * @exception LowMemException         memory error
	 * @exception Exception               other exceptions
	 */
//	public HashMap<String, List<Version>>  performJoin(HashMap<String, List<Version>>  outputMapVersion) throws IOException,
//	   IndexException,
//	 
//	   PageNotReadException,
//	   SortException,
//	   LowMemException,
//	   JoinsException,
//	   Exception{
//
//	   	int comp_res;
//      	Map _map1, _map2;
//      	int map1_size, map2_size;
//
////      	System.out.println("Before done");
//      	if (done) return outputMapVersion;
//      	
//      	
////      	System.out.println("Starting sortmerge while loop");
//      	while(true){
//      		if(process_next_block){
////      			System.out.println("Entered next block");
//      			process_next_block = false;
//      			if(get_from_s1)
////      				System.out.println("stream1.getNext() returned null");
//      				if((map1 = stream1.get_next()) == null){
//      					done = true;
////      					System.out.println("stream1.getNext() returned null");
//      					return outputMapVersion;
//      				}
//      			if(get_from_s2)
//      				if((map2 = stream2.get_next()) == null){
//      					done = true;
//      					//System.out.println("stream2.getNext() returned null");
//      					return outputMapVersion;
//      				}
//      			get_from_s1 = get_from_s2 = false;
//
//      			
//      			//compare the maps on value (orderType = 8)
//      			map1.setFldOffset(map1.getMapByteArray());
//      			map2.setFldOffset(map2.getMapByteArray());
////      			System.out.println("Mappppp sizeeee" + map1.size());
////      			System.out.println("Mappppp sizeeee 2" + map2.size());
//      			comp_res = MapUtils.CompareMapWithMapValues(map1, map2);
//
//				//Assuming ascending order
//				//Iterate on outer 
//      			while(comp_res < 0){
//      				//map1 value < map2 value
////      				System.out.println("Compare result < 0");
//      				if((map1 = stream1.get_next()) == null){
//      					done = true;
//      					return outputMapVersion;
//      				}
//      				comp_res = MapUtils.CompareMapWithMapValues(map1, map2);
//      			}
//
//      			comp_res = MapUtils.CompareMapWithMapValues(map1, map2);
//      			
//      			//Iterate over inner
//      			while(comp_res > 0){
//      				//map1 value > map2 value
////      				System.out.println("Compare result > 0");
//      				if((map2 = stream2.get_next()) == null){
//      					done = true;
//      					return outputMapVersion;
//      				}
//      				comp_res = MapUtils.CompareMapWithMapValues(map1, map2);
//      			}
//
//      			if(comp_res != 0){
//      				process_next_block = true;
//      				continue;
//      			}
//
//      			//com_res = 0 
//
//      			
//
//      			io_buf1.init(_bufs1, 1, size, temp_file_fd1);
//      			io_buf2.init(_bufs2, 1, size, temp_file_fd2);
//      			
//      			
//      			
//      			
//      			map1.setFldOffset(map1.getMapByteArray());
//      			TempMap1 = new Map();
//      			
//      			TempMap1.setDefaultHdr();
//      			TempMap1.setRowLabel(map1.getRowLabel());
//      			TempMap1.setColumnLabel(map1.getColumnLabel());
//      			TempMap1.setTimeStamp(map1.getTimeStamp());
//      			TempMap1.setValue(map1.getValue());
//      			
//      			map2.setFldOffset(map1.getMapByteArray());
//      			TempMap2 = new Map();
//      			
//      			TempMap2.setDefaultHdr();
//      			TempMap2.setRowLabel(map2.getRowLabel());
//      			TempMap2.setColumnLabel(map2.getColumnLabel());
//      			TempMap2.setTimeStamp(map2.getTimeStamp());
//      			TempMap2.setValue(map2.getValue());
//      			
//      		
//      			
//      			TempMap1.setFldOffset(TempMap1.getMapByteArray());
//      			
//				 
//				
//      			while(MapUtils.CompareMapWithMapValues(map1, TempMap1) == 0){
//      				//Insert map1 into io_buf1
//      				try {
//		    			io_buf1.Put(map1);
//		    			//System.out.print("Putting map in io_buf1 ");
//		    			//map1.print();
//		  			}
//		  			catch (Exception e){
//		    			throw new JoinsException(e,"IoBuf error in sortmerge");
//		  			}
//		  			if((map1 = stream1.get_next()) == null){
//		  				get_from_s1 = true;
//		  				break;
//		  			}
//      			}
//
//      			
//      			TempMap2.setFldOffset(TempMap2.getMapByteArray());
//      			while(MapUtils.CompareMapWithMapValues(map2, TempMap2) == 0){
//      				//Insert map2 into io_buf2
//      				try {
//		    			io_buf2.Put(map2);
//		    			//System.out.print("Putting map in io_buf2 ");
//		    			//map2.print();
//		  			}
//		  			catch (Exception e){
//		    			throw new JoinsException(e,"IoBuf error in sortmerge");
//		  			}
//		  			if((map2 = stream2.get_next()) == null){
//		  				get_from_s2 = true;
//		  				break;
//		  			}
//      			}
//
//      			// map1 and map2 contain the next map to be processed after this set.
//	      		// Now perform a join of the maps in io_buf1 and io_buf2.
//	      		// This is going to be a simple nested loops join with no frills. I guess,
//	      		// it can be made more efficient.
//			    // Another optimization that can be made is to choose the inner and outer
//			    // by checking the number of tuples in each equivalence class.
//
//			    if ((_map1=io_buf1.Get(TempMap1)) == null)                // Should not occur
//					System.out.println( "Equiv. class 1 in sort-merge has no maps");
//      		}
//
//      		if ((_map2 = io_buf2.Get(TempMap2)) == null){
//	      		if ((_map1 = io_buf1.Get(TempMap1)) == null){
//		  			process_next_block = true;
//		  			//io_buf1.i_buf.close();
//		  			continue;                                // Process next equivalence class
//				}
//	      		else{
//	      			//io_buf2.i_buf.close();
//		  			io_buf2.reread();
//		  			_map2= io_buf2.Get(TempMap2);
//				}
//	    	}
//
//	    	//Join the two maps
//	    	if(MapUtils.CompareMapWithMapValues(TempMap1, TempMap2) == 0){
//	    		String row1 = TempMap1.getRowLabel();
//	    		String row2 = TempMap2.getRowLabel();
//	    		String newRow = row1 + ":" + row2;
//	    		String newCol = TempMap1.getColumnLabel();
//	    		String value = TempMap1.getValue();
//	    		//int minTimeStamp = Math.min(TempMap1.getTimeStamp(), TempMap2.getTimeStamp());
//	    		System.out.println("Joining two maps");
//				TempMap1.print();
//				TempMap2.print();
//				System.out.println();
//				//Create two temp bigStreams to get all the columns for the matched row(in joined map) in each bigT
//	    		//Ordered on row labels
//				
//	    		FileScanMap tempbs1 = new FileScanMap(bigtable1Name,null,null,false);
//	    		FileScanMap tempbs2 = new FileScanMap(bigtable2Name,null,null,false);
//
//				//Arraylist to store all the versions of two matched maps (Max size = 6)
//				ArrayList<Map> versions = new ArrayList<Map>();
//				Map nextMap;
//				
//				MapInsert mapinsert = new MapInsert(outputBT, 1, outputBigTName);
//				//iterate over temp bigstream1 and get all the columns for matched row
//				while(true){
//					if((nextMap = tempbs1.get_next()) == null){
//						//System.out.println("Getting all columns for matched row in tempbs1");
//						break;
//					}
//					if(nextMap.getColumnLabel().equals(newCol)){
//						versions.add(nextMap);
//					}
//					else{
//						Map temp = new Map(nextMap);
//						temp.setRowLabel(newRow);
//						String tempCol = temp.getColumnLabel();
//						temp.setColumnLabel(tempCol + "_left");
//						System.out.println("Inserting ");
//					
//						System.out.println();
//						temp.setFldOffset(temp.getMapByteArray());
//						outputMapVersion = mapinsert.run(temp.getRowLabel(), temp.getColumnLabel(), temp.getValue(), temp.getTimeStamp(), outputMapVersion);
//					}
//				}
//
//				//iterate over temp bigstream2 and get all the columns for matched row
//				while(true){
//					if((nextMap = tempbs2.get_next()) == null){
//						break;
//					}
//					if(nextMap.getColumnLabel().equals(newCol)){
//						//if(!(nextMap.getTimeStamp() == minTimeStamp))
//							versions.add(nextMap);
//					}
//					else{
//						Map temp = new Map(nextMap);
//						temp.setRowLabel(newRow);
//						String tempCol = temp.getColumnLabel();
//						temp.setColumnLabel(tempCol + "_right");
//						System.out.print("Inserting ");
//						
//						temp.setFldOffset(temp.getMapByteArray());
//						outputMapVersion = mapinsert.run(temp.getRowLabel(), temp.getColumnLabel(), temp.getValue(), temp.getTimeStamp(), outputMapVersion);
//						
//					}
//				}
//				try{
//					tempbs1.close();
//					tempbs2.close();
//				}
//				catch(Exception e){
//					throw new JoinsException(e, "SortMerge.java: error in closing temp bigstreams");
//				}
//				
////				/*Till here,the matching maps have been output. Now we need to output
////				the 3 most recent versions of the joined maps*/
////
////				ArrayList<Integer> timestamps = new ArrayList<Integer>();
////				for(int i = 0; i<versions.size(); i++)
////					timestamps.add(versions.get(i).getTimeStamp());
////
////				int counter = 0;
////				while(!versions.isEmpty()){
////					int maxIndex = timestamps.indexOf(Collections.max(timestamps));
////					counter++;
////					if(counter<=3){
////						Map toinsert = versions.get(maxIndex);
////						toinsert.setRowLabel(newRow);
////						System.out.print("Inserting ");
////						toinsert.print();
////						outputBT.insertMap(toinsert.getMapByteArray());
////						versions.remove(maxIndex);
////						timestamps.remove(maxIndex);
////					}
////					else
////						break;
////				}
//				
//	    	}
//      	}
//	}
	public void close() throws JoinsException, IOException, IndexException {

		if (!closeFlag) {
			try {
				stream1.close();
				stream2.close();
				// System.out.println("Closed two streams");
			} catch (Exception e) {
				throw new JoinsException(e, "SortMerge.java: error in closing streams.");
			}

			// try{
			// io_buf1.close();
			// io_buf2.close();
			// }
			// catch(Exception e){
			// throw new JoinsException(e, "SortMerge.java: error in closing IoBuf");
			// }

			// try{
			// io_buf1.i_buf.close();
			// }
			// catch(Exception e){
			// throw new JoinsException(e, "Error closing io_buf1");
			// }

			if (temp_file_fd1 != null) {
				try {
					temp_file_fd1.deleteFileMap();
				} catch (Exception e) {
					throw new JoinsException(e, "SortMerge.java: delete file failed");
				}
				temp_file_fd1 = null;
			}
			if (temp_file_fd2 != null) {
				try {
					temp_file_fd2.deleteFileMap();
				} catch (Exception e) {
					throw new JoinsException(e, "SortMerge.java: delete file failed");
				}
				temp_file_fd2 = null;
			}
			closeFlag = true;
		}

	}

	@Override
	public Map get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
			InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
		// TODO Auto-generated method stub
		return null;
	}

	public HashMap<String, List<Version>> performJoin2(HashMap<String, List<Version>> outputMapVersion)
			throws IOException, IndexException,

			PageNotReadException, SortException, LowMemException, JoinsException, Exception {

		SortMap outSortMap = new SortMap(null, null, null, stream1, 7, new MapOrder(MapOrder.Ascending), null, 100);
		SortMap inSortMap = new SortMap(null, null, null, stream2, 7, new MapOrder(MapOrder.Ascending), null, 100);

		ArrayList<Map> mark = new ArrayList<Map>();

		Map outMap = new Map();
		Map inMap = new Map();
		String markValue = "";

		Map refMap = new Map();

		outMap = outSortMap.get_next();
		inMap = inSortMap.get_next();
		Boolean newAddition = false;
//        int i=0;
		outMap.setFldOffset(outMap.getMapByteArray());
		inMap.setFldOffset(inMap.getMapByteArray());

		String outValue = outMap.getValue();
		String inValue = inMap.getValue();
		MapInsert mapinsert = new MapInsert(outputBT, 1, outputBigTName);

		if (inMap == null) {

		} else {
			while (outMap != null) {

				if (mark.isEmpty()) {

					while (outValue.compareTo(inValue) < 0) {
						outMap = outSortMap.get_next();
						if (outMap == null) {
							return outputMapVersion;
						}
						outMap.setFldOffset(outMap.getMapByteArray());

						outValue = outMap.getValue();

					}

					while (outValue.compareTo(inValue) > 0) {
						inMap = inSortMap.get_next();
						if (inMap == null) {
							if (inValue.equals(outValue)) {

							} else {
								return outputMapVersion;
							}
						} else {
							inMap.setFldOffset(inMap.getMapByteArray());
							inValue = inMap.getValue();
						}

					}

					if (mark.isEmpty()) {
						Map newMap = new Map();

						inMap.setFldOffset(inMap.getMapByteArray());
						String newRow = inMap.getRowLabel();
						String newVal = inMap.getValue();
						String newCol = inMap.getColumnLabel();
						int newTime = inMap.getTimeStamp();

						newMap.setDefaultHdr();
						newMap.setColumnLabel(newCol);
						newMap.setRowLabel(newRow);
						newMap.setValue(newVal);
						newMap.setTimeStamp(newTime);

						mark.add(newMap);
						markValue = newMap.getValue();
						newAddition = true;
					}
				}

				if (outValue.equals(inValue)) {

					outputMapVersion = insertToTable(inMap, outMap, mapinsert, outputMapVersion);

					if (!newAddition) {
						Map newMap = new Map();

						inMap.setFldOffset(inMap.getMapByteArray());
						String newRow = inMap.getRowLabel();
						String newVal = inMap.getValue();
						String newCol = inMap.getColumnLabel();
						int newTime = inMap.getTimeStamp();

						newMap.setDefaultHdr();
						newMap.setColumnLabel(newCol);
						newMap.setRowLabel(newRow);
						newMap.setValue(newVal);
						newMap.setTimeStamp(newTime);

						mark.add(newMap);

					}
					inMap = inSortMap.get_next();
					if (inMap == null) {
						inValue = "";

					} else {
						inMap.setFldOffset(inMap.getMapByteArray());
						inValue = inMap.getValue();
						newAddition = false;
					}

				} else {

					outMap = outSortMap.get_next();
					if (outMap == null) {
						return outputMapVersion;
					}
					outMap.setFldOffset(outMap.getMapByteArray());

					outValue = outMap.getValue();

					while (outValue.compareTo(markValue) <= 0) {
						for (int i = 0; i < mark.size(); i++) {
							Map tempMap = mark.get(i);
							tempMap.setFldOffset(tempMap.getMapByteArray());
							String tempValue = tempMap.getValue();
							if (outValue.equals(tempValue)) {
								
								outputMapVersion = insertToTable(tempMap, outMap, mapinsert, outputMapVersion);
							}
						}
						outMap = outSortMap.get_next();
						if (outMap == null) {
							return outputMapVersion;
						}
						outMap.setFldOffset(outMap.getMapByteArray());

						outValue = outMap.getValue();

					}
					if (inMap == null) {
						return outputMapVersion;
					}
					markValue = "";
					mark = new ArrayList<>();
					newAddition = false;
				}

			}
		}

		outSortMap.close();
		inSortMap.close();

		return outputMapVersion;
	}

	public HashMap<String, List<Version>> insertToTable(Map inMap, Map outMap, MapInsert ms,
			HashMap<String, List<Version>> outputMapVersion)
			throws IOException, InvalidTupleSizeException, HFDiskMgrException, IndexException, HFException,
			FieldNumberOutOfBoundException, UnknownIndexTypeException, UnknownKeyTypeException,
			InvalidSlotNumberException, SpaceNotAvailableException, HFBufMgrException, InvalidTypeException,
			PageUnpinnedException, HashEntryNotFoundException, ReplacerException, InvalidFrameNumberException,
			KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException,
			ConstructPageException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException,
			DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException {

		Map newMap1 = new Map();

		inMap.setFldOffset(inMap.getMapByteArray());
		String newRow1 = inMap.getRowLabel();
		String newVal1 = inMap.getValue();
		String newCol1 = inMap.getColumnLabel();
		int newTime1 = inMap.getTimeStamp();

		newMap1.setDefaultHdr();
		newMap1.setColumnLabel(newCol1);
		newMap1.setRowLabel(newRow1);
		newMap1.setValue(newVal1);
		newMap1.setTimeStamp(newTime1);

		Map newMap2 = new Map();

		outMap.setFldOffset(outMap.getMapByteArray());
		String newRow2 = outMap.getRowLabel();
		String newVal2 = outMap.getValue();
		String newCol2 = outMap.getColumnLabel();
		int newTime2 = outMap.getTimeStamp();

		newMap2.setDefaultHdr();
		newMap2.setColumnLabel(newCol2);
		newMap2.setRowLabel(newRow2);
		newMap2.setValue(newVal2);
		newMap2.setTimeStamp(newTime2);

		newMap1.setFldOffset(newMap1.getMapByteArray());

		newMap2.setFldOffset(newMap2.getMapByteArray());

		String inRow = newMap1.getRowLabel();
		String inCol = newMap1.getColumnLabel();
		String inValue = newMap1.getValue();
		int inTimestamp = newMap1.getTimeStamp();

		String outRow = newMap2.getRowLabel();
		String outCol = newMap2.getColumnLabel();
		String outValue = newMap2.getValue();
		int outTimestamp = newMap2.getTimeStamp();

		if (inCol.equals(outCol)) {
			inCol = inCol + "_right";
			outCol = outCol + "_left";
		}

		String rowLabel = outRow + ":" + inRow;

		



		outputMapVersion = ms.run(rowLabel, outCol, outValue, outTimestamp, outputMapVersion);
		outputMapVersion = ms.run(rowLabel, inCol, inValue, inTimestamp, outputMapVersion);

		return outputMapVersion;
	}

	/*
	 * Empty get_next() method so that error is not thrown since get_next() is
	 * abstract method of class Iterator
	 */
//	public Map get_next()
//		throws IOException,
//		   JoinsException ,
//		   IndexException,
//		   PageNotReadException,
//		   TupleUtilsException, 
//		   PredEvalException,
//		   SortException,
//		   LowMemException,
//		   UnknowAttrType,
//		   UnknownKeyTypeException,
//		   Exception{
//
//		return null;
//	}
}