package BigT;

import btree.*;
import bufmgr.*;
import global.*;
import heap.*;
import index.IndexException;
import index.MapIndexScan;
import index.UnknownIndexTypeException;
import iterator.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.*;
public class RowSort {

	private bigt resultTable;
	private int numBuf;
	private bigt sourceTable;
	private String sourceTableName;
	private String ColumnName;
	private MapOrder mapOrder;
	private String outableName;

	public RowSort(String sourceTableName, String resultTable, int rowOrder, String ColumnName, int n_pages)
			throws Exception {
		this.numBuf = n_pages;
		this.sourceTableName = sourceTableName;
		this.ColumnName = ColumnName;
		this.sourceTable = new bigt(sourceTableName, 1);
		this.resultTable = new bigt(resultTable, 1);
		this.outableName = resultTable;
		if (rowOrder == 1) {
			mapOrder = new MapOrder(MapOrder.Ascending);
		} else if (rowOrder == 2) {
			mapOrder = new MapOrder(MapOrder.Descending);
		}
	}

	public void run_new(HashMap<String, List<Version>> mapVersion) throws Exception {
		MapInsert mapinsert = new MapInsert(resultTable, 1, outableName);

		Stream stream1 = new Stream(sourceTableName, null, 1, 1, "*", ColumnName, "*", 20);
		Stream stream2 = new Stream(sourceTableName, null, 1, 9, "*", "*", "*", 20);
		bigt tempbt = new bigt("temp", 1);
		Map map1 = stream1.getNext();
		System.out.println("Inserting Maps containing the column");
		Map map2 = stream2.getNext();
		List<Version> ltemp = new ArrayList<>();
		MID mid = new MID();
		ltemp = mapVersion.get(map1.getRowLabel() + "$" + map1.getColumnLabel());
		for (int i = 0; i < ltemp.size(); i++) {
			mid = ltemp.get(i).mid;
		}

		System.out.println("\n\nDisplaying Maps not containing the column");

		while (true) {
			if (map2 == null) {
				break;
			}
			map2.setFldOffset(map2.getMapByteArray());
			if (!map2.getColumnLabel().equals(ColumnName)) {
				map2.print();
			}
			mapinsert.run(map2.getRowLabel(), map2.getColumnLabel(), map2.getValue(), map2.getTimeStamp(),mapVersion);
			map2 = stream2.getNext();
		}
		System.out.println("\n\nDisplaying Maps containing the column");
		while (true) {
			if (map1 == null) {
				break;
			}
			map1.setFldOffset(map1.getMapByteArray());
			mapinsert.run(map1.getRowLabel(), map1.getColumnLabel(), map1.getValue(), map1.getTimeStamp(),mapVersion);
//			resultTable.insertMap(map1.getMapByteArray());
			map1.print();
			map1 = stream1.getNext();

		}
		stream1.closestream();
		stream2.closestream();

	}

	public void run(HashMap<String, List<Version>> mapVersion) throws Exception {
		MapInsert mapinsert = new MapInsert(resultTable, 1, outableName);
		Stream stream1 = new Stream(sourceTableName, null, 1, 5, "*", ColumnName, "*", 20);
		Stream stream2 = new Stream(sourceTableName, null, 1, 1, "*", "*", "*", 20);

//    	FileScanMap stream1 = new FileScanMap(sourceTableName,null,null,false);
////		Stream stream1 = new Stream(sourceTableName, null, 1, 1, "*", "*", "*", numBuf);
//		SortMap outSortMap = new SortMap(null, null, null, stream1, 5, new MapOrder(MapOrder.Ascending), null, 200);
		bigt tempbt = new bigt("temp", 1);
		int count = 0;
		Map map1 = stream1.getNext();
		Long convertedVal = Long.valueOf(map1.getValue());
		System.out.println("Stream1 ---[" + map1.getRowLabel() + " " + map1.getColumnLabel() + " " + map1.getTimeStamp()
				+ " ] -> " + convertedVal);
		Map map2 = stream2.getNext();
		convertedVal = Long.valueOf(map2.getValue());
		System.out.println("Stream2-------[" + map2.getRowLabel() + " " + map2.getColumnLabel() + " "
				+ map2.getTimeStamp() + " ] -> " + convertedVal);

		Map map3 = null;
		String prevRow1 = "";
		String currRow1 = "";
		String prevRow2 = "";
		String currRow2 = "";

		System.out.println("Maps inserted in the following order:");
		// get entry from stream1 using getNext
		// use this entry to iterate over stream2
		// if entry exists, put all the maps with the corresponding name in the new
		// Bigtable
		// else put it in the temporary heap file.

		while (map1 != null && map2 != null) {
			// map1.print();
			// map2.print();
			// First stream is exhausted, put maps of 2nd stream to temporary BT
			if (map1 == null) {
				// System.out.println("Case 1");
				tempbt.insertMap(map2.getMapByteArray());
				while ((map2 = stream2.getNext()) != null) {
					tempbt.insertMap(map2.getMapByteArray());
				}

			}

			// The maps at head of both the stream belong to the same row
			// Thus, insert all the maps of that row to output BT
			// Reposition Stream 1 to next row
			else if (map1.getRowLabel().equals(map2.getRowLabel())
					&& map1.getColumnLabel().equals(map2.getColumnLabel())) {
				// System.out.println("Case 2");
				resultTable.insertMap(map2.getMapByteArray());
				map2.print();
				prevRow2 = map2.getRowLabel();

				// Insert all the maps having this specific rowlabel
				while (((map2 = stream2.getNext()) != null) && prevRow2.equals(map2.getRowLabel())) {
					resultTable.insertMap(map2.getMapByteArray());
					map2.print();
					prevRow2 = map2.getRowLabel();
				}

				// Jump in stream1 to the next row
				prevRow1 = map1.getRowLabel();
				while (((map1 = stream1.getNext()) != null) && prevRow1.equals(map1.getRowLabel())) {
					prevRow1 = map1.getRowLabel();
				}
			}

			// Encountered a row in stream 2 that does not contain the desired column
			// Output the entire row to temporary BT
			else if (!map1.getColumnLabel().equals(map2.getColumnLabel())) {
				tempbt.insertMap(map2.getMapByteArray());

			}

		}

		stream1.closestream();
		stream2.closestream();
		FileScanMap fs = new FileScanMap("temp", null, null, false);

//		FileScan fs = new FileScan("temp_1", "*", "*", "*");
		System.out.println("Inserting Maps Not containing the column");
		while ((map3 = fs.get_next()) != null) {
			resultTable.insertMap(map3.getMapByteArray());
			map3.print();
		}

		fs.close();
//		tempbt.deletebigT();

//		while (true) {
//			if (t == null) {
//				break;
//			}
//			count++;
//			t.setFldOffset(t.getMapByteArray());
//			t.print();
//			t = stream.getNext();
//		}
//		stream.closestream();	

//		Stream stream2 = new Stream(sourceTableName, null, 1, 1, "*", ColumnName, "*", numBuf);
//		Heapfile filterRows = new Heapfile("filterRows");
//		createHeapFilter(stream2, filterRows);
////		System.exit(0);
//		stream2.closestream();
//
//		dumpDefault(rows, filterRows);
//
//		// Sort iterator on filterRows heapfile to insert the filterRows in bigt -> sort
//		// order 7 (value based)
//		FileScanMap tempScan = new FileScanMap("filterRows", null, null, false);
////        SortMap sort = new SortMap(null, null, null, tempScan, 7, mapOrder,null, numBuf);
//
//		Map mapSort = tempScan.get_next();
//		String rowLabel;
//
//		while (mapSort != null) {
//			mapSort.setDefaultHdr();
//			rowLabel = mapSort.getRowLabel();
//			FileScanMap rowLabelScan = new FileScanMap(sourceTableName, null,
//					getConditionalExpression(rowLabel, ColumnName), true);
//			SortMap sort1 = new SortMap(null, null, null, rowLabelScan, 3, mapOrder, null, (int) numBuf / 20);
//			Map tempMap = sort1.get_next();
//			while (tempMap != null) {
//				tempMap.setDefaultHdr();
//				resultTable.insertMap(tempMap.getMapByteArray());
//				Long convertedVal = Long.valueOf(tempMap.getValue());
//				System.out.println("--------[" + tempMap.getRowLabel() + " " + tempMap.getColumnLabel() + " "
//						+ tempMap.getTimeStamp() + " ] -> " + convertedVal);
//
//				tempMap = sort1.get_next();
//			}
//			sort1.close();
//			rowLabelScan.close();
//			mapSort = tempScan.get_next();
//		}
//		tempScan.close();
//		System.out.println("NUMBER OF MAPS IN OUTPUT BIGTABLE: " + resultTable.getMapCnt());
//
//		rows.deleteFileMap();
//		filterRows.deleteFileMap();
	}

	public void createHeapAll(Stream stream, Heapfile heapfile) throws IOException, InvalidTupleSizeException,
			SpaceNotAvailableException, HFException, HFBufMgrException, InvalidSlotNumberException, HFDiskMgrException {
		Map tempMap = null;
		tempMap = stream.getNext();
		Map prevMap = null;
		if (tempMap != null) {
			tempMap.setDefaultHdr();
			prevMap = new Map(tempMap);
		}
		while (tempMap != null) {
			tempMap.setDefaultHdr();
			if (!tempMap.getColumnLabel().equals(prevMap.getColumnLabel())
					&& tempMap.getRowLabel().equals(prevMap.getRowLabel())) {
				heapfile.insertRecordMap(prevMap.getMapByteArray());
				Long convertedVal = Long.valueOf(prevMap.getValue());
				System.out.println("-------[" + prevMap.getRowLabel() + " " + prevMap.getColumnLabel() + " "
						+ prevMap.getTimeStamp() + " ] -> " + convertedVal);

			}
			prevMap = new Map(tempMap);
			tempMap = stream.getNext();
		}
		heapfile.insertRecordMap(prevMap.getMapByteArray());
		Long convertedVal = Long.valueOf(prevMap.getValue());
		System.out.println("-------[" + prevMap.getRowLabel() + " " + prevMap.getColumnLabel() + " "
				+ prevMap.getTimeStamp() + " ] -> " + convertedVal);

		System.out.println("-------------Get Map Count: " + heapfile.getRecCntMap());
	}

	public void createHeapFilter(Stream stream, Heapfile heapfile) throws IOException, InvalidTupleSizeException,
			SpaceNotAvailableException, HFException, HFBufMgrException, InvalidSlotNumberException, HFDiskMgrException {
		Map tempMap = null;
		tempMap = stream.getNext();
		Map prevMap = null;
		if (tempMap != null) {
			tempMap.setDefaultHdr();
			prevMap = new Map(tempMap);
		}
		while (tempMap != null) {
			tempMap.setDefaultHdr();
			if (!tempMap.getRowLabel().equals(prevMap.getRowLabel())) {
				heapfile.insertRecordMap(prevMap.getMapByteArray());
				Long convertedVal = Long.valueOf(prevMap.getValue());
				System.out.println("fffff[" + prevMap.getRowLabel() + " " + prevMap.getColumnLabel() + " "
						+ prevMap.getTimeStamp() + " ] -> " + convertedVal);

			}
			prevMap = new Map(tempMap);
			tempMap = stream.getNext();
		}
		heapfile.insertRecordMap(prevMap.getMapByteArray());
		Long convertedVal = Long.valueOf(prevMap.getValue());
		System.out.println("fffff[" + prevMap.getRowLabel() + " " + prevMap.getColumnLabel() + " "
				+ prevMap.getTimeStamp() + " ] -> " + convertedVal);

		System.out.println("------Get Map Count: " + heapfile.getRecCntMap());
	}

	public void dumpDefault(Heapfile allRows, Heapfile filterRows) throws LowMemException, Exception {
		Scan sc1 = allRows.openScanMap();
		Scan sc2 = filterRows.openScanMap();

		MID mid_left = new MID();
		MID mid_right = new MID();
		CondExpr[] expr;

		String rowLabel1, rowLabel2;
		Map map_left = sc1.getNextMap(mid_left);
		Map map_right = sc2.getNextMap(mid_right);
		while (map_left != null) {
			map_left.setDefaultHdr();
			rowLabel1 = map_left.getRowLabel();
			if (map_right != null) {
				map_right.setDefaultHdr();
				rowLabel2 = map_right.getRowLabel();
			} else {
				rowLabel2 = "";
			}
			if (!rowLabel1.equals(rowLabel2)) {
				expr = getConditionalExpression(rowLabel1, map_left.getColumnLabel());
				FileScanMap tempFileScan = new FileScanMap(sourceTableName, null, expr, true);
				SortMap sort2 = new SortMap(null, null, null, tempFileScan, 1, mapOrder, null, (int) numBuf / 20);
				Map tempMap = sort2.get_next();
				while (tempMap != null) {
					tempMap.setDefaultHdr();
					resultTable.insertMap(tempMap.getMapByteArray());
					Long convertedVal = Long.valueOf(tempMap.getValue());
					System.out.println("+++++++[" + tempMap.getRowLabel() + " " + tempMap.getColumnLabel() + " "
							+ tempMap.getTimeStamp() + " ] -> " + convertedVal);

					tempMap = sort2.get_next();
				}
				sort2.close();
				tempFileScan.close();
			} else {
				if (map_left.getColumnLabel().equals(map_right.getColumnLabel())) {
					map_right = sc2.getNextMap(mid_right);
				} else {
					expr = getConditionalExpression(rowLabel1, map_left.getColumnLabel());
					FileScanMap tempFileScan = new FileScanMap(sourceTableName, null, expr, true);
					SortMap sort2 = new SortMap(null, null, null, tempFileScan, 1, mapOrder, null, (int) numBuf / 20);
					Map tempMap = sort2.get_next();
					while (tempMap != null) {
						tempMap.setDefaultHdr();
						resultTable.insertMap(tempMap.getMapByteArray());
						Long convertedVal = Long.valueOf(tempMap.getValue());
						System.out.println("*******[" + tempMap.getRowLabel() + " " + tempMap.getColumnLabel() + " "
								+ tempMap.getTimeStamp() + " ] -> " + convertedVal);

						tempMap = sort2.get_next();
					}
					sort2.close();
					tempFileScan.close();
				}

			}

			map_left = sc1.getNextMap(mid_left);
		}
		sc1.closescan();
		sc2.closescan();
	}

	public CondExpr[] getConditionalExpression(String rowLabel) {
		CondExpr[] res = new CondExpr[2];
		CondExpr expr = new CondExpr();
		expr.op = new AttrOperator(AttrOperator.aopEQ);
		expr.type1 = new AttrType(AttrType.attrSymbol);
		expr.type2 = new AttrType(AttrType.attrString);
		expr.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
		expr.operand2.string = rowLabel;
		expr.next = null;
		res[0] = expr;
		res[1] = null;

		return res;
	}

	public CondExpr[] getConditionalExpression(String rowLabel, String colLabel) {
		CondExpr[] res = new CondExpr[3];
		CondExpr expr = new CondExpr();
		expr.op = new AttrOperator(AttrOperator.aopEQ);
		expr.type1 = new AttrType(AttrType.attrSymbol);
		expr.type2 = new AttrType(AttrType.attrString);
		expr.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
		expr.operand2.string = rowLabel;
		expr.next = null;
		res[0] = expr;

		CondExpr expr1 = new CondExpr();
		expr1.op = new AttrOperator(AttrOperator.aopEQ);
		expr1.type1 = new AttrType(AttrType.attrSymbol);
		expr1.type2 = new AttrType(AttrType.attrString);
		expr1.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
		expr1.operand2.string = colLabel;
		expr1.next = null;
		res[1] = expr1;

		res[2] = null;

		return res;
	}
}

/*
 * package BigT;
 * 
 * import btree.*; import bufmgr.*; import global.*; import heap.*; import
 * index.IndexException; import index.MapIndexScan; import
 * index.UnknownIndexTypeException; import iterator.*;
 * 
 * import java.io.IOException; import java.util.ArrayList; import
 * java.util.Comparator; import java.util.List;
 * 
 * public class RowSort{
 * 
 * 
 * private bigt resultTable; private int numBuf; private bigt sourceTable;
 * private String sourceTableName; private String ColumnName; private MapOrder
 * mapOrder;
 * 
 * public RowSort(String sourceTableName, String resultTable, int rowOrder,
 * String ColumnName, int n_pages) throws Exception{ this.numBuf = n_pages;
 * this.sourceTableName = sourceTableName; this.ColumnName = ColumnName;
 * this.sourceTable = new bigt(sourceTableName, false); this.resultTable = new
 * bigt(resultTable, false);
 * 
 * if(rowOrder == 1){ mapOrder = new MapOrder(MapOrder.Ascending); }else
 * if(rowOrder == 2){ mapOrder = new MapOrder(MapOrder.Descending); } }
 * 
 * public void run() throws Exception { //create heap file with all rows recent
 * timestamps sorted based on row + timestamp Stream stream1 = new
 * Stream(sourceTableName, null, 1, 3, "*", "*", "*", numBuf); Heapfile rows =
 * new Heapfile("rows"); createHeap(stream1, rows); stream1.closestream();
 * 
 * Stream stream2 = new Stream(sourceTableName, null, 1, 3, "*", ColumnName,
 * "*", numBuf); Heapfile filterRows = new Heapfile("filterRows");
 * createHeap(stream2, filterRows); stream2.closestream();
 * 
 * dumpDefault(rows, filterRows);
 * 
 * //Sort iterator on filterRows heapfile to insert the filterRows in bigt ->
 * sort order 7 (value based) FileScanMap tempScan = new
 * FileScanMap("filterRows", null, null, false); SortMap sort = new
 * SortMap(null, null, null, tempScan, 7, mapOrder, null, numBuf);
 * 
 * Map mapSort = sort.get_next(); String rowLabel;
 * 
 * while(mapSort!=null){ mapSort.setDefaultHdr(); rowLabel =
 * mapSort.getRowLabel(); FileScanMap rowLabelScan = new
 * FileScanMap(sourceTableName, null, getConditionalExpression(rowLabel), true);
 * Map tempMap = rowLabelScan.get_next(); while(tempMap!=null){
 * tempMap.setDefaultHdr(); resultTable.insertMap(tempMap, 1); tempMap =
 * rowLabelScan.get_next(); } rowLabelScan.close(); mapSort = sort.get_next(); }
 * sort.close(); System.out.println("NUMBER OF MAPS IN OUTPUT BIGTABLE: " +
 * resultTable.getMapCnt());
 * 
 * rows.deleteFileMap(); filterRows.deleteFileMap(); }
 * 
 * public void createHeap(Stream stream, Heapfile heapfile) throws IOException,
 * InvalidTupleSizeException, SpaceNotAvailableException, HFException,
 * HFBufMgrException, InvalidSlotNumberException, HFDiskMgrException { Map
 * tempMap = null; tempMap = stream.getNext(); Map prevMap = null;
 * if(tempMap!=null){ tempMap.setDefaultHdr(); prevMap = new Map(tempMap); }
 * while(tempMap!=null){ tempMap.setDefaultHdr();
 * if(!tempMap.getRowLabel().equals(prevMap.getRowLabel())){
 * heapfile.insertRecordMap(prevMap.getMapByteArray()); } prevMap = new
 * Map(tempMap); tempMap = stream.getNext(); }
 * heapfile.insertRecordMap(prevMap.getMapByteArray()); }
 * 
 * public void dumpDefault(Heapfile allRows, Heapfile filterRows) throws
 * InvalidTupleSizeException, IOException, FileScanException,
 * TupleUtilsException, InvalidRelation, HFBufMgrException, HFException,
 * FieldNumberOutOfBoundException, InvalidSlotNumberException,
 * SpaceNotAvailableException, HFDiskMgrException, JoinsException,
 * PageNotReadException, WrongPermat, InvalidTypeException, PredEvalException,
 * UnknowAttrType { Scan sc1 = allRows.openScanMap(); Scan sc2 =
 * filterRows.openScanMap();
 * 
 * MID mid_left = new MID(); MID mid_right = new MID(); CondExpr[] expr;
 * 
 * String rowLabel1, rowLabel2; Map map_left = sc1.getNextMap(mid_left); Map
 * map_right = sc2.getNextMap(mid_right); while(map_left!=null){
 * map_left.setDefaultHdr(); rowLabel1 = map_left.getRowLabel();
 * if(map_right!=null){ map_right.setDefaultHdr(); rowLabel2 =
 * map_right.getRowLabel(); }else{ rowLabel2 = ""; }
 * if(!rowLabel1.equals(rowLabel2)){ expr = getConditionalExpression(rowLabel1);
 * FileScanMap tempFileScan = new FileScanMap(sourceTableName, null, expr,
 * true); Map tempMap = tempFileScan.get_next(); while(tempMap!=null){
 * tempMap.setDefaultHdr(); resultTable.insertMap(tempMap, 1); tempMap =
 * tempFileScan.get_next(); } tempFileScan.close(); }else{ map_right =
 * sc2.getNextMap(mid_right); }
 * 
 * map_left = sc1.getNextMap(mid_left); } sc1.closescan(); sc2.closescan(); }
 * 
 * 
 * 
 * public CondExpr[] getConditionalExpression(String rowLabel){ CondExpr[] res =
 * new CondExpr[2]; CondExpr expr = new CondExpr(); expr.op = new
 * AttrOperator(AttrOperator.aopEQ); expr.type1 = new
 * AttrType(AttrType.attrSymbol); expr.type2 = new
 * AttrType(AttrType.attrString); expr.operand1.symbol = new FldSpec(new
 * RelSpec(RelSpec.outer), 1); expr.operand2.string = rowLabel; expr.next =
 * null; res[0] = expr; res[1] = null;
 * 
 * return res; } }
 * 
 */
