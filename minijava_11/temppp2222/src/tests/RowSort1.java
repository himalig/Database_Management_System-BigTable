package tests;

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
import java.util.List;

public class RowSort1 {

	private bigt resultTable;
	private int numBuf;
	private bigt sourceTable;
	private String sourceTableName;
	private String ColumnName;
	private MapOrder mapOrder;

	public RowSort1(String sourceTableName, String resultTable, int rowOrder, String ColumnName, int n_pages)
			throws Exception {
		this.numBuf = n_pages;
		this.sourceTableName = sourceTableName;
		this.ColumnName = ColumnName;
		this.sourceTable = new bigt(sourceTableName, 1);
		this.resultTable = new bigt(resultTable, 1);

		if (rowOrder == 1) {
			mapOrder = new MapOrder(MapOrder.Ascending);
		} else if (rowOrder == 2) {
			mapOrder = new MapOrder(MapOrder.Descending);
		}
	}

	public void run() throws Exception {
		// create heap file with all rows recent timestamps sorted based on row +
		// timestamp
		System.out.println("hiiiii ");

		Stream stream1 = new Stream(sourceTableName, null, 1, 1, "*", "*", "*", numBuf);
//        mapIterator = new FileScanMap(bigtName, null, condExprs, true);

//		FileScanMap tempScan = new FileScanMap(sourceTableName, null, null, false);
		
		Map map1 = stream1.getNext();
		Map recentmap=new Map();
		boolean firstRecord = true;
		int resetFlag=0;
		
		String resetRow = "";

		Heapfile filterRows = new Heapfile("filterRows");
		
		while (map1 != null) {
			map1.setFldOffset(map1.getMapByteArray());

			if(resetFlag==1) {
				if(!map1.getRowLabel().equals(resetRow)) {
					filterRows.insertRecordMap(recentmap.getMapByteArray());
					resetFlag=0;
				}
				else {
					if(!map1.getColumnLabel().equals(ColumnName)) {
						filterRows.insertRecordMap(recentmap.getMapByteArray());
						resetFlag=0;
					}
				}
			}
			
			if(map1.getColumnLabel().equals(ColumnName)) {
				if(firstRecord) {
					recentmap = new Map(map1);
					resetFlag=1;
					resetRow = map1.getRowLabel();
					firstRecord=false;
				}
				//Do nothing
				else {
					if(map1.getTimeStamp() > recentmap.getTimeStamp() )
					{
						recentmap = new Map(map1);
						resetFlag=1;
						resetRow = map1.getRowLabel();
				
					}
				}
				

			}
			else{
				resultTable.insertMap(map1.getMapByteArray());
			}
			map1 = stream1.getNext();
		}
		
		
		FileScanMap newScan = new FileScanMap("filterRows", null, null, false);

		Map mapSort = newScan.get_next();
		String rowLabel;

		while (mapSort != null) {
			mapSort.setDefaultHdr();
			rowLabel = mapSort.getRowLabel();
			FileScanMap rowLabelScan = new FileScanMap(sourceTableName, null,
					getConditionalExpression(rowLabel, ColumnName), true);
			SortMap sort1 = new SortMap(null, null, null, rowLabelScan, 3, mapOrder, null, (int) numBuf / 20);
			Map tempMap = sort1.get_next();
			while (tempMap != null) {
				tempMap.setDefaultHdr();
				resultTable.insertMap(tempMap.getMapByteArray());
				Long convertedVal = Long.valueOf(tempMap.getValue());
				System.out.println("--------[" + tempMap.getRowLabel() + " " + tempMap.getColumnLabel() + " "
						+ tempMap.getTimeStamp() + " ] -> " + convertedVal);

				tempMap = sort1.get_next();
			}
			sort1.close();
			rowLabelScan.close();
			mapSort = newScan.get_next();
		}
		newScan.close();
		
		
		System.exit(0);
		
		Heapfile rows = new Heapfile("rows");
		createHeapAll(stream1, rows);
		stream1.closestream();
		
		

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
        System.out.println("fffff[" + prevMap.getRowLabel() + " " + prevMap.getColumnLabel() + " " + prevMap.getTimeStamp() + " ] -> " + convertedVal);

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
				expr = getConditionalExpression(rowLabel1,map_left.getColumnLabel());
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
