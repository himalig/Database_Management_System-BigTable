package BigT;

import java.io.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import btree.ConstructPageException;
import btree.ConvertException;
import btree.DeleteRecException;
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
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import global.*;
import heap.*;
import index.IndexException;
import index.UnknownIndexTypeException;
import iterator.UnknownKeyTypeException;

/**
 * BatchInsert
 */
public class MapInsert {
	String bigTable;
	int storageType;
	bigt table;
	Version r;

	public MapInsert(bigt table, int type, String bigTable) {
		this.table = table;
		this.storageType = type;
		this.bigTable = bigTable;
	}

	public HashMap<String, List<Version>> run(String RowLabel, String ColumnLabel, String Value, int TimeStamp, HashMap<String, List<Version>> mapVersion)
			throws InvalidTupleSizeException, HFDiskMgrException, IndexException, HFException, IOException,
			FieldNumberOutOfBoundException, UnknownIndexTypeException, UnknownKeyTypeException,
			InvalidSlotNumberException, SpaceNotAvailableException, HFBufMgrException, InvalidTypeException,
			PageUnpinnedException, HashEntryNotFoundException, ReplacerException, InvalidFrameNumberException,
			KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException,
			ConstructPageException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException,
			DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException {
	
		Map map = new Map();
		long startTime = System.nanoTime();
		int pages = 0;

			map.setDefaultHdr();
			map.setRowLabel(RowLabel);
			map.setColumnLabel(ColumnLabel);
			map.setTimeStamp(TimeStamp);

			String mapKey = RowLabel + "$" + ColumnLabel;
			String valueLabel = Value;
			for (int j = Value.length(); j < Map.DEFAULT_STRING_ATTRIBUTE_SIZE; j++) {
				valueLabel = "0" + valueLabel;
			}
			map.setValue(valueLabel);

			MID mid = table.insertMap(map.getMapByteArray());
			table.insertIndex(mid, map, storageType);

			if (mapVersion.containsKey(mapKey)) {
				List<Version> tempmid = mapVersion.get(mapKey);

				if (tempmid.size() == 3) {

					int tempStorageType1 = tempmid.get(0).storageType;
					int tempStorageType2 = tempmid.get(1).storageType;
					int tempStorageType3 = tempmid.get(2).storageType;

					MID mid1 = tempmid.get(0).mid;
					MID mid2 = tempmid.get(1).mid;
					MID mid3 = tempmid.get(2).mid;

					int timestamp1 = tempmid.get(0).timestamp;
					int timestamp2 = tempmid.get(1).timestamp;
					int timestamp3 = tempmid.get(2).timestamp;

					int minStorageType = tempStorageType1;
					MID minMid = mid1;
					int minTimestamp = timestamp1;
					int minIndex = 0;

					if (timestamp2 < minTimestamp) {
						minStorageType = tempStorageType2;
						minMid = mid2;
						minTimestamp = timestamp2;
						minIndex = 1;
					}
					if (timestamp3 < minTimestamp) {
						minStorageType = tempStorageType3;
						minMid = mid3;
						minTimestamp = timestamp3;
						minIndex = 2;
					}
					if (TimeStamp > minTimestamp) {
						table.deleteRecordMap(minMid, minStorageType);
						tempmid.remove(minIndex);
						tempmid.add(new Version(storageType, mid, TimeStamp));
						mapVersion.put(mapKey, tempmid);
					} else {
						table.deleteRecordMap(mid, storageType);
					}
				} else if (tempmid.size() == 2) {

					int timestamp1 = tempmid.get(0).timestamp;
					int timestamp2 = tempmid.get(1).timestamp;

					if (TimeStamp == timestamp1 || TimeStamp == timestamp2) {

						table.deleteRecordMap(mid, storageType);

					} else {
						tempmid.add(new Version(storageType, mid, TimeStamp));
						mapVersion.put(mapKey, tempmid);
					}
				} else if (tempmid.size() == 1) {

					int timestamp1 = tempmid.get(0).timestamp;

					if (TimeStamp == timestamp1) {

						table.deleteRecordMap(mid, storageType);

					} else {
						tempmid.add(new Version(storageType, mid, TimeStamp));
						mapVersion.put(mapKey, tempmid);
					}
				}

			} else {
				List<Version> ltemp = new ArrayList<>();
				ltemp.add(new Version(storageType, mid, TimeStamp));
				mapVersion.put(mapKey, ltemp);
			}

			pages = mid.pageNo.pid;
		
		long endTime = System.nanoTime();
//		System.out.println();
//
//		System.out.println("Time taken for inserting map: " + ((endTime - startTime) / 1000000000) + " s");
//
		return mapVersion;
	}
}
