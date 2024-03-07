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
public class BatchInsert {
	String datafile;
	String bigTable;
	int storageType;
	bigt table;
	Version r;


	public BatchInsert(bigt table, String datafile, int type, String bigTable) {
		this.table = table;
		this.datafile = datafile;
		this.storageType = type;
		this.bigTable = bigTable;
	}

	public HashMap<String, List<Version>> run(HashMap<String, List<Version>> mapVersion)
			throws InvalidTupleSizeException, HFDiskMgrException, IndexException, HFException, IOException,
			FieldNumberOutOfBoundException, UnknownIndexTypeException, UnknownKeyTypeException,
			InvalidSlotNumberException, SpaceNotAvailableException, HFBufMgrException, InvalidTypeException,
			PageUnpinnedException, HashEntryNotFoundException, ReplacerException, InvalidFrameNumberException,
			KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException,
			ConstructPageException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException,
			DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException {
		File inputFile = new File(this.datafile);
		BufferedReader br = new BufferedReader(new FileReader(inputFile));
		Map map = new Map();
		List<Version> tempmid=null;
		String line = "";
		String[] labels;
		int i = 1;
		int pages = 0;
		long startTime = System.nanoTime();

		while ((line = br.readLine()) != null) {
			line = line.replaceAll("[^\\x00-\\x7F]", "");
			i++;
			if (i % 1000 == 0) {

				System.out.print("*");

			}
			labels = line.split(",");
			map.setDefaultHdr();
			map.setRowLabel(labels[0]);
			map.setColumnLabel(labels[1]);
			map.setTimeStamp(Integer.parseInt(labels[2]));

			String mapKey = labels[0] + "$" + labels[1];
			String valueLabel = labels[3];
			for (int j = labels[3].length(); j < Map.DEFAULT_STRING_ATTRIBUTE_SIZE; j++) {
				valueLabel = "0" + valueLabel;
			}
			map.setValue(valueLabel);

			MID mid = table.insertMap(map.getMapByteArray());
			table.insertIndex(mid, map, storageType);
			if (mapVersion.containsKey(mapKey)) {
				tempmid = mapVersion.get(mapKey);

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
					if (Integer.parseInt(labels[2]) > minTimestamp) {
						table.deleteRecordMap(minMid, minStorageType);
						tempmid.remove(minIndex);
						tempmid.add(new Version(storageType, mid, Integer.parseInt(labels[2])));
						mapVersion.put(mapKey, tempmid);
					} else {
						table.deleteRecordMap(mid, storageType);
					}
				} else if (tempmid.size() == 2) {

					int timestamp1 = tempmid.get(0).timestamp;
					int timestamp2 = tempmid.get(1).timestamp;

					if (Integer.parseInt(labels[2]) == timestamp1 || Integer.parseInt(labels[2]) == timestamp2) {

						table.deleteRecordMap(mid, storageType);

					} else {
						tempmid.add(new Version(storageType, mid, Integer.parseInt(labels[2])));
						mapVersion.put(mapKey, tempmid);
					}
				} else if (tempmid.size() == 1) {

					int timestamp1 = tempmid.get(0).timestamp;

					if (Integer.parseInt(labels[2]) == timestamp1) {

						table.deleteRecordMap(mid, storageType);

					} else {
						tempmid.add(new Version(storageType, mid, Integer.parseInt(labels[2])));
						mapVersion.put(mapKey, tempmid);
					}
				}

			} else {
				List<Version> ltemp = new ArrayList<>();
				ltemp.add(new Version(storageType, mid, Integer.parseInt(labels[2])));
				mapVersion.put(mapKey, ltemp);
			}

			pages = mid.pageNo.pid;
		}
		long endTime = System.nanoTime();
		System.out.println();

		System.out.println("Time taken for inserting all records: " + ((endTime - startTime) / 1000000000) + " s");

		System.out.println("Total number of maps: " + table.getMapCnt());

		System.out.println();
		return mapVersion;
	}
}
