package BigT;

import java.io.*;
import java.util.*;

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
import global.MID;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.SpaceNotAvailableException;

public class CreateIndex {
	String btName;
	int old_indextype, new_indextype;
	bigt bt;

	public CreateIndex(String btName, int old_indextype, int new_indextype, bigt bt) {
		super();
		this.btName = btName;
		this.old_indextype = old_indextype;
		this.new_indextype = new_indextype;
		this.bt = bt;
	}

	public void createIndex(HashMap<String, HashMap> tableVersions) throws InvalidSlotNumberException, InvalidTupleSizeException, SpaceNotAvailableException, HFException, HFBufMgrException, HFDiskMgrException, IOException, KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException {
		Stream stream = bt.openStream(btName,old_indextype,1, "*", "*", "*",200);
		Map map = stream.getNext();
		int count = 0;
		Map t = stream.getNext();
		List<Version> version=null;
		while (true) {
			if (t == null) {
				break;
			}
			count++;
			t.setFldOffset(t.getMapByteArray());
			MID mid = null;
			
			HashMap<String, List<Version>> tempmap= tableVersions.get(btName);
			version=tempmap.get(t.getRowLabel()+"$"+t.getColumnLabel());
			for(int i=0;i<version.size();i++)
			{
				mid=version.get(i).mid;
				bt.insertIndex(version.get(i).mid, t, new_indextype);
			}

//			mid = bt.heapFiles.get(new_indextype).insertRecordMap(t.getMapByteArray());
			t.print();
			t = stream.getNext();
		}
		stream.closestream();
	}

}
