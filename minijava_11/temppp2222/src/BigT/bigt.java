package BigT;

import java.io.*;
import java.util.ArrayList;
import bufmgr.*;
import global.*;
import btree.*;
import heap.*;
import iterator.*;
import index.*;

public class bigt {

	private String name;

	public ArrayList<Heapfile> heapFiles;
	public ArrayList<String> heapFileNames;
	public ArrayList<String> indexFileNames;
	public ArrayList<BTreeFile> indexFiles;

	private AttrType[] attrType;
	private FldSpec[] projlist;
	private CondExpr[] expr;
	MapIndexScan mapscan;

	private int insertType;
	// private int insertType;
	short[] res_str_sizes = new short[] { Map.DEFAULT_ROW_LABEL_ATTRIBUTE_SIZE, Map.DEFAULT_STRING_ATTRIBUTE_SIZE,
			Map.DEFAULT_STRING_ATTRIBUTE_SIZE };

	public bigt(String name) throws HFException, HFBufMgrException, HFDiskMgrException, IOException,
			GetFileEntryException, ConstructPageException, AddFileEntryException, btree.IteratorException,
			btree.UnpinPageException, btree.FreePageException, btree.DeleteFileEntryException, btree.PinPageException,
			PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
		heapFiles = new ArrayList<>(6);
		indexFiles = new ArrayList<>(6);
		heapFileNames = new ArrayList<>(6);
		indexFileNames = new ArrayList<>(6);
		this.name = name;
		heapFiles.add(null);
		heapFileNames.add("");
		indexFileNames.add("");
		for (int i = 1; i <= 5; i++) {
			heapFileNames.add(name + "_" + i);
			indexFileNames.add(name + "_index_" + i);
			heapFiles.add(new Heapfile(heapFileNames.get(i)));
		}
		initializeCondExprs();
	}

	public bigt(String name, int type) throws HFException, HFBufMgrException, HFDiskMgrException, IOException,
			GetFileEntryException, ConstructPageException, AddFileEntryException, btree.IteratorException,
			btree.UnpinPageException, btree.FreePageException, btree.DeleteFileEntryException, btree.PinPageException,
			PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
		heapFiles = new ArrayList<>(6);
		indexFiles = new ArrayList<>(6);
		heapFileNames = new ArrayList<>(6);
		indexFileNames = new ArrayList<>(6);
		this.name = name;
		this.insertType = type;
		heapFiles.add(null);
		heapFileNames.add("");
		indexFileNames.add("");
		for (int i = 1; i <= 5; i++) {
			heapFileNames.add(name + "_" + i);
			indexFileNames.add(name + "_index_" + i);
			heapFiles.add(new Heapfile(heapFileNames.get(i)));
		}
		createIndex();
		try {
			for (int i = 2; i <= 5; i++) {
				deleteAllInIndex(indexFiles.get(i));
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Exception occurred while destroying all nodes of index files");
		}

		initializeCondExprs();
	}

	public void createIndex()
			throws IOException, ConstructPageException, PinPageException, UnpinPageException, IteratorException,
			GetFileEntryException, DeleteFileEntryException, AddFileEntryException, FreePageException,
			PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
		indexFiles.add(null);
		BTreeFile _index = null;
		for (int i = 1; i <= 5; i++) {
			_index = createIndex(indexFileNames.get(i), i);
			indexFiles.add(_index);

		}
	}

	public BTreeFile createIndex(String indexName1, int type) throws GetFileEntryException, ConstructPageException,
			IOException, AddFileEntryException, btree.IteratorException, btree.UnpinPageException,
			btree.FreePageException, btree.DeleteFileEntryException, btree.PinPageException {
		BTreeFile index = null;
		switch (type) {
		case 1:
			break;
		case 2:
			index = new BTreeFile(indexName1, AttrType.attrString, Map.DEFAULT_ROW_LABEL_ATTRIBUTE_SIZE,
					DeleteFashion.NAIVE_DELETE);
			break;
		case 3:
			index = new BTreeFile(indexName1, AttrType.attrString, Map.DEFAULT_STRING_ATTRIBUTE_SIZE,
					DeleteFashion.NAIVE_DELETE);
			break;
		case 4:
			index = new BTreeFile(indexName1, AttrType.attrString,
					Map.DEFAULT_ROW_LABEL_ATTRIBUTE_SIZE + Map.DEFAULT_STRING_ATTRIBUTE_SIZE + 5,
					DeleteFashion.NAIVE_DELETE);
			break;
		case 5:
			index = new BTreeFile(indexName1, AttrType.attrString,
					Map.DEFAULT_ROW_LABEL_ATTRIBUTE_SIZE + Map.DEFAULT_STRING_ATTRIBUTE_SIZE + 5,
					DeleteFashion.NAIVE_DELETE);
			break;
		}
		return index;
	}

	public MID insertMap(byte[] mapPtr) {
		MID mid = null;
		try {
			mid = heapFiles.get(insertType).insertRecordMap(mapPtr);
		} catch (InvalidSlotNumberException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidTupleSizeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SpaceNotAvailableException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (HFException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (HFBufMgrException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (HFDiskMgrException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return mid;
	}

	// This function inserts the record.
	public void insertIndex(MID mid, Map map, int type) throws KeyTooLongException, KeyNotMatchException,
			LeafInsertRecException, IndexInsertRecException, ConstructPageException, UnpinPageException,
			PinPageException, NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException,
			IteratorException, LeafDeleteException, InsertException, IOException {
		switch (type) {
		case 1:
			break;
		case 2:
			indexFiles.get(2).insert(new StringKey(map.getRowLabel()), mid);
			break;
		case 3:
			indexFiles.get(3).insert(new StringKey(map.getColumnLabel()), mid);
			break;
		case 4:
			indexFiles.get(4).insert(new StringKey(map.getColumnLabel() + "$" + map.getRowLabel()), mid);
			break;
		case 5:
			indexFiles.get(5).insert(new StringKey(map.getRowLabel() + "$" + map.getValue()), mid);
			break;
		}
	}

	public Stream openStream(String bigTableName, int type, int orderType, String rowFilter, String columnFilter,
			String valueFilter, int numBuf) {
		Stream stream = new Stream(bigTableName, null, type, orderType, rowFilter, columnFilter, valueFilter, numBuf);
		return stream;
	}

	public void deleteRecordMap(MID mid, int type) throws HFDiskMgrException, InvalidTupleSizeException, HFException,
			IOException, InvalidSlotNumberException, HFBufMgrException {
		getHeapFile(type).deleteRecordMap(mid);
	}
	
	public Map getRecordMap(MID mid, int type) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, Exception {
		Map amap = getHeapFile(type).getRecordMap(mid);
		return amap;
	}

	public int getMapCnt() {
		int totalMapCount = 0;
		for (int i = 1; i <= 5; i++) {
			try {
				totalMapCount += heapFiles.get(i).getRecCntMap();
			} catch (InvalidSlotNumberException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidTupleSizeException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (HFDiskMgrException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (HFBufMgrException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return totalMapCount;
	}

	public int getRowCnt() throws Exception {
		return getCount(3);
	}

	public int getColumnCnt() throws Exception {
		return getCount(4);
	}

	public int getCount(int orderType) throws Exception {
		int numBuf = (int) ((SystemDefs.JavabaseBM.getNumBuffers() * 3) / 4);
		Stream stream = new Stream(this.name, null, 1, orderType, "*", "*", "*", numBuf);
		Map t = stream.getNext();
		int count = 0;
		String temp = "\0";
		while (t != null) {
			t.setFldOffset(t.getMapByteArray());
			if (orderType == 3) {
				if (!t.getRowLabel().equals(temp)) {
					temp = t.getRowLabel();
					count++;
				}
			} else {
				if (!t.getColumnLabel().equals(temp)) {
					temp = t.getColumnLabel();
					count++;
				}
			}
			t = stream.getNext();
		}
		stream.closestream();
		return count;
	}

	public void initializeCondExprs() {
		attrType = new AttrType[4];
		attrType[0] = new AttrType(AttrType.attrString);
		attrType[1] = new AttrType(AttrType.attrString);
		attrType[2] = new AttrType(AttrType.attrInteger);
		attrType[3] = new AttrType(AttrType.attrString);
		projlist = new FldSpec[4];
		RelSpec rel = new RelSpec(RelSpec.outer);
		projlist[0] = new FldSpec(rel, 0);
		projlist[1] = new FldSpec(rel, 1);
		projlist[2] = new FldSpec(rel, 2);
		projlist[3] = new FldSpec(rel, 3);

		expr = new CondExpr[3];
		expr[0] = new CondExpr();
		expr[0].op = new AttrOperator(AttrOperator.aopEQ);
		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		expr[0].type2 = new AttrType(AttrType.attrString);
		expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
		expr[0].operand2.string = "";
		expr[0].next = null;
		expr[1] = new CondExpr();
		expr[1].op = new AttrOperator(AttrOperator.aopEQ);
		expr[1].type1 = new AttrType(AttrType.attrSymbol);
		expr[1].type2 = new AttrType(AttrType.attrString);
		expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
		expr[1].operand2.string = "";
		expr[1].next = null;
		expr[2] = null;
	}

	public String getName() {
		return name;
	}

	public Heapfile getHeapFile(int i) {
		return heapFiles.get(i);
	}

	public int getinsertType() {
		return this.insertType;
	}

	public String getIndexFileName(int i) {
		return indexFileNames.get(i);
	}

	public String getHeapFileName(int i) {
		return heapFileNames.get(i);
	}

	public String indexName() {
		return name + "_index_" + insertType;
	}

	public void deleteAllInIndex(BTreeFile index) throws PinPageException, KeyNotMatchException, IteratorException,
			IOException, ConstructPageException, UnpinPageException, ScanIteratorException, ScanDeleteException {
		BTFileScan scan = index.new_scan(null, null);
		boolean isScanComplete = false;
		while (!isScanComplete) {
			KeyDataEntry entry = scan.get_next();
			if (entry == null) {
				isScanComplete = true;
				break;
			}
			scan.delete_current();
		}
	}
}
