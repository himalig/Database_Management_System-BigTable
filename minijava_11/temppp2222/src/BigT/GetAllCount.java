package BigT;

import btree.*;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import global.MapOrder;
import heap.*;
import iterator.*;

import java.io.IOException;
import java.util.HashSet;

public class GetAllCount {

	int numBuf;

	public GetAllCount(int numBuf) {
		this.numBuf = (int) (numBuf * 3) / 4;
	}

	public void run(String bigtName) throws Exception {

		Stream tempStream;
		int rowCount;
		int colCount;
		bigt tempBigt;
		int mapCount = 0;

		System.out.println("Bigtable: " + bigtName);
		System.out.println("---------------------------------------");
		tempBigt = new bigt(bigtName);
		mapCount = tempBigt.getMapCnt();
		rowCount = tempBigt.getRowCnt();
		colCount = tempBigt.getColumnCnt();

		System.out.println("TOTAL NUMBER OF MAPS: " + mapCount);
		System.out.println("NUMBER OF DISTINCT ROW LABELS: " + rowCount);
		System.out.println("NUMBER OF DISTINCT COLUMN LABELS: " + colCount);
		System.out.println("---------------------------------------");
	}

}