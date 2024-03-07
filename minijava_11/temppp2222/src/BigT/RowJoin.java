package BigT;

import btree.*;
import bufmgr.*;
import global.*;
import heap.*;

import iterator.*;

import java.io.IOException;

import java.util.HashMap;
import java.util.List;

public class RowJoin{
    private String outBigTable;
    private String inBigTable;
    private String outputBigTable;
    private Heapfile outHeapFile;
    private Heapfile inHeapFile;
    private Heapfile outUniqueValue;
    private Heapfile inUniqueValue;
    private String outHeapFileName;
    private String inHeapFileName;
    private String outUniqueHeapFileName;
    private String inUniqueHeapFileName;
    private Stream outStream;
    private Stream inStream;
    private bigt outBigT;
    private bigt inBigT;
    private int joinType;
    private int numBuf;
    private bigt outputBigT;


    public RowJoin(String bigTable1, String bigTable2, String outBigTable, int joinType, int numBuf) throws HFException, HFBufMgrException, HFDiskMgrException, GetFileEntryException, ConstructPageException, AddFileEntryException, IteratorException, UnpinPageException, FreePageException, DeleteFileEntryException, PinPageException, PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException, IOException{
        this.outBigTable = bigTable1;
        this.inBigTable = bigTable2;
        this.outputBigTable = outBigTable;
        this.outHeapFileName = "outerTable";
        this.inHeapFileName = "innerTable";
        this.outHeapFile = new Heapfile(outHeapFileName);
        this.inHeapFile = new Heapfile(inHeapFileName);
        this.inUniqueHeapFileName = "innerUnique";
        this.outUniqueHeapFileName = "outerUnqiue";
        this.outUniqueValue = new Heapfile(this.outUniqueHeapFileName);
        this.inUniqueValue = new Heapfile(this.inUniqueHeapFileName);
        this.outputBigT = new bigt(outBigTable, 1);
        this.joinType = joinType;
        this.numBuf = numBuf;
        try{
            this.outBigT = new bigt(this.outBigTable);
            this.inBigT = new bigt(this.inBigTable);
        }catch(Exception e){
            System.err.println("RowJoin.java: Error caused in retrieving the bigtables");
            e.printStackTrace();
        }
        try{
            System.out.println("Records in outer relation: " + this.outBigT.getMapCnt());
            System.out.println("Records in inner relation: " + this.inBigT.getMapCnt());
            this.numBuf = (int)((SystemDefs.JavabaseBM.getNumBuffers()*3)/4);
        }catch(Exception e){
            System.err.println("RowJoin.java: Error in getting the number of records in the bigtables");
            e.printStackTrace();
        }
    }

    public HashMap<String, List<Version>> run(HashMap<String, List<Version>>  outputMapVersion) throws Exception {
        buildTempHeapFiles();
        System.out.println("-------------Get Map Count: " + inHeapFile.getRecCntMap());
        System.out.println("-------------Get Map Count: " + outHeapFile.getRecCntMap());
        
        if(joinType==1) {
        		outputMapVersion = performJoinNestedLoop(outputMapVersion);
        }
        else if(joinType==2) {
//        		generateUniqueValueFiles();
//        		System.out.println("-------------Get Unique Map Count: " + inUniqueValue.getRecCntMap());
//            System.out.println("-------------Get Unique Map Count: " + outUniqueValue.getRecCntMap());
//        		outputMapVersion = performJoinSortMerge(outputMapVersion);
        	
        	outputMapVersion = 	performSMJoin(outputMapVersion);
        }
        

        outHeapFile.deleteFileMap();
        inHeapFile.deleteFileMap();
        
        return outputMapVersion;
    }

    
    public HashMap<String, List<Version>> performSMJoin(HashMap<String, List<Version>>  outputMapVersion) throws FileScanException, TupleUtilsException, InvalidRelation, IOException, JoinsException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, PredEvalException, UnknowAttrType, FieldNumberOutOfBoundException, WrongPermat {

        //Create streams on the new temp bigTs, ordered on values
    	FileScanMap outerStream = new FileScanMap(outHeapFileName,null,null,false);
        
     
//        Stream innerStream = new Stream(inHeapFileName, null,1,8,"*", "*", "*", (int)this.numBuf/5);
        
        FileScanMap innerStream = new FileScanMap(inHeapFileName,null,null,false);

        SortMergeMap sm = null;
        try{
            sm = new SortMergeMap(outHeapFileName, inHeapFileName, outerStream, innerStream, outputBigTable);
        }
        catch(Exception e){
            System.err.println("*** join error in SortMerge constructor ***"); 

            System.err.println (""+e);
            e.printStackTrace();
        }

        try{
        	outputMapVersion =sm.performJoin2(outputMapVersion);
             System.out.println("Performed sortmerge join");
        }
        catch(Exception e){
            System.err.println (""+e);
            e.printStackTrace();
            
        }
        
  
        try {
            sm.close();
        }
        catch (Exception e) {
         
            e.printStackTrace();
        }
        System.out.println ("\n"); 
        

       return outputMapVersion; 

	}
   
    
    public HashMap<String, List<Version>> performJoinSortMerge(HashMap<String, List<Version>>  outputMapVersion) throws Exception{
    	
    		FileScanMap outScanUnique = new FileScanMap(outUniqueHeapFileName, null, null, false);
        SortMap outSortMapUnique = new SortMap(null, null, null, outScanUnique, 7, new MapOrder(MapOrder.Ascending), null, (int) (this.numBuf )/5);
    		
        FileScanMap inScanUnique = new FileScanMap(inUniqueHeapFileName, null, null, false);
        SortMap inSortMapUnique = new SortMap(null, null, null, inScanUnique, 7, new MapOrder(MapOrder.Ascending), null, (int) (this.numBuf )/5);
        
        Map leftMap=new Map();
        leftMap = outSortMapUnique.get_next();
        
        Map rightMap = new Map();
        rightMap = inSortMapUnique.get_next();
        
        while(rightMap!=null && leftMap!=null) {
        
        		leftMap.setFldOffset(leftMap.getMapByteArray());
        		rightMap.setFldOffset(rightMap.getMapByteArray());
        		
        		String leftValue = leftMap.getValue();
        		String rightValue = rightMap.getValue();
        		
        		if(leftValue.equals(rightValue)) {
        			
        			FileScanMap outScan = new FileScanMap(outHeapFileName, null, getConditionalExpression(leftValue), false);
        			Map outMap = outScan.get_next();
        			MapInsert mapinsert = new MapInsert(outputBigT, 1, outputBigTable);
        			
        			while(outMap!=null) {
        				
        				outMap.setFldOffset(outMap.getMapByteArray());
        				String outValue = outMap.getValue();
        				String outColumn = outMap.getColumnLabel();
        				String outRow = outMap.getRowLabel();
        				int outTimestamp = outMap.getTimeStamp();
        				
        			    	FileScanMap inScan = new FileScanMap(inHeapFileName, null, getConditionalExpression(rightValue), false);
        			    	
        				Map inMap = inScan.get_next();
        				
        				while(inMap!=null) {
        					inMap.setFldOffset(inMap.getMapByteArray());
        					String inValue = inMap.getValue();
        					String inColumn = inMap.getColumnLabel();
        					String inRow = inMap.getRowLabel();
        					int inTimestamp = inMap.getTimeStamp();
        					
        					if(outColumn.equals(inColumn)) {
        						
        						int finalTimestamp = outTimestamp;
        						if(inTimestamp>outTimestamp) {
        							finalTimestamp = inTimestamp;
        						}
        						outputMapVersion = mapinsert.run(outRow + ":"+inRow, outColumn, outValue,finalTimestamp, outputMapVersion);
        						
        					}
        					else {
        						
        						outputMapVersion = mapinsert.run(outRow + ":"+inRow, outColumn, outValue,outTimestamp, outputMapVersion);
        						outputMapVersion = mapinsert.run(outRow + ":"+inRow, inColumn, inValue,inTimestamp, outputMapVersion);
        					}
        					inMap = inScan.get_next();
        					
        				}
        				inScan.close();
            			
            			
        				outMap = outScan.get_next();	
        			}
        			outScan.close();
        			
        			
        			leftMap = outSortMapUnique.get_next();
        			rightMap = inSortMapUnique.get_next();
        		}
        		else if(leftValue.compareTo(rightValue)<0) {
        			leftMap = outSortMapUnique.get_next();
        		}
        		else  {
        			rightMap = inSortMapUnique.get_next();
        		}
        		
        	
        }
        outSortMapUnique.close();
        outScanUnique.close();
        inSortMapUnique.close();
        inScanUnique.close();
    	
    		return outputMapVersion;
    }
    
    public HashMap<String, List<Version>> performJoinNestedLoop(HashMap<String, List<Version>>  outputMapVersion) throws Exception{

		FileScanMap outScan = new FileScanMap(outHeapFileName, null, null, false);


		

		Map outMap = outScan.get_next();
		
		
		
		MapInsert mapinsert = new MapInsert(outputBigT, 1, outputBigTable);

		while (outMap != null) {
			outMap.setFldOffset(outMap.getMapByteArray());
			String outValue = outMap.getValue();
			String outColumn = outMap.getColumnLabel();
			String outRow = outMap.getRowLabel();
			int outTimestamp = outMap.getTimeStamp();
			
		    	FileScanMap inScan = new FileScanMap(inHeapFileName, null, null, false);
		    	
			Map inMap = inScan.get_next();
			
			while(inMap != null) {
				inMap.setFldOffset(inMap.getMapByteArray());
				String inValue = inMap.getValue();
				String inColumn = inMap.getColumnLabel();
				String inRow = inMap.getRowLabel();
				int inTimestamp = inMap.getTimeStamp();
				
				if(outValue.equals(inValue)) {
					
					if(outColumn.equals(inColumn)) {
						
					
						outputMapVersion = mapinsert.run(outRow + ":"+inRow, outColumn+"_left", outValue,outTimestamp, outputMapVersion);
						outputMapVersion = mapinsert.run(outRow + ":"+inRow, inColumn+"_right", inValue,inTimestamp, outputMapVersion);
						
					}
					else {
						
						outputMapVersion = mapinsert.run(outRow + ":"+inRow, outColumn, outValue,outTimestamp, outputMapVersion);
						outputMapVersion = mapinsert.run(outRow + ":"+inRow, inColumn, inValue,inTimestamp, outputMapVersion);
					}
				}
				
				inMap = inScan.get_next();
				
			}
			


			outMap = outScan.get_next();
		}
		outScan.close();
		
		return outputMapVersion;
    }
    
    public void generateUniqueValueFiles() throws UnknowAttrType, LowMemException, JoinsException, Exception {
    	
    		
    		
    		FileScanMap outScan = new FileScanMap(outHeapFileName, null, null, false);
        SortMap outSortMap = new SortMap(null, null, null, outScan, 7, new MapOrder(MapOrder.Ascending), null, this.numBuf);
        
        Map outPrevMap=null;
        Map outTemp = new Map();
        outTemp = outSortMap.get_next();

        String outValue = "";
        
        
        
        if(outTemp!=null){
            outTemp.setFldOffset(outTemp.getMapByteArray());
            outValue = outTemp.getValue();
            outPrevMap = new Map(outTemp);
        }
        while (true) {
            outTemp = outSortMap.get_next();
            if (outTemp == null) {
                break;
            }
            outTemp.setFldOffset(outTemp.getMapByteArray());
            if(!outTemp.getValue().equals(outValue)){
                this.outUniqueValue.insertRecordMap(outPrevMap.getMapByteArray());
                outValue = outTemp.getValue();
            }
            outPrevMap = new Map(outTemp);

        }
        if(outPrevMap!=null) {
            this.outUniqueValue.insertRecordMap(outPrevMap.getMapByteArray());
        }
        outSortMap.close();
        
        
        FileScanMap inScan = new FileScanMap(inHeapFileName, null, null, false);
        SortMap inSortMap = new SortMap(null, null, null, inScan, 7, new MapOrder(MapOrder.Ascending), null, this.numBuf);
        
        Map inPrevMap=null;
        Map inTemp = new Map();
        inTemp = inSortMap.get_next();

        String inValue = "";
        
        
        
        if(inTemp!=null){
            inTemp.setFldOffset(inTemp.getMapByteArray());
            inValue = inTemp.getValue();
            inPrevMap = new Map(inTemp);
        }
        while (true) {
            inTemp = inSortMap.get_next();
            if (inTemp == null) {
                break;
            }
            inTemp.setFldOffset(inTemp.getMapByteArray());
            if(!inTemp.getValue().equals(inValue)){
                this.inUniqueValue.insertRecordMap(inPrevMap.getMapByteArray());
                inValue = inTemp.getValue();
            }
            inPrevMap = new Map(inTemp);

        }
        if(inPrevMap!=null) {
            this.inUniqueValue.insertRecordMap(inPrevMap.getMapByteArray());
        }
        inSortMap.close();
        
    }
    
    public void buildTempHeapFiles(){
        try{
            this.outHeapFile = new Heapfile(this.outHeapFileName);
            this.inHeapFile = new Heapfile(this.inHeapFileName);
        }catch(Exception e){
            System.err.println("RowJoin.java: Error caused in creating temporary heapfiles");
        }

        try {
            // Creating tempHeapFile 1
            try{
                this.outStream = new Stream(this.outBigTable, null, 1, 1, "*",
                        "*", "*", this.numBuf);
            }catch(Exception e){
                System.err.println("RowJoin.java: Error caused in opening stream on bigtable1");
                return;
            }
            Map outPrevMap=null;
            Map outTemp = new Map();
            outTemp = this.outStream.getNext();

            String outColLabel = "";
            String outRowLabel ="";
            
            
            
            if(outTemp!=null){
                outTemp.setFldOffset(outTemp.getMapByteArray());
                outColLabel = outTemp.getColumnLabel();
                outRowLabel = outTemp.getRowLabel();
                outPrevMap = new Map(outTemp);
            }
            while (true) {
                outTemp = this.outStream.getNext();
                if (outTemp == null) {
                    break;
                }
                outTemp.setFldOffset(outTemp.getMapByteArray());
                if(!outTemp.getColumnLabel().equals(outColLabel)){
                    this.outHeapFile.insertRecordMap(outPrevMap.getMapByteArray());
                    outColLabel = outTemp.getColumnLabel();
                    outRowLabel = outTemp.getRowLabel();
                }
                else if(!outTemp.getRowLabel().equals(outRowLabel)) {
                		this.outHeapFile.insertRecordMap(outPrevMap.getMapByteArray());
                    outColLabel = outTemp.getColumnLabel();
                    outRowLabel = outTemp.getRowLabel();
                }
                outPrevMap = new Map(outTemp);

            }
            if(outPrevMap!=null) {
                this.outHeapFile.insertRecordMap(outPrevMap.getMapByteArray());
            }
            this.outStream.closestream();

            

        }catch(Exception e){
            System.err.println("RowJoin.java: Error caused in building temporary heapfiles");
        }
        
        
        try {
            // Creating tempHeapFile 2
            try{
                this.inStream = new Stream(this.inBigTable, null, 1, 1, "*",
                        "*", "*", this.numBuf);
            }catch(Exception e){
                System.err.println("RowJoin.java: Error caused in opening stream on bigtable1");
                return;
            }
            Map inPrevMap=null;
            Map inTemp = new Map();
            inTemp = this.inStream.getNext();

            String inColLabel = "";
            String inRowLabel="";
            
            
            
            if(inTemp!=null){
                inTemp.setFldOffset(inTemp.getMapByteArray());
                inColLabel = inTemp.getColumnLabel();
                inRowLabel = inTemp.getRowLabel();
                inPrevMap = new Map(inTemp);
            }
            while (true) {
                inTemp = this.inStream.getNext();
                if (inTemp == null) {
                    break;
                }
                inTemp.setFldOffset(inTemp.getMapByteArray());
                if(!inTemp.getColumnLabel().equals(inColLabel)){
                    this.inHeapFile.insertRecordMap(inPrevMap.getMapByteArray());
                    inColLabel = inTemp.getColumnLabel();
                    inRowLabel=inTemp.getColumnLabel();
                }
                else if(!inTemp.getRowLabel().equals(inRowLabel)) {
	                	this.inHeapFile.insertRecordMap(inPrevMap.getMapByteArray());
	                	inColLabel = inTemp.getColumnLabel();
	                inRowLabel=inTemp.getColumnLabel();
                }
                inPrevMap = new Map(inTemp);

            }
            if(inPrevMap!=null) {
                this.inHeapFile.insertRecordMap(inPrevMap.getMapByteArray());
            }
            this.inStream.closestream();

            

        }catch(Exception e){
            System.err.println("RowJoin.java: Error caused in building temporary heapfiles");
        }
    }



public CondExpr[] getConditionalExpression(String valueLabel) {
	CondExpr[] res = new CondExpr[2];
	CondExpr expr = new CondExpr();
	expr.op = new AttrOperator(AttrOperator.aopEQ);
	expr.type1 = new AttrType(AttrType.attrSymbol);
	expr.type2 = new AttrType(AttrType.attrString);
	expr.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 4);
	expr.operand2.string = valueLabel;
	expr.next = null;
	res[0] = expr;
	res[1] = null;

	return res;
}

}
