package tests;

import java.io.*;

import global.*;
import iterator.*;
import BigT.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;

class MainTest implements GlobalConst {
	public static String remove_dbcmd;
	public static String remove_logcmd;
	public static String replacement_policy = "Clock";

	public static String remove_cmd = "/bin/rm -rf ";
	public static String dbpath = "/tmp/maintest" + System.getProperty("user.name") + ".minibase-db";
	public static SystemDefs sysdef = null;
	public static HashMap<String, HashMap> tableVersions = new HashMap<>();

	public static void display() {
		System.out.println("Press 1 for Batch Insert");
		System.out.println("Press 2 for Query");
		System.out.println("Press 3 for getCounts");
		System.out.println("Press 4 for MapInsert");
		System.out.println("Press 5 for rowSort");
		System.out.println("Press 6 for createindex");
		System.out.println("Press 7 for rowjoin");
		System.out.println("Press 9 to Quit");
		System.out.println("----------------------------------------------------------------");

	}

	public static void m_persistenceDB(String batch) {
		String[] splits = batch.split(" ");

		SystemDefs.JavabaseDB.pcounter.initialize();
		try {
			sysdef.changeNumberOfBuffers(Integer.parseInt(splits[4]), replacement_policy);
			bigt big = new bigt(splits[3], Integer.parseInt(splits[2]));
			BatchInsert batchInsert = new BatchInsert(big, splits[1], Integer.parseInt(splits[2]), splits[3]);
			HashMap<String, List<Version>> mapVersion = null;
			if (!tableVersions.containsKey(splits[3])) {
				mapVersion = new HashMap<>();
			} else {
				mapVersion = tableVersions.get(splits[3]);
			}

			mapVersion = batchInsert.run(mapVersion);
			tableVersions.put(splits[3], mapVersion);
		} catch (Exception e) {
			System.out.println("Error Occured");
			e.printStackTrace();
		}
	}

	public static void main(String argv[]) throws IOException {

		SystemDefs sysdef = start();
		display();
		Scanner sc = new Scanner(System.in);
		String option = sc.nextLine();
		bigt big = null;
		File file = new File("/tmp/persistentDB.txt");

		if (file.length() != 0 && file.exists()) {
			List<String> lines = new ArrayList<>();
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line;
			while ((line = br.readLine()) != null)
				m_persistenceDB(line);
		}

		while (!option.equals("9")) {
			if (option.equals("1")) {
				FileWriter fileWriter = new FileWriter(file, true);
				BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
				System.out.println("FORMAT: batchinsert DATAFILENAME TYPE BIGTABLENAME NUMBUF");
				String batch = sc.nextLine();

				String[] splits = batch.split(" ");
				if (splits.length != 5) {
					System.out.println("Wrong format, try again!");
					display();
					option = sc.nextLine();
					continue;
				}
				bufferedWriter.write(batch + "\n");
				bufferedWriter.close();
				fileWriter.close();
				SystemDefs.JavabaseDB.pcounter.initialize();
				try {
					long startTime = System.nanoTime();
					sysdef.changeNumberOfBuffers(Integer.parseInt(splits[4]), replacement_policy);
					big = new bigt(splits[3], Integer.parseInt(splits[2]));
					BatchInsert batchInsert = new BatchInsert(big, splits[1], Integer.parseInt(splits[2]), splits[3]);
					HashMap<String, List<Version>> mapVersion = null;
					if (!tableVersions.containsKey(splits[3])) {
						mapVersion = new HashMap<>();
					} else {
						mapVersion = tableVersions.get(splits[3]);
					}

					mapVersion = batchInsert.run(mapVersion);
					tableVersions.put(splits[3], mapVersion);

					long endTime = System.nanoTime();
					System.out.println("TIME TAKEN " + ((endTime - startTime) / 1000000) + " ms");
					System.out.println("\n----------------------------------------------------------------\n\n");
					System.out.println("DISTINCT ROWS : " + big.getRowCnt());
					System.out.println("DISTINCT COLUMNS : " + big.getColumnCnt() + "\n");

				} catch (Exception e) {
					System.out.println("Error Occured");
					e.printStackTrace();
					display();
					option = sc.nextLine();
					continue;
				}
			} else if (option.equals("2")) {
				System.out.println("FORMAT: query BIGTABLENAME ORDERTYPE ROWFILTER COLUMNFILTER VALUEFILTER NUMBUF");
				String query = sc.nextLine();
				String[] splits = query.split(" ");
				if (splits.length != 7) {
					System.out.println("Wrong format, try again!");
					display();
					option = sc.nextLine();
					continue;
				}
				try {
					long startTime = System.nanoTime();
					sysdef.changeNumberOfBuffers(Integer.parseInt(splits[6]), replacement_policy);
					SystemDefs.JavabaseDB.pcounter.initialize();
					big = new bigt(splits[1]);
//					Stream stream = big.openStream(splits[1], Integer.parseInt(splits[7]), Integer.parseInt(splits[2]),
//							splits[3], splits[4], splits[5], (int) ((Integer.parseInt(splits[6]) * 3) / 4));
					Stream stream = big.openStream(splits[1], 1, Integer.parseInt(splits[2]), splits[3], splits[4],
							splits[5], (int) ((Integer.parseInt(splits[6]) * 3) / 4));
					int count = 0;
					Map t = stream.getNext();
					while (true) {
						if (t == null) {
							break;
						}
						count++;
						t.setFldOffset(t.getMapByteArray());
						t.print();
						t = stream.getNext();
					}
					stream.closestream();
					long endTime = System.nanoTime();
					System.out.println("Time Taken " + ((endTime - startTime) / 1000000) + " ms");
					System.out.println("Record Count : " + count);
					System.out.println("\n----------------------------------------------------------------\n\n");
					System.out.println("Distinct rows : " + big.getRowCnt());
					System.out.println("Distinct columns : " + big.getColumnCnt() + "\n");
				} catch (Exception e) {
					System.out.println("Error Occured");
					e.printStackTrace();
					display();
					option = sc.nextLine();
					continue;
				}
			} else if (option.equals("3")) {
				System.out.println("FORMAT: getCounts BIGTABLENAME NUMBUF");
				String[] splits = sc.nextLine().split(" ");
				if (splits.length != 3) {
					System.out.println("Wrong format, try again!");
					display();
					option = sc.nextLine();
					continue;
				}
				try {
					sysdef.changeNumberOfBuffers(Integer.parseInt(splits[2]), replacement_policy);
					SystemDefs.JavabaseDB.pcounter.initialize();

				} catch (Exception e) {
					System.err.println("MainTest.java: Exception in setting the NUMBUF");
					e.printStackTrace();
				}
				long startTime = System.nanoTime();
				try {
					GetAllCount getAllCount = new GetAllCount(Integer.parseInt(splits[2]));
					getAllCount.run(splits[1]);
				} catch (Exception e) {
					System.err.println("MainTest.java: Exception caused in executing getCounts");
					e.printStackTrace();
					display();
					option = sc.nextLine();
					continue;
				}
				long endTime = System.nanoTime();
				System.out.println("TIME TAKEN " + ((endTime - startTime) / 1000000000) + " s");
			} else if (option.equals("4")) {
				System.out.println("FORMAT: mapinsert ROWLABEL COLUMNLABEL VALUE TIMESTAMP TYPE BIGTABLENAME NUMBUF");
				String[] splits = sc.nextLine().split(" ");
				if (splits.length != 8) {
					System.out.println("Wrong format, try again!");
					display();
					option = sc.nextLine();
					continue;
				}
				try {
					sysdef.changeNumberOfBuffers(Integer.parseInt(splits[7]), replacement_policy);
					SystemDefs.JavabaseDB.pcounter.initialize();
				} catch (Exception e) {
					System.err.println("MainTest.java: Exception in setting the NUMBUF");
					e.printStackTrace();
				}
				try {
					bigt table = new bigt(splits[6], Integer.parseInt(splits[5]));
					
					HashMap<String, List<Version>> mapVersion = null;
					if (!tableVersions.containsKey(splits[6])) {
						mapVersion = new HashMap<>();
					} else {
						mapVersion = tableVersions.get(splits[6]);
					}

				
					MapInsert mapinsert = new MapInsert(table, Integer.parseInt(splits[5]), splits[6]);

					mapVersion = mapinsert.run(splits[1], splits[2], splits[3], Integer.parseInt(splits[4]), mapVersion);
					tableVersions.put(splits[6], mapVersion);
					System.out.println("Total number of maps: " + table.getMapCnt());

					System.out.println();

				} catch (Exception e) {
					System.err.println("MainTest.java: Exception caused in executing MapInsert");
					e.printStackTrace();
					display();
					option = sc.nextLine();
					continue;
				}
			} else if (option.equals("5")) {
				System.out.println("FORMAT: rowsort INBTNAME OUTBTNAME ROWORDER COLUMNNAME NUMBUF");
				System.out.println("ROWORDER: \n 1. Ascending\n 2. Descending");
				String[] splits = sc.nextLine().split(" ");
				if (splits.length != 6) {
					System.out.println("Wrong format, try again!");
					display();
					option = sc.nextLine();
					continue;
				}
				try {
					sysdef.changeNumberOfBuffers(Integer.parseInt(splits[5]), replacement_policy);
					SystemDefs.JavabaseDB.pcounter.initialize();

				} catch (Exception e) {
					System.err.println("MainTest.java: Exception in setting the NUMBUF");
					e.printStackTrace();
				}
				long startTime = System.nanoTime();
				try {
					HashMap<String, List<Version>> mapVersion = null;
					RowSort rowSort = new RowSort(splits[1], splits[2], Integer.parseInt(splits[3]), splits[4],
							(int) ((Integer.parseInt(splits[5]) * 3) / 4));
					mapVersion=tableVersions.get(splits[1]);
					rowSort.run_new(mapVersion);
				} catch (Exception e) {
					System.err.println("MainTest.java: Exception caused in executing RowSort");
					e.printStackTrace();
					display();
					option = sc.nextLine();
					continue;
				}
				long endTime = System.nanoTime();
				System.out.println("TIME TAKEN " + ((endTime - startTime) / 1000000000) + " s");

			} else if (option.equals("6")) {
				System.out.println("FORMAT: createindex BIGTABLENAME ORIGINAL_STORAGE_TYPE NEW_INDEX_TYPE");
				String[] splits = sc.nextLine().split(" ");
				if (splits.length != 4) {
					System.out.println("Wrong format, try again!");
					display();
					option = sc.nextLine();
					continue;
				}
				try {
//                    sysdef.changeNumberOfBuffers(Integer.parseInt(splits[5]), replacement_policy);
					SystemDefs.JavabaseDB.pcounter.initialize();

				} catch (Exception e) {
					System.err.println("MainTest.java: Exception in setting the NUMBUF");
					e.printStackTrace();
				}
				long startTime = System.nanoTime();
				try {
					bigt bt = new bigt(splits[1],Integer.parseInt(splits[2]));
					CreateIndex ci = new CreateIndex(splits[1], Integer.parseInt(splits[2]),
							Integer.parseInt(splits[3]), bt);
					ci.createIndex(tableVersions);

				} catch (Exception e) {
					System.err.println("MainTest.java: Exception caused in executing Createindex");
					e.printStackTrace();
					display();
					option = sc.nextLine();
					continue;
				}
				long endTime = System.nanoTime();
				System.out.println("TIME TAKEN " + ((endTime - startTime) / 1000000000) + " s");

			}
			else if (option.equals("7")) {
				System.out.println("FORMAT: rowjoin OUTTABLE INTABLE OUTPUTTABLE JOINTYPE NUMBUF COLUMNFILTER");
				System.out.println("JOINTYPE: ");
				System.out.println("1: Nested Loop Join");
				System.out.println("2: Sort Merge Join");
				String[] splits = sc.nextLine().split(" ");
				if (splits.length != 7) {
					System.out.println("Wrong format, try again!");
					display();
					option = sc.nextLine();
					continue;
				}
				try {
//                    sysdef.changeNumberOfBuffers(Integer.parseInt(splits[5]), replacement_policy);
					SystemDefs.JavabaseDB.pcounter.initialize();

				} catch (Exception e) {
					System.err.println("MainTest.java: Exception in setting the NUMBUF");
					e.printStackTrace();
				}
				long startTime = System.nanoTime();
				try {
					bigt bt = new bigt(splits[1]);
					
					RowJoin rj = new RowJoin(splits[1],splits[2],splits[3],Integer.parseInt(splits[4]),Integer.parseInt(splits[5]));
					
					HashMap<String, List<Version>> mapVersionOutputTable = null;
					if (!tableVersions.containsKey(splits[3])) {
						mapVersionOutputTable = new HashMap<>();
					} else {
						mapVersionOutputTable = tableVersions.get(splits[3]);
					}
					
					mapVersionOutputTable = rj.run(mapVersionOutputTable);
					tableVersions.put(splits[3], mapVersionOutputTable);
					
							

				} catch (Exception e) {
					System.err.println("MainTest.java: Exception caused in executing RowJoin");
					e.printStackTrace();
					display();
					option = sc.nextLine();
					continue;
				}
				long endTime = System.nanoTime();
				System.out.println("TIME TAKEN " + ((endTime - startTime) / 1000000000) + " s");

			}

			try {
				int read = SystemDefs.JavabaseDB.pcounter.getRCounter();
				int write = SystemDefs.JavabaseDB.pcounter.getWCounter();
				System.out.println("------------------------ Counts --------------------------------");
				System.out.println("READ COUNT : " + read);
				System.out.println("WRITE COUNT : " + write);
				System.out.println("----------------------------------------------------------------");

			} catch (Exception e) {
				System.out.println("Wrong Input!");
			}
			display();
			option = sc.nextLine();
		}
//		finalMethod();
		sc.close();
	}

	public static SystemDefs start() {
		String logpath = "/tmp/maintest" + System.getProperty("user.name") + ".minibase-log";
		String newdbpath;
		String newlogpath;

//		newdbpath = dbpath;
//		newlogpath = logpath;
//
//		remove_logcmd = remove_cmd + logpath;
//		remove_dbcmd = remove_cmd + dbpath;
//		try {
//			Runtime.getRuntime().exec(remove_logcmd);
//			Runtime.getRuntime().exec(remove_dbcmd);
//		} catch (IOException e) {
//			System.err.println("" + e);
//		}
//
//		remove_logcmd = remove_cmd + newlogpath;
//		remove_dbcmd = remove_cmd + newdbpath;
//		try {
//			Runtime.getRuntime().exec(remove_logcmd);
//			Runtime.getRuntime().exec(remove_dbcmd);
//		} catch (IOException e) {
//			System.err.println("" + e);
//		}
		sysdef = new SystemDefs(dbpath, 100000, 1000, "Clock");
		return sysdef;
	}

	public static void finalMethod() {
		remove_dbcmd = remove_cmd + dbpath;
		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		} catch (Exception e) {
			System.err.println("" + e);
		}
	}

}
