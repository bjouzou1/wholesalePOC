package bank_1.wholesale.services.priv_1.v1.common;

import com.wm.data.*;
import com.wm.util.Values;
import com.wm.app.b2b.server.Service;
import com.wm.app.b2b.server.ServiceException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class records

{
	// ---( internal utility methods )---

	final static records _instance = new records();

	static records _newInstance() { return new records(); }

	static records _cast(Object o) { return (records)o; }

	// ---( server methods )---




	public static final void mergeRecordsByMvtType (IData pipeline)
        throws ServiceException
	{
		// --- <<IS-START(mergeRecordsByMvtType)>> ---
		// @sigtype java 3.5
		// [i] record:0:required records
		// [o] record:0:required records_merged
		// input
		IDataCursor pipelineCursor = pipeline.getCursor();
		IData records = IDataUtil.getIData(pipelineCursor, "records");
		pipelineCursor.destroy();
		
		if (records != null) {
			IDataCursor recordsCursor = records.getCursor();
			
			HashMap<String, List<IData>> output_map = new HashMap<String, List<IData>>();
			
			// for each records list in input doc, get mvt code without first letter and sort in hashmap
			while (recordsCursor.hasMoreData()) {
				recordsCursor.next();
				
				IData[]	records_list = IDataUtil.getIDataArray(recordsCursor, recordsCursor.getKey());
				for (IData record:records_list) {
					
					IDataCursor recordCursor = record.getCursor();
					String mvt_code = IDataUtil.getString(recordCursor, "movement_code");
					mvt_code = mvt_code.substring(1);
					
					if (!output_map.containsKey(mvt_code)) {
						output_map.put(mvt_code, new ArrayList<IData>());
					}
					
					output_map.get(mvt_code).add(record);
				}
			}
			
			recordsCursor.destroy();
			
			IData records_merged = IDataFactory.create();
			IDataCursor records_mergedCursor = records_merged.getCursor();
			
			for (Map.Entry<String, List<IData>> entry:output_map.entrySet()) {
				
				IData[] mvt_list = new IData[entry.getValue().size()];
				mvt_list = entry.getValue().toArray(mvt_list);
				IDataUtil.put(records_mergedCursor, entry.getKey(), mvt_list);	
			}
			
			records_mergedCursor.destroy();
			
			// output
			IDataCursor pipelineCursor_out = pipeline.getCursor();
			IDataUtil.put( pipelineCursor_out, "records_merged", records_merged);
			pipelineCursor_out.destroy();
		}
			
			
		// --- <<IS-END>> ---

                
	}



	public static final void sortRecordsByMvtCode (IData pipeline)
        throws ServiceException
	{
		// --- <<IS-START(sortRecordsByMvtCode)>> ---
		// @sigtype java 3.5
		// [i] record:0:required records
		// [o] record:0:required records_sorted
		// input
		IDataCursor pipelineCursor = pipeline.getCursor();
		IData records = IDataUtil.getIData(pipelineCursor, "records");
		pipelineCursor.destroy();
		
		if (records != null) {
			IDataCursor recordsCursor = records.getCursor();
			
			HashMap<String, List<IData>> output_map = new HashMap<String, List<IData>>();
			
			// for each records list in input doc, get mvt code and sort in hashmap
			while (recordsCursor.hasMoreData()) {
				recordsCursor.next();
				
				IData[]	records_list = IDataUtil.getIDataArray(recordsCursor, recordsCursor.getKey());
				for (IData record:records_list) {
					
					IDataCursor recordCursor = record.getCursor();
					String mvt_code = IDataUtil.getString(recordCursor, "movement_code");
					
					if (mvt_code != null) {
						if (!output_map.containsKey(mvt_code)) {
							output_map.put(mvt_code, new ArrayList<IData>());
						}
						
						output_map.get(mvt_code).add(record);
					}
				}
			}
			
			recordsCursor.destroy();
			
			IData records_sorted = IDataFactory.create();
			IDataCursor records_sortedCursor = records_sorted.getCursor();
			
			for (Map.Entry<String, List<IData>> entry:output_map.entrySet()) {
				
				IData[] mvt_list = new IData[entry.getValue().size()];
				mvt_list = entry.getValue().toArray(mvt_list);
				IDataUtil.put(records_sortedCursor, entry.getKey(), mvt_list);	
			}
			
			records_sortedCursor.destroy();
			
			// output
			IDataCursor pipelineCursor_out = pipeline.getCursor();
			IDataUtil.put( pipelineCursor_out, "records_sorted", records_sorted);
			pipelineCursor_out.destroy();
		}
			
			
		// --- <<IS-END>> ---

                
	}
}

