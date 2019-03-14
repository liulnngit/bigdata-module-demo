package com.javadev.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Objects;
import com.javadev.demo.FingerVO;
/**
* @Description: HBase的增删改查操作
* @author ll-t150
* @date 2017年12月12日 下午8:25:11 
* @version V2.0
 */
public class HBaseUtil {
    public static final Log log = LogFactory.getLog(HBaseUtil.class);
    
    public static Connection connection;
    
    public static Connection getConnection(){
        if(connection==null){
            connection = HbaseConnection.getConnection();
        }
        return connection;
    } 

    /**
     * @param tableName 创建一个表 tableName 指定的表名 seriesStr
     * @param seriesStr 以字符串的形式指定表的列族，每个列族以逗号的形式隔开,(例如：＂f1,f2＂两个列族，分别为f1和f2)
     **/
    public static boolean createTable(String tableName, String seriesStr) {
        boolean isSuccess = false;
        Admin admin = null;
        TableName table = TableName.valueOf(tableName);
        try {
            admin = getConnection().getAdmin();
            if (!admin.tableExists(table)) {
                log.info("INFO:Hbase::  " + tableName + "原数据库中表不存在！开始创建...");
                HTableDescriptor descriptor = new HTableDescriptor(table);
                String[] series = seriesStr.split(",");
                for (String s : series) {
                    descriptor.addFamily(new HColumnDescriptor(s.getBytes()));
                }
                admin.createTable(descriptor);
                log.info("INFO:Hbase::  " + tableName + "新的" + tableName + "表创建成功！");
                isSuccess = true;
            } else {
                log.info("INFO:Hbase::  该表已经存在，不需要在创建！");
            }
        } catch (Exception e) {
            log.error("createTable is exception, " + e);
        } finally {
            IOUtils.closeQuietly(admin);
        }
        return isSuccess;
    }

    /**
     * 向指定表中插入数据(更行数据共用)
     * @param tableName 要插入数据的表名
     * @param rowkey 指定要插入数据的表的行键
     * @param family 指定要插入数据的表的列族family
     * @param qualifier 要插入数据的qualifier
     * @param value 要插入数据的值value
     */
    public static void insert(String tableName,String rowkey,String family,String qualifier,long time,String value)throws IOException {
        Admin admin = null;
        Table table = null;
        TableName tName = TableName.valueOf(tableName);
        try{
            admin = getConnection().getAdmin();
            if (admin.tableExists(tName)) {
                table = connection.getTable(tName);
                Put put = new Put(rowkey.getBytes());
                put.addColumn(family.getBytes(), qualifier.getBytes(), time, value.getBytes());
                table.put(put);
                log.debug("插入数据到HBase"+tableName+"表完成！");
            } else {
                log.info("插入数据的表不存在，请指定正确的tableName ! ");
            }
        } catch (Exception e) {
            log.error("insert is exception, " + e);
        }finally{
            if(table !=null){
                table.close();
            }
        }
    }
    
    
    /**
     * 根据指定表获取指定行键rowKey的所有数据 并以Map集合的形式返回查询到的结果 每条数据之间用&&&将Qualifier和Value进行区分
     * @param tableName 要获取表 tableName 的表名
     * @param rowkey 指定的行键rowKey
     * @throws IOException 
     */
    public static JSONObject getFromRowkeyValuesToJson(String tableName, String rowkey) throws IOException {
        Table table = null;
        JSONObject columnJson = new JSONObject();
        JSONObject qualifierJson = new JSONObject();
        try {
            connection = getConnection();
            Get get = new Get(Bytes.toBytes(rowkey));
            table = connection.getTable(TableName.valueOf(tableName));
            Result r = table.get(get);
            for (Cell cell : r.rawCells()) {
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                // 每条数据之间用&&&将Qualifier和Value进行区分
                if ("" != qualifier) {
                    qualifierJson.put(qualifier, value);
                    columnJson.put(family, qualifierJson);
                }else {
                    columnJson.put(family, value);
                }
            }
        } catch (Exception e) {
            log.error("getFromRowkeyValuesToJson出错："+e);
        }finally{
            if(table !=null){
                table.close();
            }
        }
        return columnJson;
    }
    
    /**
     * 根据table查询数据
     */
    public static void getValueByTable(String tableName) throws Exception {
		Table table = null;
		try {
			connection = getConnection();
			table = connection.getTable(TableName.valueOf(tableName));
			ResultScanner scanner = table.getScanner(new Scan());

			for (Result r : scanner) {
				System.out.println("获得到rowkey:" + new String(r.getRow()));
				
				for (Cell cell : r.rawCells()) { 
					System.out.println("列：" +
						 new String(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength()) + ":" + 
						 new String(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength()) + "====值:" + 
						 new String(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength())); 
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}
    }
    
    public static List<FingerVO> getFinger(String tableName) throws Exception {
		Table table = null;
		List<FingerVO> fingers = new ArrayList<FingerVO>();
		try {
			connection = getConnection();
			table = connection.getTable(TableName.valueOf(tableName));
			ResultScanner scanner = table.getScanner(new Scan());
			
			for (Result r : scanner) {
				for (Cell cell : r.rawCells()) { 
					String key = new String(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
					if("fingerParameters".equals(key)){
						String value =  new String(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
						//System.err.println("获得到rowkey:" + new String(r.getRow()));
						try {
							JSONArray jsonArr = JSONObject.parseArray(value);
							FingerVO finger = new FingerVO();
							for (Object object : jsonArr) {
								JSONObject jsonObj = (JSONObject)object;
								String field = String.valueOf(jsonObj.get("key"));
								String val = String.valueOf(jsonObj.get("value"));
								if(Objects.equal(field,"user_agent")){ finger.setUser_agent(val);}
								if(Objects.equal(field,"language")){finger.setLanguage(val);}
								if(Objects.equal(field,"color_depth")){finger.setColor_depth(val);}
								if(Objects.equal(field,"pixel_ratio")){finger.setPixel_ratio(val);}
								if(Objects.equal(field,"resolution")){finger.setResolution(val);}
								if(Objects.equal(field,"available_resolution")){finger.setAvailable_resolution(val);}
								if(Objects.equal(field,"timezone_offset")){finger.setTimezone_offset(val);}
								if(Objects.equal(field,"session_storage")){finger.setSession_storage(val);}
								if(Objects.equal(field,"local_storage")){finger.setLocal_storage(val);}
								if(Objects.equal(field,"indexed_db")){finger.setIndexed_db(val);}
								if(Objects.equal(field,"open_database")){finger.setOpen_database(val);}
								if(Objects.equal(field,"cpu_class")){finger.setCpu_class(val);}
								if(Objects.equal(field,"navigator_platform")){finger.setNavigator_platform(val);}
								if(Objects.equal(field,"do_not_track")){finger.setDo_not_track(val);}
								if(Objects.equal(field,"regular_plugins")){finger.setRegular_plugins(val);}
								if(Objects.equal(field,"canvas")){finger.setCanvas(val);}
								if(Objects.equal(field,"webgl")){finger.setWebgl(val);}
								if(Objects.equal(field,"adblock")){finger.setAdblock(val);}
								if(Objects.equal(field,"has_lied_languages")){finger.setHas_lied_browser(val);}
								if(Objects.equal(field,"has_lied_resolution")){finger.setHas_lied_resolution(val);}
								if(Objects.equal(field,"has_lied_os")){finger.setHas_lied_os(val);}
								if(Objects.equal(field,"has_lied_browser")){finger.setHas_lied_browser(val);}
								if(Objects.equal(field,"touch_support")){finger.setTouch_support(val);}
								if(Objects.equal(field,"js_fonts")){finger.setJs_fonts(val);}
							}
							fingers.add(finger);
						} catch (Exception e) {
							continue;
						}
					}
					
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		return fingers;
    }
    
}
