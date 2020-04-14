using ERP8.Common;
using NcDatabaseToSQLForApps;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Web;
using System.Web.Services;

namespace NcDatabaseToSQL
{
    /// <summary>
    /// NcDatabaseToSQL 的摘要说明
    /// </summary>
    [WebService(Namespace = "http://tempuri.org/")]
    [WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
    [System.ComponentModel.ToolboxItem(false)]
    // 若要允许使用 ASP.NET AJAX 从脚本中调用此 Web 服务，请取消注释以下行。 
    // [System.Web.Script.Services.ScriptService]
    public class NCU8Compare : System.Web.Services.WebService
    {
        //开始时间
        string startTime = DateTime.Now.AddDays(1 - DateTime.Now.Day).AddMonths(-1).ToString("yyyy-MM-dd");
        //结束时间
        string endTime = DateTime.Now.AddDays(1 - DateTime.Now.Day).AddDays(-1).ToString("yyyy-MM-dd");

        private static string connectionString = ConfigurationManager.ConnectionStrings["U8Conn"].ToString();

        private static string conneU8ctionString = ConfigurationManager.ConnectionStrings["U8DataConn"].ToString();

        private static string conneAppsDBctionString = ConfigurationManager.ConnectionStrings["AppsDBCoon"].ToString();

        private static string connNCSubjectEncoding = ConfigurationManager.ConnectionStrings["NCSubjectEncoding"].ToString();
        //string startTime = "2019-09-01";
        //string endTime = "2019-09-30";
        //[WebMethod]
        //public string HelloWorld()
        //{
        //    return "Hello World";
        //}

        string queryDate = "";

        /// <summary>
        /// 公共调用方法
        /// 创建人：吕贺
        /// 创建时间：2019年10月23日 13:55:45
        /// </summary>
        /// <returns></returns>
        [WebMethod]
        public string NcInsertToSql(string queryDateParameters)
        {
            string msg = "";
            queryDate = queryDateParameters;
            if (string.IsNullOrEmpty(queryDate))
            {
                string delstrApps = "truncate table Spl_NCU8Compare";
                SqlHelperForApps.ExecuteNonQuerys(delstrApps);
                msg = "请先选择查询条件";
            }
            else
            {
                GetPurchaseinToSql();
                GetPurBillVouchToSql();
                GetSaleBillVouchToSql();
                GetDispatchListToSql();
                GetWhstransToSql();
                GetGeneraloutToSql();
                GetGeneralinToSql();
                GetTransformToSql();
                msg = Ncu8Compare();
            }
            return msg;
        }

        /// <summary>
        /// 从U8获取采购入库数据
        /// 创建人：lvhe
        /// 创建时间：2020-3-4 16:16:40
        /// </summary>
        /// <returns></returns>
        [WebMethod]
        private string GetPurchaseinToSql()
        {
            string result = "";
            string createSql = "";
            string tableExist = "";
            int existResult = 0;
            string msg = "";
            string sql = "";
            int updateCount = 0;
            StringBuilder strbu = new StringBuilder();
            string strGetOracleSQLIn = "";
            try
            {
                //string delstrApps = "truncate table Spl_NCU8Compare";
                //SqlHelperForApps.ExecuteNonQuerys(delstrApps);
                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id( 'Spl_U8DBData') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);
                if (existResult == 0)
                {
                    //获取采购入库头数据
                    sql = "select '采购入库' cbustype,A.cRdCode cRdCode,A1.cInvCode cInvCode,isnull(SUM(A1.iQuantity),0) iQuantity,A.cWhCode publicsec1,A2.cWhName publicsec2,GETDATE() as CreateTime from RdRecord01 A left join RdRecords01 A1 on A.ID=A1.ID left join Warehouse A2 on A2.cWhCode=A.cWhCode WHERE Convert(nvarchar(7),A.dDate,121)='" + queryDate + "' GROUP BY A.cRdCode,A1.cInvCode,A.cWhCode,A2.cWhName";
                }
                else
                {
                    string delstr = "DROP table Spl_U8DBData";
                    SqlHelper.ExecuteNonQuerys(delstr);
                    sql = "select '采购入库' cbustype,A.cRdCode cRdCode,A1.cInvCode cInvCode,isnull(SUM(A1.iQuantity),0) iQuantity,A.cWhCode publicsec1,A2.cWhName publicsec2,GETDATE() as CreateTime from RdRecord01 A left join RdRecords01 A1 on A.ID=A1.ID left join Warehouse A2 on A2.cWhCode=A.cWhCode WHERE Convert(nvarchar(7),A.dDate,121)='" + queryDate + "' GROUP BY A.cRdCode,A1.cInvCode,A.cWhCode,A2.cWhName";
                }
                //获取采购入库头数据
                DataSet PurchaseIn = SqlHelperForU8.ExecuteDataset(conneU8ctionString, CommandType.Text, sql);


                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id( 'Spl_NCDBData') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    //获取采购入库行数据
                    sql = "SELECT '采购入库' cbustype,CASE WHEN A.Vtrantypecode='45-01' THEN '101' ELSE A.Vtrantypecode END AS cRdCode,A2.code cInvCode,nvl(sum(A1.nassistnum),0) iQuantity,A5.code publicsec1,A5.name publicsec2,to_char(sysdate,'yyyy-mm-dd hh24:mi:ss')  as CreateTime  FROM ic_purchasein_h A left join ic_purchasein_b A1 on A1.cgeneralhid = A.cgeneralhid and A1.dr!=1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc left join bd_stordoc A5 ON A5.Pk_Stordoc=A.cwarehouseid where A.PK_ORG = '0001A110000000001V70' AND substr(A.dbilldate,0,7)= '" + queryDate + "' and A.fbillflag = 3 and substr(A2.code,0,4) != '0915' and A.cwarehouseid  not in('1001A1100000000T5S5Z','1001A11000000003CYSY') group by A2.code,A.Vtrantypecode,A5.code,A5.name";
                }
                else
                {
                    string delstr = "DROP table Spl_NCDBData";
                    SqlHelper.ExecuteNonQuerys(delstr);
                    sql = "SELECT '采购入库' cbustype,CASE WHEN A.Vtrantypecode='45-01' THEN '101' ELSE A.Vtrantypecode END AS cRdCode,A2.code cInvCode,nvl(sum(A1.nassistnum),0) iQuantity,A5.code publicsec1,A5.name publicsec2,to_char(sysdate,'yyyy-mm-dd hh24:mi:ss')  as CreateTime  FROM ic_purchasein_h A left join ic_purchasein_b A1 on A1.cgeneralhid = A.cgeneralhid and A1.dr!=1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc left join bd_stordoc A5 ON A5.Pk_Stordoc=A.cwarehouseid where A.PK_ORG = '0001A110000000001V70' AND substr(A.dbilldate,0,7)= '" + queryDate + "' and A.fbillflag = 3 and substr(A2.code,0,4) != '0915' and A.cwarehouseid  not in('1001A1100000000T5S5Z','1001A11000000003CYSY') group by A2.code,A.Vtrantypecode,A5.code,A5.name";
                }

                //获取采购入库行数据
                DataSet PurchaseInLine = OracleHelper.ExecuteDataset(sql);

                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id( 'Spl_U8DBData') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "CREATE TABLE [dbo].[Spl_U8DBData]([Id] [int] IDENTITY (1, 1) NOT NULL,[cbustype] [varchar](50) NULL,[cRdCode] [varchar](50) NULL,[cInvCode] [varchar](50) NULL,[iQuantity] [decimal](18, 2) NULL,[publicsec1] [varchar](50) NULL,[CreateTime] [datetime] NOT NULL)";
                    SqlHelper.ExecuteNonQuery(createSql);
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(PurchaseIn, "Spl_U8DBData");
                    SqlHelper.ExecuteNonQuery(str.ToString());
                    //msg = "采购入库U8数据插入成功";
                }
                else
                {
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(PurchaseIn, "Spl_U8DBData");
                    SqlHelper.ExecuteNonQuery(str.ToString());
                    //msg = "采购入库U8数据更新成功";
                }

                tableExist = "if object_id( 'Spl_NCDBData') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "CREATE TABLE [dbo].[Spl_NCDBData]([Id] [int] IDENTITY (1, 1) NOT NULL,[cbustype] [varchar](50) NULL,[cRdCode] [varchar](50) NULL,[cInvCode] [varchar](50) NULL,[iQuantity] [decimal](18, 2) NULL,[publicsec1] [varchar](50) NULL,[CreateTime] [datetime] NOT NULL)";
                    SqlHelper.ExecuteNonQuery(createSql);
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(PurchaseInLine, "Spl_NCDBData");
                    SqlHelper.ExecuteNonQuery(str.ToString());
                    // msg = "采购入库NC数据插入成功";
                }
                else
                {
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(PurchaseInLine, "Spl_NCDBData");
                    SqlHelper.ExecuteNonQuery(str.ToString());
                    // msg = "采购入库NC数据更新成功";
                }
                // result = msg;
            }
            catch (Exception e)
            {

                result = "采购入库表错误：" + e.Message;
            }
            return result;
        }


        /// <summary>
        /// 从U8获取销售出库数据
        /// 创建人：lvhe
        /// 创建时间：2020-3-5 14:17:47
        /// </summary>
        /// <returns></returns>
        //[WebMethod]
        public string GetDispatchListToSql()
        {
            string result = "";
            string sql = "";
            StringBuilder strbu = new StringBuilder();
            try
            {
                //获取销售出库数据
                sql = "select '销售出库' cbustype,A.cSTCode cRdCode,A1.cInvCode cInvCode,isnull(SUM(A1.iQuantity),0) iQuantity,A1.cWhCode publicsec1,A2.cWhCode publicsec2,GETDATE() as CreateTime from DispatchList A left join DispatchLists A1 on A.DLID=A1.DLID left join Warehouse A2 on A2.cWhCode=A1.cWhCode WHERE Convert(nvarchar(7),A.dDate,121)='" + queryDate + "' GROUP BY A.cSTCode,A1.cInvCode,A1.cWhCode,A2.cWhCode";

                //获取销售出库数据
                DataSet Dispatch = SqlHelperForU8.ExecuteDataset(conneU8ctionString, CommandType.Text, sql);

                //获取销售出库数据
                sql = "select'销售出库' cbustype,A5.pk_billtypecode cRdCode,A2.code cInvCode,nvl(sum(A1.nassistnum),0) iQuantity,A6.code publicsec1,A6.Name publicsec2,to_char(sysdate,'yyyy-mm-dd hh24:mi:ss')  as CreateTime from ic_saleout_h A left join ic_saleout_b A1 on A.cgeneralhid = A1.cgeneralhid and A1.DR != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc left join bd_billtype A5 on A5.pk_billtypeid =A.ctrantypeid left join bd_stordoc A6 on A6.pk_stordoc =A.cwarehouseid  where A.PK_ORG = '0001A110000000001V70' and A.DR != 1 AND substr(A.dbilldate,0,7)= '" + queryDate + "'  and A.fbillflag = 3 and substr(A2.code,0,4) != '0915' and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY') group by A2.code,A5.pk_billtypecode,A6.code,A6.Name";

                //获取销售出库数据
                DataSet Dispatchs = OracleHelper.ExecuteDataset(sql);

                StringBuilder str = DataSetToArrayList.DataSetToArrayLists(Dispatch, "Spl_U8DBData");
                SqlHelper.ExecuteNonQuery(str.ToString());

                StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(Dispatchs, "Spl_NCDBData");
                SqlHelper.ExecuteNonQuery(str.ToString());
            }
            catch (Exception e)
            {

                result = "销售出库插入错误：" + e.Message;
            }
            return result;
        }


        /// <summary>
        /// 从U8获取采购发票数据
        /// 创建人：lvhe
        /// 创建时间：2020-3-9 23:59:28
        /// </summary>
        /// <returns></returns>
        [WebMethod]
        private string GetPurBillVouchToSql()
        {
            string result = "";
            string sql = "";
            StringBuilder strbu = new StringBuilder();
            try
            {
                //获取采购发票数据
                sql = "select '采购发票' cbustype,A.cPBVCode publicsec4,A1.cInvCode cInvCode,A.cVenCode publicsec1,A2.cVenName publicsec3,isnull(SUM(A1.iPBVQuantity),0) iQuantity,isnull(SUM(A1.iSum),0) publicsec2,GETDATE() as CreateTime from PurBillVouch A left join PurBillVouchs A1 on A.PBVID=A1.PBVID WHERE Convert(nvarchar(7),A.dPBVDate,121)='" + queryDate + "' GROUP BY A1.cInvCode,A.cPBVBillType,A.cVenCode,A2.cVenName,A.cPBVCode";

                //获取采购发票数据
                DataSet PurBillVouch = SqlHelperForU8.ExecuteDataset(conneU8ctionString, CommandType.Text, sql);

                //获取采购发票数据
                sql = "select '采购发票' cbustype,A.vbillcode publicsec4,A2.code cInvCode,A3.code publicsec1,A3.Name publicsec3,nvl(sum(A1.nastnum),0) iQuantity,nvl(sum(A1.ntaxmny),0) publicse2,to_char(sysdate,'yyyy-mm-dd hh24:mi:ss')  as CreateTime from po_invoice A left join po_invoice_b A1 on A.PK_INVOICE = A1.PK_INVOICE and A1.DR!=1 left join bd_material A2 on A1.pk_material = A2.pk_material left join bd_supplier A3 on A.PK_SUPPLIER=A3.PK_SUPPLIER where A.PK_ORG = '0001A110000000001V70' and A.DR != 1 AND substr(A.taudittime,0,7)= '" + queryDate + "' and A.fbillstatus = 3 and substr(A2.code,0,4) != '0915' group by A2.code,A.Vtrantypecode,A3.code,A3.Name,A.vbillcode publicsec4";

                //获取采购发票数据
                DataSet PurBillVouchs = OracleHelper.ExecuteDataset(sql);

                StringBuilder str = DataSetToArrayList.DataSetToArrayLists(PurBillVouch, "Spl_U8DBData");
                SqlHelper.ExecuteNonQuery(str.ToString());

                StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(PurBillVouchs, "Spl_NCDBData");
                SqlHelper.ExecuteNonQuery(str.ToString());
            }
            catch (Exception e)
            {

                result = "采购发票插入错误：" + e.Message;
            }
            return result;
        }


        /// <summary>
        /// 从U8获取销售发票数据
        /// 创建人：lvhe
        /// 创建时间：2020-3-9 23:59:28
        /// </summary>
        /// <returns></returns>
        [WebMethod]
        private string GetSaleBillVouchToSql()
        {
            string result = "";
            string sql = "";
            StringBuilder strbu = new StringBuilder();
            try
            {
                //获取采购发票数据
                sql = "select '销售发票' cbustype,A.cSBVCode publicsec4,A1.cInvCode cInvCode,A.cCusCode publicsec1,A.cCusName publicsec3,isnull(SUM(A1.iQuantity),0) iQuantity,isnull(SUM(A1.iSum),0) publicsec2,GETDATE() as CreateTime from SaleBillVouch A left join SaleBillVouchs A1 on A1.SBVID=A.SBVID WHERE Convert(nvarchar(7),A.dDate,121)='" + queryDate + "' GROUP BY A1.cInvCode,A.cCusCode,A.cCusName,A.cSBVCode";

                //获取采购发票数据
                DataSet PurBillVouch = SqlHelperForU8.ExecuteDataset(conneU8ctionString, CommandType.Text, sql);

                //获取采购发票数据
                sql = "select '销售发票' cbustype,A.vbillcode publicsec4,A2.code cInvCode,A3.Code publicsec1,A3.Name publicsec3,nvl(sum(A1.nastnum),0) iQuantity,nvl(sum(A1.ntaxmny),0) publicsec2,to_char(sysdate,'yyyy-mm-dd hh24:mi:ss')  as CreateTime from so_saleinvoice A left join so_saleinvoice_b A1 on A.csaleinvoiceid = A1.csaleinvoiceid and A1.dr!=1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_customer A3 on  A.cinvoicecustid=A3.pk_customer where A.PK_ORG = '0001A110000000001V70' and A.dr != 1 AND substr(A.dbilldate,0,7)= '" + queryDate + "' and substr(A2.code,0,4) != '0915' and A1.csendstordocid not in('1001A1100000000T5S5Z','1001A11000000003CYSY') GROUP BY A2.code,A3.Code,A3.Name,A.vbillcode";

                //获取采购发票数据
                DataSet PurBillVouchs = OracleHelper.ExecuteDataset(sql);

                StringBuilder str = DataSetToArrayList.DataSetToArrayLists(PurBillVouch, "Spl_U8DBData");
                SqlHelper.ExecuteNonQuery(str.ToString());

                StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(PurBillVouchs, "Spl_NCDBData");
                SqlHelper.ExecuteNonQuery(str.ToString());
            }
            catch (Exception e)
            {

                result = "采购发票插入错误：" + e.Message;
            }
            return result;
        }


        /// <summary>
        /// 从U8获取转库入库数据
        /// 创建人：lvhe
        /// 创建时间：2020-3-4 16:16:40
        /// </summary>
        /// <returns></returns>
        [WebMethod]
        private string GetWhstransToSql()
        {
            string result = "";
            string sql = "";
            StringBuilder strbu = new StringBuilder();
            try
            {
                //获取转库入库数据
                sql = "select '转库入库' cbustype,A1.cInvCode cInvCode,isnull(SUM(A1.iTVQuantity),0) iQuantity,A.ciwhcode publicsec1,A2.cWhName publicsec2,GETDATE() as CreateTime from TransVouch A left join TransVouchs A1 on A.ID=A1.ID  left join Warehouse A2 on A2.cWhCode=A.ciwhcode WHERE Convert(nvarchar(7),A.dTVDate,121)='" + queryDate + "' group by A1.cInvCode,A.ciwhcode,A2.cWhName";

                //获取转库入库数据
                DataSet TransVouch = SqlHelperForU8.ExecuteDataset(conneU8ctionString, CommandType.Text, sql);

                //获取转库入库数据
                sql = "select '转库入库' cbustype,A2.code cInvCode,nvl(sum(A1.nassistnum),0) iQuantity,A5.code publicsec1,A5.name publicsec2,to_char(sysdate,'yyyy-mm-dd hh24:mi:ss')  as CreateTime FROM ic_whstrans_h A left join ic_whstrans_b A1 on A1.cspecialhid = A.cspecialhid and A1.dr != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc left join bd_stordoc A5 on A5.pk_stordoc = A.cwarehouseid where A.PK_ORG = '0001A110000000001V70' AND substr(A.dbilldate,0,7)= '" + queryDate + "' and A.fbillflag = 4 and substr(A2.code,0,4) != '0915' and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY') and A.cotherwhid  not in('1001A1100000000T5S5Z','1001A11000000003CYSY') GROUP BY A2.code,A5.code,A5.name";

                //获取转库入库数据
                DataSet TransVouchs = OracleHelper.ExecuteDataset(sql);

                StringBuilder str = DataSetToArrayList.DataSetToArrayLists(TransVouch, "Spl_U8DBData");
                SqlHelper.ExecuteNonQuery(str.ToString());

                StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(TransVouchs, "Spl_NCDBData");
                SqlHelper.ExecuteNonQuery(str.ToString());
            }
            catch (Exception e)
            {

                result = "转库入库插入错误：" + e.Message;
            }
            return result;
        }


        /// <summary>
        /// 从U8获取其他出库单数据
        /// 创建人：lvhe
        /// 创建时间：2020-3-4 16:16:40
        /// </summary>
        /// <returns></returns>
        [WebMethod]
        private string GetGeneraloutToSql()
        {
            string result = "";
            string sql = "";
            StringBuilder strbu = new StringBuilder();
            try
            {
                //获取其他出库单数据
                sql = "select '其他出库单' cbustype,A1.cInvCode,SUM(A1.iQuantity) iQuantity,A.cWhCode publicsec1,A2.cWhName publicsec2,GETDATE() as CreateTime from RdRecord09 A left join rdrecords09 A1 on A.ID=A1.ID left join Warehouse A2 on A2.cWhCode=A.cWhCode WHERE Convert(nvarchar(7),A.dDate,121)='" + queryDate + "' and A.cRdCode NOT IN ('207','210') group by A1.cInvCode,A.cWhCode,A2.cWhName";

                //获取其他出库单数据
                DataSet Generalout = SqlHelperForU8.ExecuteDataset(conneU8ctionString, CommandType.Text, sql);

                //获取其他出库单数据
                sql = "select '其他出库单' cbustype,A2.code cInvCode,nvl(sum(A1.nshouldassistnum),0) iQuantity,A6.code publicsec1,A6.name publicsec2,to_char(sysdate,'yyyy-mm-dd hh24:mi:ss')  as CreateTime FROM ic_generalout_h A left join ic_generalout_b A1 on A1.cgeneralhid = A.cgeneralhid and A1.dr != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 on A3.pk_marbasclass = A2.pk_marbasclass left join bd_measdoc A4 on A4.pk_measdoc = A2.pk_measdoc left join bd_billtype A5 on A5.pk_billtypeid = A.ctrantypeid left join bd_stordoc A6 on A6.pk_stordoc = A.cwarehouseid where  (A.vtrantypecode not IN ('4I-02','4I-06')) AND substr(A.dbilldate,0,7)= '" + queryDate + "' and A2.CODE NOT LIKE '0915%' and A6.CODE NOT IN('PPG','STJGC') and A.PK_ORG='0001A110000000001V70' group by A2.code,A6.code,A6.name";

                //获取其他出库单数据
                DataSet GeneraloutLine = OracleHelper.ExecuteDataset(sql);

                StringBuilder str = DataSetToArrayList.DataSetToArrayLists(Generalout, "Spl_U8DBData");
                SqlHelper.ExecuteNonQuery(str.ToString());

                StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(GeneraloutLine, "Spl_NCDBData");
                SqlHelper.ExecuteNonQuery(str.ToString());
            }
            catch (Exception e)
            {

                result = "其他出库插入错误：" + e.Message;
            }
            return result;
        }


        /// <summary>
        /// 从U8获取其他入库单数据
        /// 创建人：lvhe
        /// 创建时间：2020-3-4 16:16:40
        /// </summary>
        /// <returns></returns>
        [WebMethod]
        private string GetGeneralinToSql()
        {
            string result = "";
            string sql = "";
            StringBuilder strbu = new StringBuilder();
            try
            {
                //获取其他出库单数据
                sql = "select '其他入库单' cbustype,A1.cInvCode,SUM(A1.iQuantity) iQuantity,A.cWhCode publicsec1,A2.cWhName publicsec2,GETDATE() as CreateTime from RdRecord08 A left join rdrecords08 A1 on A.ID=A1.ID left join Warehouse A2 on A2.cWhCode=A.cWhCode WHERE Convert(nvarchar(7),A.dDate,121)='" + queryDate + "' and A.cRdCode NOT IN ('107','105') group by A1.cInvCode,A.cWhCode,A2.cWhName";

                //获取其他出库单数据
                DataSet Generalout = SqlHelperForU8.ExecuteDataset(conneU8ctionString, CommandType.Text, sql);

                //获取其他出库单数据
                sql = "select '其他入库单' cbustype,A2.code cInvCode,nvl(sum(A1.nshouldassistnum),0) iQuantity,A6.code publicsec1,A6.name publicsec2,to_char(sysdate,'yyyy-mm-dd hh24:mi:ss')  as CreateTime FROM ic_generalin_h A left join ic_generalin_b A1 on A1.cgeneralhid = A.cgeneralhid and A1.dr != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 on A3.pk_marbasclass = A2.pk_marbasclass left join bd_measdoc A4 on A4.pk_measdoc = A2.pk_measdoc left join bd_billtype A5 on A5.pk_billtypeid = A.ctrantypeid left join bd_stordoc A6 on A6.pk_stordoc = A.cwarehouseid where  (A.vtrantypecode not IN ('4A-02','4A-06')) AND substr(A.dbilldate,0,7)= '" + queryDate + "' and A2.CODE NOT LIKE '0915%' and A6.CODE NOT IN('PPG','STJGC') and A.PK_ORG='0001A110000000001V70' group by A2.code,A6.code,A6.name";

                //获取其他出库单数据
                DataSet GeneraloutLine = OracleHelper.ExecuteDataset(sql);

                StringBuilder str = DataSetToArrayList.DataSetToArrayLists(Generalout, "Spl_U8DBData");
                SqlHelper.ExecuteNonQuery(str.ToString());

                StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(GeneraloutLine, "Spl_NCDBData");
                SqlHelper.ExecuteNonQuery(str.ToString());
            }
            catch (Exception e)
            {

                result = "其他入库插入错误：" + e.Message;
            }
            return result;
        }


        /// <summary>
        /// 从U8获取形态转换单数据
        /// 创建人：lvhe
        /// 创建时间：2020-3-4 16:16:40
        /// </summary>
        /// <returns></returns>
        [WebMethod]
        private string GetTransformToSql()
        {
            string result = "";
            string sql = "";
            StringBuilder strbu = new StringBuilder();
            try
            {
                //获取其他出库单数据
                sql = "select '形态转换单' cbustype,A1.cInvCode,SUM(A1.iAVQuantity) iQuantity,A1.cWhCode publicsec1,A2.cWhName publicsec2,GETDATE() as CreateTime from AssemVouch A left join AssemVouchs A1 on A.ID=A1.ID left join Warehouse A2 on A2.cWhCode=A.cWhCode WHERE Convert(nvarchar(7),A.dAVDate,121)='" + queryDate + "' group by A1.cInvCode,A1.cWhCode,A2.cWhName";

                //获取其他出库单数据
                DataSet Generalout = SqlHelperForU8.ExecuteDataset(conneU8ctionString, CommandType.Text, sql);

                //获取其他出库单数据
                sql = "select '形态转换单' cbustype,A2.code cInvCode,nvl(sum(A1.nassistnum),0) iQuantity,A5.code publicsec1,A5.name publicsec1,to_char(sysdate,'yyyy-mm-dd hh24:mi:ss')  as CreateTime FROM ic_transform_h A left join ic_transform_b A1 on A1.cspecialhid = A.cspecialhid and A1.dr != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc left join bd_stordoc A5 on A5.pk_stordoc = A1.cbodywarehouseid where A.PK_ORG = '0001A110000000001V70' AND substr(A.dbilldate,0,7)= '" + queryDate + "' and substr(A2.code,0,4) != '0915' and A5.code not in('1001A1100000000T5S5Z','1001A11000000003CYSY') group by A2.code,A5.code,A5.name";

                //获取其他出库单数据
                DataSet GeneraloutLine = OracleHelper.ExecuteDataset(sql);

                StringBuilder str = DataSetToArrayList.DataSetToArrayLists(Generalout, "Spl_U8DBData");
                SqlHelper.ExecuteNonQuery(str.ToString());

                StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(GeneraloutLine, "Spl_NCDBData");
                SqlHelper.ExecuteNonQuery(str.ToString());
            }
            catch (Exception e)
            {

                result = "形态转换单插入错误：" + e.Message;
            }
            return result;
        }

        /// <summary>
        /// NcU8数据比对
        /// 创建人：lvhe
        /// 创建时间：2020年3月6日 15:51:13
        /// </summary>
        /// <returns></returns>

        private string Ncu8Compare()
        {
            string msg = "";
            try
            {
                string delstrApps = "truncate table Spl_NCU8Compare";
                SqlHelperForApps.ExecuteNonQuerys(delstrApps);
                string sql = " select isnull(A.cbustype,A1.cbustype) cbustype,isnull(A.crdcode,A1.crdcode) cRdCode,isnull(A.cinvcode,A1.cinvcode) cInvCode,isnull(A.iquantity,0) iQuantity,isnull(A1.iquantity,0) nciQuantity,isnull(A.publicsec1,A1.publicsec1) publicsec1,isnull(A.publicsec2,A1.publicsec2) publicsec2,GETDATE() as CreateTime from [Spl_U8DBData] A full join [Spl_ncDBData] A1 on A.crdcode=A1.crdCode and A.Cinvcode=A1.Cinvcode and A.publicsec1=A1.publicsec1 and A.publicsec2=A1.publicsec2";
                //获取采购入库汇总数据
                DataSet ncu8compareDs = SqlHelper.ExecuteDataset(connectionString, CommandType.Text, sql);
                StringBuilder strt = DataSetToArrayList.DataSetToArrayLists(ncu8compareDs, "Spl_NCU8Compare");
                SqlHelperForApps.ExecuteNonQuery(strt.ToString());
            }
            catch (Exception e)
            {
                msg = "数据对比失败：" + e.Message;
            }
            return msg;
        }
    }
}
