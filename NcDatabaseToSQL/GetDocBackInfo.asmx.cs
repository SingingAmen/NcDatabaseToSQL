using ERP8.Common;
using NcDatabaseToSQLForApps;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Linq;
using System.Text;
using System.Web;
using System.Web.Services;

namespace NcDatabaseToSQL
{
    /// <summary>
    /// GetDocBackInfo 的摘要说明
    /// </summary>
    [WebService(Namespace = "http://tempuri.org/")]
    [WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
    [System.ComponentModel.ToolboxItem(false)]
    // 若要允许使用 ASP.NET AJAX 从脚本中调用此 Web 服务，请取消注释以下行。 
    // [System.Web.Script.Services.ScriptService]
    public class GetDocBackInfo : System.Web.Services.WebService
    {
        private static string connectionString = ConfigurationManager.ConnectionStrings["U8Conn"].ToString();

        private static string conneU8ctionString = ConfigurationManager.ConnectionStrings["U8DataConn"].ToString();

        private static string conneAppsDBctionString = ConfigurationManager.ConnectionStrings["AppsDBCoon"].ToString();

        private static string connNCSubjectEncoding = ConfigurationManager.ConnectionStrings["NCSubjectEncoding"].ToString();


        [WebMethod]
        public string GetDocBackMemoInfo()
        {
            string result = "";
            string createSql = "";
            string tableExist = "";
            int existResult = 0;
            string msg = "";
            string sql = "";
            StringBuilder strbu = new StringBuilder();
            DataSet sqlServerInvoices = new DataSet();
            try
            {
                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id( 'Spl_DocBackMemo') is not null select 1 else select 0";
                existResult = SqlHelperForApps.ExecuteNonQuerys(tableExist);
                //获取满足条件的单号
                sql = "select '形态转换' docType,code,memo,SUBSTRING(ddate,0,8) ddate,GETDATE() createtime from AssemVouch where zt=1 and datalength(memo)>0 union all select '销售出库' docType,code,memo,SUBSTRING(ddate,0,8) ddate,GETDATE() createtime from DispatchList where zt=1 and datalength(memo)>0 union all select '采购发票' docType,code,memo,SUBSTRING(ddate,0,8) ddate,GETDATE() createtime from PurBillVouch where zt=1 and datalength(memo)>0 union all select '采购入库' docType,code,memo,SUBSTRING(ddate,0,8) ddate,GETDATE() createtime from Rdrecord01 where zt=1 and datalength(memo)>0 union all select '其他入库' docType,code,memo,SUBSTRING(ddate,0,8) ddate,GETDATE() createtime from RdRecord08 where zt=1 and datalength(memo)>0 union all select '其他出库' docType,code,memo,SUBSTRING(ddate,0,8) ddate,GETDATE() createtime from RdRecord09 where zt=1 and datalength(memo)>0 union all select '产成品入库' docType,code,memo,SUBSTRING(ddate,0,8) ddate,GETDATE() createtime from RdRecord10 where zt=1 and datalength(memo)>0 union all select '材料出库' docType,code,memo,SUBSTRING(ddate,0,8) ddate,GETDATE() createtime from RdRecord11 where zt=1 and datalength(memo)>0 union all select '销售发票' docType,code,memo,SUBSTRING(ddate,0,8) ddate,GETDATE() createtime from SaleBillVouch where zt=1 and datalength(memo)>0 union all select '调拨单'  docType,code,memo,SUBSTRING(ddate,0,8) ddate,GETDATE() createtime from TransVouch where zt=1 and datalength(memo)>0";

                if (existResult == 1)
                {
                    string delstr = "delete from Spl_DocBackMemo";
                    SqlHelperForApps.ExecuteNonQuerys(delstr);

                }
                DataSet Invoices = SqlHelper.ExecuteDataset(connectionString, CommandType.Text, sql);

                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id( 'Spl_DocBackMemo') is not null select 1 else select 0";
                existResult = SqlHelperForApps.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table Spl_DocBackMemo(Id [bigint] IDENTITY(1,1) primary key not null,[docType] [nvarchar](50) NULL,[code] [nvarchar](50) NULL,[memo] [nvarchar](500) NULL,[ddate] [nvarchar](200) NULL,[def1] [nvarchar](200) NULL,[def2] [nvarchar](200) NULL,[def3] [nvarchar](200) NULL,[def4] [nvarchar](200) NULL,[def5] [nvarchar](200) NULL,[def6] [nvarchar](200) NULL,[def7] [nvarchar](200) NULL,[def8] [nvarchar](200) NULL,[def9] [nvarchar](200) NULL,[def10] [nvarchar](200) NULL,[CreateTime] [datetime] NOT NULL)";
                    SqlHelperForApps.ExecuteNonQuery(createSql);
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(Invoices, "Spl_DocBackMemo");
                    SqlHelperForApps.ExecuteNonQuery(str.ToString());
                    msg = "单据结果检查表插入成功";
                }
                else
                {
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(Invoices, "Spl_DocBackMemo");
                    if (!string.IsNullOrEmpty(str.ToString()))
                    {
                        SqlHelperForApps.ExecuteNonQuery(str.ToString());
                        msg = "单据结果检查表插入成功";
                    }
                }
                result = msg;
            }
            catch (Exception e)
            {

                result = "单据结果检查表插入错误：" + e.Message;
            }
            return result;
        }
    }
}
