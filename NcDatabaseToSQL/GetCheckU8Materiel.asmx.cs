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
    /// GetCheckU8Materiel 的摘要说明
    /// </summary>
    [WebService(Namespace = "http://tempuri.org/")]
    [WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
    [System.ComponentModel.ToolboxItem(false)]
    // 若要允许使用 ASP.NET AJAX 从脚本中调用此 Web 服务，请取消注释以下行。 
    // [System.Web.Script.Services.ScriptService]
    public class GetCheckU8Materiel : System.Web.Services.WebService
    {
        private static string connectionString = ConfigurationManager.ConnectionStrings["U8Conn"].ToString();

        private static string conneU8ctionString = ConfigurationManager.ConnectionStrings["U8DataConn"].ToString();

        private static string conneAppsDBctionString = ConfigurationManager.ConnectionStrings["AppsDBCoon"].ToString();

        private static string connNCSubjectEncoding = ConfigurationManager.ConnectionStrings["NCSubjectEncoding"].ToString();


        [WebMethod]
        public string GetDocNOFromNc(string datetime)
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
                tableExist = "if object_id( 'Spl_CheckNCItemCode') is not null select 1 else select 0";
                existResult = SqlHelperForApps.ExecuteNonQuerys(tableExist);
                //获取满足条件的单号
                sql = "select distinct A.vbillcode DocNO,'材料出库' DocName,A4.code ItemCode,to_char(sysdate,'yyyy-mm-dd hh24:mi:ss') as CreateTime from ic_material_h A left join ic_material_b A1 on A.cgeneralhid = A1.cgeneralhid left join bd_billtype A2 on A2.pk_billtypeid=A.ctrantypeid left join org_dept_v A3 on A3.pk_vid=A.cdptvid left join bd_material A4 on A1.cmaterialvid = A4.pk_material where A2.billtypename='生产辅助领用（名鸿）' and A3.name not like '%行政%' and substr(A4.code,1,2) not in('01','03','10','11','12','13','14')";

                if (existResult == 1)
                {
                    string delstr = "delete from Spl_CheckNCItemCode";
                    SqlHelperForApps.ExecuteNonQuerys(delstr);

                }
                DataSet Invoices = OracleHelper.ExecuteDataset(sql);

                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id( 'Spl_CheckNCItemCode') is not null select 1 else select 0";
                existResult = SqlHelperForApps.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table Spl_CheckNCItemCode(Id [bigint] IDENTITY(1,1) primary key not null,[DocNO] [nvarchar](50) NULL,[DocName] [nvarchar](200) NULL,[ItemCode] [nvarchar](200) NULL,[ItemName] [nvarchar](200) NULL,[def1] [nvarchar](200) NULL,[def2] [nvarchar](200) NULL,[def3] [nvarchar](200) NULL,[def4] [nvarchar](200) NULL,[def5] [nvarchar](200) NULL,[def6] [nvarchar](200) NULL,[def7] [nvarchar](200) NULL,[def8] [nvarchar](200) NULL,[def9] [nvarchar](200) NULL,[def10] [nvarchar](200) NULL,[CreateTime] [datetime] NOT NULL)";
                    SqlHelperForApps.ExecuteNonQuery(createSql);
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(Invoices, "Spl_CheckNCItemCode");
                    SqlHelperForApps.ExecuteNonQuery(str.ToString());
                    msg = "单据检查表插入成功";
                }
                else
                {
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(Invoices, "Spl_CheckNCItemCode");
                    if (!string.IsNullOrEmpty(str.ToString()))
                    {
                        SqlHelperForApps.ExecuteNonQuery(str.ToString());
                        msg = "单据检查表插入成功";
                    }
                }
                result = msg;
            }
            catch (Exception e)
            {

                result = "单据检查表插入错误：" + e.Message;
            }
            return result;
        }
    }
}
