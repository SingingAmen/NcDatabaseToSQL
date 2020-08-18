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
                sql = CheckSql(datetime);

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


        /// <summary>
        /// 拼接检查脚本
        /// 创建人：lvhe
        /// 创建时间:2020-8-14 15:25:05
        /// </summary>
        /// <param name="datetime"></param>
        /// <returns></returns>

        private string CheckSql(string datetime)
        {
            string u8depsql = "";
            //string U8Dept = "";
            string sql = "";
            DataSet U8DeptList = new DataSet();
            //行政部门领用（名鸿）	物料编码开头07 / 08 / 51 / 52 / 54        行政 部门、物料编码、出入库类型须匹配
            // u8depsql = " select cDepCode from Department where cDepProp = '行政'";
            //获取采购入库头数据
            //U8DeptList = SqlHelperForU8.ExecuteDataset(conneU8ctionString, CommandType.Text, u8depsql);
            //U8Dept = GetU8Depts(U8DeptList);
            sql = "select distinct docno,DocName,ItemCode,CreateTime,def1 from (";
            sql += "select distinct A.vbillcode DocNO,'材料出库' DocName,A4.code ItemCode,'行政部门领用（名鸿）物料编码开头不为07/08/51/52/54 行政' def1,to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') as CreateTime " +
            "from ic_material_h A " +
            "left join ic_material_b A1 on A.cgeneralhid = A1.cgeneralhid " +
            "left join bd_billtype A2 on A2.pk_billtypeid = A.ctrantypeid " +
            "left join org_dept_v A3 on A3.pk_vid = A.cdptvid " +
            "left join bd_material A4 on A1.cmaterialvid = A4.pk_material " +
            "left join bd_defdoc A5 on A5.pk_defdoc = A3.deptlevel " +
            "where A2.billtypename = '行政部门领用（名鸿）'and A5.name = '行政' " +
            "and substr(A4.code,1,2) not in('07', '08', '51', '52', '54') and substr(A.dbilldate,0,7) = '" + datetime + "'";

            sql += " union all ";

            //生产辅助领用（名鸿）	物料编码开头不为01/03/10/11/12/13/14		非行政	部门、物料编码、出入库类型须匹配
            //u8depsql = " select cDepCode from Department where cDepProp != '行政'";
            ////获取采购入库头数据
            //U8DeptList = SqlHelperForU8.ExecuteDataset(conneU8ctionString, CommandType.Text, u8depsql);
            //U8Dept = GetU8Depts(U8DeptList);

            sql += "select distinct A.vbillcode DocNO,'材料出库' DocName,A4.code ItemCode,'生产辅助领用（名鸿）物料编码开头为01/03/10/11/12/13/14 非行政' def1,to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') as CreateTime " +
            "from ic_material_h A " +
            "left join ic_material_b A1 on A.cgeneralhid = A1.cgeneralhid " +
            "left join bd_billtype A2 on A2.pk_billtypeid = A.ctrantypeid " +
            "left join org_dept_v A3 on A3.pk_vid = A.cdptvid " +
            "left join bd_material A4 on A1.cmaterialvid = A4.pk_material " +
            "left join bd_defdoc A5 on A5.pk_defdoc = A3.deptlevel " +
            "where A2.billtypename = '生产辅助领用（名鸿）' and A5.name != '行政' " +
            "and substr(A4.code,1,2) in('01', '03', '10', '11', '12', '13', '14') and substr(A.dbilldate,0,7) = '" + datetime + "'";


            sql += " union all ";

            //u8depsql = " select cDepCode from Department where cDepProp = '塑件生产'";
            ////获取采购入库头数据
            //U8DeptList = SqlHelperForU8.ExecuteDataset(conneU8ctionString, CommandType.Text, u8depsql);
            //U8Dept = GetU8Depts(U8DeptList);
            sql += "select distinct A.vbillcode DocNO,'材料出库' DocName,A4.code ItemCode,'生产辅助领用（名鸿）物料编码开头不为01/07 塑件生产' def1,to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') as CreateTime " +
            "from ic_material_h A " +
            "left join ic_material_b A1 on A.cgeneralhid = A1.cgeneralhid " +
            "left join bd_billtype A2 on A2.pk_billtypeid = A.ctrantypeid " +
            "left join org_dept_v A3 on A3.pk_vid = A.cdptvid " +
            "left join bd_material A4 on A1.cmaterialvid = A4.pk_material " +
            "left join bd_defdoc A5 on A5.pk_defdoc = A3.deptlevel " +
            "where A2.billtypename = '生产辅助领用（名鸿）' and A5.name = '塑件生产' " +
            "and substr(A4.code,1,2) not in('01', '07') and substr(A.dbilldate,0,7) = '" + datetime + "'";

            sql += " union all ";
            //u8depsql = " select cDepCode from Department where cDepProp = '塑磨件生产'";
            ////获取采购入库头数据
            //U8DeptList = SqlHelperForU8.ExecuteDataset(conneU8ctionString, CommandType.Text, u8depsql);
            //U8Dept = GetU8Depts(U8DeptList);

            sql += "select distinct A.vbillcode DocNO,'材料出库' DocName,A4.code ItemCode,'生产辅助领用（名鸿）物料编码开头不为07/10 塑磨件生产' def1,to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') as CreateTime " +
            "from ic_material_h A " +
            "left join ic_material_b A1 on A.cgeneralhid = A1.cgeneralhid " +
            "left join bd_billtype A2 on A2.pk_billtypeid = A.ctrantypeid " +
            "left join org_dept_v A3 on A3.pk_vid = A.cdptvid " +
            "left join bd_material A4 on A1.cmaterialvid = A4.pk_material " +
            "left join bd_defdoc A5 on A5.pk_defdoc = A3.deptlevel " +
            "where A2.billtypename = '生产辅助领用（名鸿）' and A5.name = '塑磨件生产' " +
            "and substr(A4.code,1,2) not in('07', '10') and substr(A.dbilldate,0,7) = '" + datetime + "'";

            sql += " union all ";
            //u8depsql = " select cDepCode from Department where cDepProp = '喷涂生产'";
            ////获取采购入库头数据
            //U8DeptList = SqlHelperForU8.ExecuteDataset(conneU8ctionString, CommandType.Text, u8depsql);
            //U8Dept = GetU8Depts(U8DeptList);

            sql += "select distinct A.vbillcode DocNO,'材料出库' DocName,A4.code ItemCode,'生产辅助领用（名鸿）物料编码开头不为03/07/10/11 喷涂生产' def1,to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') as CreateTime " +
            "from ic_material_h A " +
            "left join ic_material_b A1 on A.cgeneralhid = A1.cgeneralhid " +
            "left join bd_billtype A2 on A2.pk_billtypeid = A.ctrantypeid " +
            "left join org_dept_v A3 on A3.pk_vid = A.cdptvid " +
            "left join bd_material A4 on A1.cmaterialvid = A4.pk_material " +
            "left join bd_defdoc A5 on A5.pk_defdoc = A3.deptlevel " +
            "where A2.billtypename = '生产辅助领用（名鸿）' and A5.name = '喷涂生产' " +
            "and substr(A4.code,1,2) not in('03', '07', '10', '11') and substr(A.dbilldate,0,7) = '" + datetime + "'";

            sql += " union all ";

            sql += "select distinct A.vbillcode DocNO,'材料出库' DocName,A4.code ItemCode,'生产辅助领用（名鸿）物料编码开头不为07/10/11/12/13 装配生产' def1,to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') as CreateTime " +
            "from ic_material_h A " +
            "left join ic_material_b A1 on A.cgeneralhid = A1.cgeneralhid " +
            "left join bd_billtype A2 on A2.pk_billtypeid = A.ctrantypeid " +
            "left join org_dept_v A3 on A3.pk_vid = A.cdptvid " +
            "left join bd_material A4 on A1.cmaterialvid = A4.pk_material " +
            "left join bd_defdoc A5 on A5.pk_defdoc = A3.deptlevel " +
            "where A2.billtypename = '生产辅助领用（名鸿）' " +
            "and A5.name = '装配生产' " +
            "and substr(A4.code,1,2) not in('07', '10', '11', '12', '13') and substr(A.dbilldate,0,7) = '" + datetime + "'";


            sql += " union all ";
            sql += "select distinct A.vbillcode DocNO,'产成品入库' DocName,A4.code ItemCode,'生产产成品入库（名鸿）物料编码开头不为10 塑件生产' def1,to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') as CreateTime " +
            "from ic_finprodin_h A " +
            "left join ic_finprodin_b A1 on A1.cgeneralhid = A.cgeneralhid " +
            "left join bd_billtype A2 on A2.pk_billtypeid = A.ctrantypeid " +
            "left join org_dept_v A3 on A3.pk_vid = A.cdptvid " +
            "left join bd_material A4 on A1.cmaterialvid = A4.pk_material " +
            "left join bd_defdoc A5 on A5.pk_defdoc = A3.deptlevel " +
            "where A2.billtypename = '生产产成品入库（名鸿）' and A5.name = '塑件生产' " +
            "and substr(A4.code,1,2) not in('10') and substr(A.dbilldate,0,7) = '" + datetime + "'";

            sql += " union all ";
            sql += "select distinct A.vbillcode DocNO,'产成品入库' DocName,A4.code ItemCode,'生产产成品入库（名鸿）物料编码开头不为11 塑磨件生产' def1,to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') as CreateTime " +
            "from ic_finprodin_h A " +
            "left join ic_finprodin_b A1 on A1.cgeneralhid = A.cgeneralhid " +
            "left join bd_billtype A2 on A2.pk_billtypeid = A.ctrantypeid " +
            "left join org_dept_v A3 on A3.pk_vid = A.cdptvid " +
            "left join bd_material A4 on A1.cmaterialvid = A4.pk_material " +
            "left join bd_defdoc A5 on A5.pk_defdoc = A3.deptlevel " +
            "where A2.billtypename = '生产产成品入库（名鸿）' and A5.name = '塑磨件生产' " +
            "and substr(A4.code,1,2) not in('11') and substr(A.dbilldate,0,7) = '" + datetime + "'";

            sql += " union all ";
            sql += "select distinct A.vbillcode DocNO,'产成品入库' DocName,A4.code ItemCode,'生产产成品入库（名鸿）物料编码开头不为13 喷涂生产' def1,to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') as CreateTime " +
            "from ic_finprodin_h A " +
            "left join ic_finprodin_b A1 on A1.cgeneralhid = A.cgeneralhid " +
            "left join bd_billtype A2 on A2.pk_billtypeid = A.ctrantypeid " +
            "left join org_dept_v A3 on A3.pk_vid = A.cdptvid " +
            "left join bd_material A4 on A1.cmaterialvid = A4.pk_material " +
            "left join bd_defdoc A5 on A5.pk_defdoc = A3.deptlevel " +
            "where A2.billtypename = '生产产成品入库（名鸿）'and A5.name = '喷涂生产' " +
            "and substr(A4.code,1,2) not in('0915') and substr(A.dbilldate,0,7) = '" + datetime + "'";

            sql += " union all ";
            sql += "select distinct A.vbillcode DocNO,'产成品入库' DocName,A4.code ItemCode,'生产产成品入库（名鸿）物料编码开头不为14 装配生产' def1,to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') as CreateTime " +
            "from ic_finprodin_h A " +
            "left join ic_finprodin_b A1 on A1.cgeneralhid = A.cgeneralhid " +
            "left join bd_billtype A2 on A2.pk_billtypeid = A.ctrantypeid " +
            "left join org_dept_v A3 on A3.pk_vid = A.cdptvid " +
            "left join bd_material A4 on A1.cmaterialvid = A4.pk_material " +
            "left join bd_defdoc A5 on A5.pk_defdoc = A3.deptlevel " +
            "where A2.billtypename = '生产产成品入库（名鸿）' and A5.name = '装配生产' " +
            "and substr(A4.code,1,2) not in('14') and substr(A.dbilldate,0,7) = '" + datetime + "'";

            sql += " union all ";
            sql += "select distinct A.vbillcode DocNO,'材料出库' DocName,A4.code ItemCode,'生产直接领用（名鸿）委外成本中心(部门档案备注为委外）' def1,to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') as CreateTime " +
            "from ic_material_h A " +
            "left join ic_material_b A1 on A.cgeneralhid = A1.cgeneralhid " +
            "left join bd_billtype A2 on A2.pk_billtypeid = A.ctrantypeid " +
            "left join org_dept_v A3 on A3.pk_vid = A.cdptvid " +
            "left join bd_material A4 on A1.cmaterialvid = A4.pk_material " +
            "where A2.billtypename = '生产直接领用（名鸿）' and A3.memo = '委外' " +
            "and substr(A.dbilldate,0,7) = '" + datetime + "'";

            sql += " union all ";
            sql += "select distinct A.vbillcode DocNO,'产成品入库' DocName,A4.code ItemCode,'生产产成品入库（名鸿）委外成本中心(部门档案备注为委外）' def1,to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') as CreateTime " +
            "from ic_finprodin_h A " +
            "left join ic_finprodin_b A1 on A.cgeneralhid = A1.cgeneralhid " +
            "left join bd_billtype A2 on A2.pk_billtypeid = A.ctrantypeid " +
            "left join org_dept_v A3 on A3.pk_vid = A.cdptvid " +
            "left join bd_material A4 on A1.cmaterialvid = A4.pk_material " +
            "where A2.billtypename = '生产产成品入库（名鸿）' and A3.memo = '委外' " +
            "and substr(A.dbilldate,0,7) = '" + datetime + "'";

            sql += " union all ";
            sql += "select distinct A.vbillcode DocNO,'其它入库' DocName,A4.code ItemCode,'包装材料入库 物料编码开头不为0915开头的编码 物料编码与出入库类型匹配' def1,to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') as CreateTime " +
            "from ic_generalin_h A " +
            "left join ic_generalin_b A1 on A1.cgeneralhid = A.cgeneralhid " +
            "left join bd_billtype A2 on A2.pk_billtypeid = A.ctrantypeid " +
            "left join org_dept_v A3 on A3.pk_vid = A.cdptvid " +
            "left join bd_material A4 on A1.cmaterialvid = A4.pk_material " +
            "left join bd_billtype A5 ON A5.pk_billtypecode = A.vtrantypecode " +
            "where A2.billtypename = '包装材料入库' " +
            "and substr(A4.code,1,4) not in('0915') and substr(A.dbilldate,0,7) = '" + datetime + "'";

            sql += " union all ";
            sql += "select distinct A.vbillcode DocNO,'其它出库' DocName,A4.code ItemCode,'包装材料出库 物料编码开头不为0915开头的编码 物料编码与出入库类型匹配' def1,to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') as CreateTime " +
            "from ic_generalout_h A " +
            "left join ic_generalout_b A1 on A1.cgeneralhid = A.cgeneralhid " +
            "left join bd_billtype A2 on A2.pk_billtypeid = A.ctrantypeid " +
            "left join org_dept_v A3 on A3.pk_vid = A.cdptvid " +
            "left join bd_material A4 on A1.cmaterialvid = A4.pk_material " +
            "left join bd_billtype A5 ON A5.pk_billtypecode = A.vtrantypecode " +
            "where A2.billtypename = '包装材料出库' " +
            "and substr(A4.code,1,4) not in('0915') and substr(A.dbilldate,0,7) = '" + datetime + "'";


            sql += " union all ";
            sql += "select distinct A.vbillcode DocNO,'其它入库' DocName,A4.code ItemCode,'项目入库 部门编码为0207 部门编码0207、备注与出入库类型不匹配' def1,to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') as CreateTime " +
            "from ic_generalin_h A " +
            "left join ic_generalin_b A1 on A1.cgeneralhid = A.cgeneralhid " +
            "left join bd_billtype A2 on A2.pk_billtypeid = A.ctrantypeid " +
            "left join org_dept_v A3 on A3.pk_vid = A.cdptvid " +
            "left join bd_material A4 on A1.cmaterialvid = A4.pk_material " +
            "left join bd_billtype A5 ON A5.pk_billtypecode = A.vtrantypecode " +
            "where A2.billtypename = '项目入库' and A3.Code = '0207' AND A.vnote != A5.pk_billtypecode " +
            "and substr(A.dbilldate,0,7) = '" + datetime + "'";

            sql += " union all ";
            sql += "select distinct A.vbillcode DocNO,'其它出库' DocName,A4.code ItemCode,'项目领用 物料编码开头不为0915开头的编码 物料编码与出入库类型不匹配' def1,to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') as CreateTime " +
            "from ic_generalout_h A " +
            "left join ic_generalout_b A1 on A1.cgeneralhid = A.cgeneralhid " +
            "left join bd_billtype A2 on A2.pk_billtypeid = A.ctrantypeid " +
            "left join org_dept_v A3 on A3.pk_vid = A.cdptvid " +
            "left join bd_material A4 on A1.cmaterialvid = A4.pk_material " +
            "left join bd_billtype A5 ON A5.pk_billtypecode = A.vtrantypecode " +
            "where A2.billtypename = '项目领用'  and A3.Code = '0207' AND A.vnote != A5.pk_billtypecode " +
            "and substr(A.dbilldate,0,7) = '" + datetime + "'";


            sql += " union all ";
            sql += "select distinct A.vbillcode DocNO,'其它出库' DocName,A4.code ItemCode,'研发领用 研发部-质量部、备注为空 部门编码0204、备注与出入库类型不匹配' def1,to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') as CreateTime " +
             "from ic_generalout_h A " +
             "left join ic_generalout_b A1 on A1.cgeneralhid = A.cgeneralhid  " +
             "left join bd_billtype A2 on A2.pk_billtypeid = A.ctrantypeid " +
             "left join org_dept_v A3 on A3.pk_vid = A.cdptvid " +
             "left join bd_material A4 on A1.cmaterialvid = A4.pk_material " +
             "left join bd_billtype A5 ON A5.pk_billtypecode = A.vtrantypecode " +
             "left join bd_defdoc A6 on A6.pk_defdoc = A3.deptlevel " +
             "where A2.billtypename = '研发领用'  and A3.Code = '0204' AND A.vnote != A5.pk_billtypecode " +
             "and A6.name = '研发部-质量部' and substr(A.dbilldate,0,7) = '" + datetime + "'";

            sql += " union all ";
            sql += "select distinct A.vbillcode DocNO,'其它入库' DocName,A4.code ItemCode,'仓库不为PPG仓库、受托加工仓 仓库与单据类型匹配' def1,to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') as CreateTime "+
            "from ic_generalin_h A " +
            "left join ic_generalin_b A1 on A1.cgeneralhid = A.cgeneralhid " +
            "left join bd_billtype A2 on A2.pk_billtypeid = A.ctrantypeid " +
            "left join org_dept_v A3 on A3.pk_vid = A.cdptvid " +
            "left join bd_material A4 on A1.cmaterialvid = A4.pk_material " +
            "left join bd_stordoc A5 on A5.pk_stordoc = A.cwarehouseid " +
            "where A.cwarehouseid = A.ctrantypeid and A5.name not in('PPG仓库', '受托加工仓') and substr(A.dbilldate,0,7) = '" + datetime + "'";

            sql += " union all ";
            sql += "select distinct A.vbillcode DocNO,'其它出库' DocName,A4.code ItemCode,'仓库不为PPG仓库、受托加工仓 仓库与单据类型匹配' def1,to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') as CreateTime "+
            "from ic_generalin_h A " +
            "left join ic_generalout_b A1 on A1.cgeneralhid = A.cgeneralhid " +
            "left join bd_billtype A2 on A2.pk_billtypeid = A.ctrantypeid " +
            "left join org_dept_v A3 on A3.pk_vid = A.cdptvid " +
            "left join bd_material A4 on A1.cmaterialvid = A4.pk_material " +
            "left join bd_stordoc A5 on A5.pk_stordoc = A.cwarehouseid " +
            "where A.cwarehouseid = A.ctrantypeid and A5.name not in('PPG仓库', '受托加工仓') and substr(A.dbilldate,0,7) = '" + datetime + "'";

            sql += ")";

            return sql;
        }


        /// <summary>
        /// 拼接部门code
        /// 创建人：lvhe
        /// 创建时间：2020-8-14 15:49:23
        /// </summary>
        /// <param name="dt"></param>
        /// <returns></returns>
        private string GetU8Depts(DataSet dt)
        {
            string U8Dept = "";
            foreach (DataRow row in dt.Tables[0].Rows)
            {
                U8Dept += "'" + row[0].ToString() + "'";
                U8Dept += ",";
            }
            U8Dept = U8Dept.Substring(0, U8Dept.Length - 1); //去掉最后一个“，”
            return U8Dept;
        }
    }
}
