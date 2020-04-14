using ERP8.Common;
using System;
using System.Collections;
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
    /// NcDatabaseToSQL 的摘要说明
    /// </summary>
    [WebService(Namespace = "http://tempuri.org/")]
    [WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
    [System.ComponentModel.ToolboxItem(false)]
    // 若要允许使用 ASP.NET AJAX 从脚本中调用此 Web 服务，请取消注释以下行。 
    // [System.Web.Script.Services.ScriptService]
    public class NcDatabaseToSQL : System.Web.Services.WebService
    {
        //开始时间
        string startTime = DateTime.Now.AddDays(1 - DateTime.Now.Day).AddMonths(-1).ToString("yyyy-MM-dd");
        //结束时间
        string endTime = DateTime.Now.AddDays(1 - DateTime.Now.Day).AddDays(-1).ToString("yyyy-MM-dd");

        private static string connectionString = ConfigurationManager.ConnectionStrings["U8Conn"].ToString();
        //string startTime = "2019-09-01";
        //string endTime = "2019-09-30";
        //[WebMethod]
        //public string HelloWorld()
        //{
        //    return "Hello World";
        //}

        /// <summary>
        /// 公共调用方法
        /// 创建人：吕贺
        /// 创建时间：2019年10月23日 13:55:45
        /// </summary>
        /// <returns></returns>
        [WebMethod]
        public string NcInsertToSql(string interfaceParameters)
        {
            string msg = "";
            switch (interfaceParameters)
            {
                case "cgfp":
                    msg = GetPurchaseInvoicesToSql();
                    break;
                case "xsfp":
                    msg = GetSoSaleinvoiceToSql();
                    break;
                //采购入库
                case "cgrk":
                    msg = GetPurchaseinToSql();
                    break;
                case "xsck":
                    msg = GetSaleOutToSql();
                    break;
                case "clck":
                    msg = GetMaterialToSql();
                    break;
                case "qtrk":
                    msg = GetIAi4billToSql();
                    break;
                case "qtck":
                    msg = GetIAi7billToSql();
                    break;
                //产成品入库    
                case "ccprk":
                    msg = GetFinprodInToSql();
                    break;
                case "dbd":
                    msg = GetIcWhstransHToSql();
                    break;
                case "xtzh":
                    msg = GetIcTransformHToSql();
                    break;
                default:
                    msg = "请检查参数是否正确,没有找到输入参数对应的接口信息！";
                    break;
            }

            //switch (interfaceParameters)
            //{
            //    case "cgfp":
            //        msg = "cgfp";
            //        break;
            //    case "xsfp":
            //        msg = "xsfp";
            //        break;
            //    case "cgrk":
            //        msg = "cgrk";
            //        break;
            //    case "xsck":
            //        msg = "xsck";
            //        break;
            //    default:
            //        msg = "请检查参数是否正确,没有找到输入参数对应的接口信息！";
            //        break;
            //}

            return msg;
        }

        /// <summary>
        /// 从nc获取采购发票数据插入到sql
        /// 创建人：lvhe
        /// 创建时间：2019年10月13日 22:59:30
        /// </summary>
        /// <returns></returns>
        [WebMethod]
        private string GetPurchaseInvoicesToSql()
        {
            string result = "";
            string createSql = "";
            string tableExist = "";
            int existResult = 0;
            string msg = "";
            string sql = "";
            StringBuilder strbu = new StringBuilder();
            string strGetOracleSQLIn = "";
            DataSet sqlServerInvoices = new DataSet();
            try
            {
                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id( 'PurBillVouch') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    //获取采购发票头数据
                    sql = " select A.PK_INVOICE ID,A.vbillcode code,A.DBILLDATE ddate,A.darrivedate darrivedate,A1.code cvencode,A2.CODE cdepcode,CASE WHEN A.ntotalastnum>0 THEN 0 ELSE 1 END AS isRed,A.VMEMO remark,A.modifiedtime ts from po_invoice A left join org_dept A2 on A2.PK_DEPT=A.PK_DEPT  and nvl(A2.dr,0)=0 left join bd_supplier A1 on A.PK_SUPPLIER=A1.PK_SUPPLIER and nvl(A1.dr,0)=0 where  not exists (select pk_invoice from (select distinct pob.pk_invoice pk_invoice from po_invoice_b pob left join bd_material mat on mat.pk_material = pob.Pk_material and nvl(mat.dr,0)=0 left join po_invoice poh on poh.PK_INVOICE = pob.PK_INVOICE and nvl(poh.dr,0)=0 where  nvl(pob.dr,0)=0 and substr(mat.code,0,4) = '0915' and poh.PK_ORG='0001A110000000001V70'  AND substr(poh.taudittime,0,10) between '" + startTime + "' and '" + endTime + "' and poh.fbillstatus=3) po  where po.pk_invoice = A.pk_invoice) and  A.PK_ORG='0001A110000000001V70'  AND substr(A.taudittime,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillstatus=3";
                }
                else
                {
                    string delstr = "delete from PurBillVouch where id in(select ID from PurBillVouch where zt != 1 and ts is not null and ts !='')";
                    string delstr2 = "delete from PurBillVouchs where id in(select ID from PurBillVouch where zt != 1 and ts is not null and ts !='')";
                    SqlHelper.ExecuteNonQuerys(delstr);
                    SqlHelper.ExecuteNonQuerys(delstr2);
                    string str = "select id from PurBillVouch";
                    DataSet ds = SqlHelper.ExecuteDataset(connectionString, CommandType.Text, str);
                    foreach (DataRow dr in ds.Tables[0].Rows)
                    {
                        strbu.Append(dr["id"].ToString() + ",");
                    }
                    strbu = strbu.Remove(strbu.Length - 1, 1);
                    String[] ids = strbu.ToString().Split(',');
                    strGetOracleSQLIn = getOracleSQLIn(ids, "A.PK_INVOICE");
                    //获取采购发票头数据
                    sql = " select A.PK_INVOICE ID,A.vbillcode code,A.DBILLDATE ddate,A.darrivedate darrivedate,A1.code cvencode,A2.CODE cdepcode,CASE WHEN A.ntotalastnum>0 THEN 0 ELSE 1 END AS isRed,A.VMEMO remark,A.modifiedtime ts from po_invoice A left join org_dept A2 on A2.PK_DEPT=A.PK_DEPT  and nvl(A2.dr,0)=0 left join bd_supplier A1 on A.PK_SUPPLIER=A1.PK_SUPPLIER and nvl(A1.dr,0)=0 where  not exists (select pk_invoice from (select distinct pob.pk_invoice pk_invoice from po_invoice_b pob left join bd_material mat on mat.pk_material = pob.Pk_material and nvl(mat.dr,0)=0 left join po_invoice poh on poh.PK_INVOICE = pob.PK_INVOICE and nvl(poh.dr,0)=0 where  nvl(pob.dr,0)=0 and substr(mat.code,0,4) = '0915' and poh.PK_ORG='0001A110000000001V70'  AND substr(poh.taudittime,0,10) between '" + startTime + "' and '" + endTime + "' and poh.fbillstatus=3) po  where po.pk_invoice = A.pk_invoice) and  A.PK_ORG='0001A110000000001V70'  AND substr(A.taudittime,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillstatus=3 and " + strGetOracleSQLIn + "";

                }
                DataSet Invoices = OracleHelper.ExecuteDataset(sql);

                //获取采购发票行数据
                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id( 'PurBillVouchs') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    sql = "select A.PK_INVOICE ID,A1.pk_invoice_b autoid,A1.vsourcecode srcdocno,A1.crowno doclineno,A1.vsourcerowno srcdoclineno,A2.code cinvcode,NVL(A1.nastnum ,0) qty,A1.ntaxrate itaxrate, A1.nastorigtaxprice iOriTaxCost, A1.nastorigprice iOriCost, A1.norigtaxmny ioriSum,A1.nnosubtax iOriTaxPrice, A1.norigmny iOriMoney, A1.ntaxmny isum, A1.nmny iMoney, A1.ntax iTaxPrice,A1.nastorigtaxprice iUnitCost, A1.vmemob remark from po_invoice A left join po_invoice_b A1 on A.PK_INVOICE = A1.PK_INVOICE and A1.DR!=1  left join bd_material A2 on A1.pk_material = A2.pk_material where A.PK_ORG = '0001A110000000001V70' and A.DR != 1 AND substr(A.taudittime,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillstatus = 3 and substr(A2.code,0,4) != '0915'";

                }
                else
                {
                    sql = "select A.PK_INVOICE ID,A1.pk_invoice_b autoid,A1.vsourcecode srcdocno,A1.crowno doclineno,A1.vsourcerowno srcdoclineno,A2.code cinvcode,NVL(A1.nastnum ,0) qty,A1.ntaxrate itaxrate, A1.nastorigtaxprice iOriTaxCost, A1.nastorigprice iOriCost, A1.norigtaxmny ioriSum,A1.nnosubtax iOriTaxPrice, A1.norigmny iOriMoney, A1.ntaxmny isum, A1.nmny iMoney, A1.ntax iTaxPrice,A1.nastorigtaxprice iUnitCost, A1.vmemob remark from po_invoice A left join po_invoice_b A1 on A.PK_INVOICE = A1.PK_INVOICE and A1.DR!=1  left join bd_material A2 on A1.pk_material = A2.pk_material where A.PK_ORG = '0001A110000000001V70' and A.DR != 1 AND substr(A.taudittime,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillstatus = 3 and substr(A2.code,0,4) != '0915' and " + strGetOracleSQLIn + "";
                }
                DataSet InvoiceLine = OracleHelper.ExecuteDataset(sql);

                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id( 'PurBillVouch') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table PurBillVouch(ID nvarchar(30) primary key not null,code nvarchar(50),ddate nvarchar(20),darrivedate nvarchar(20),cvencode nvarchar(50),cdepcode nvarchar(50),isRed bit,remark nvarchar(100),ts nvarchar(50),zt bit default 0,memo text)";
                    SqlHelper.ExecuteNonQuery(createSql);
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(Invoices, "PurBillVouch");
                    SqlHelper.ExecuteNonQuery(str.ToString());
                    msg = "采购发票表插入成功";
                }
                else
                {
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(Invoices, "PurBillVouch");
                    SqlHelper.ExecuteNonQuery(str.ToString());
                    msg = "采购发票表更新成功";
                }
                tableExist = "if object_id( 'PurBillVouchs') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table PurBillVouchs(ID nvarchar(30),autoid nvarchar(30) primary key not null,srcdocno nvarchar(50),doclineno nvarchar(30),srcdoclineno nvarchar(50),cinvcode nvarchar(50),qty decimal(28, 8),itaxrate decimal(28, 8),iOriTaxCost decimal(28, 8),iOriCost decimal(28, 8),ioriSum decimal(28, 8),iOriTaxPrice decimal(28, 8),iOriMoney decimal(28, 8),isum decimal(28, 8),iMoney decimal(28, 8),iTaxPrice decimal(28, 8),iUnitCost decimal(28, 8),remark nvarchar(100))";
                    SqlHelper.ExecuteNonQuery(createSql);
                    StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(InvoiceLine, "PurBillVouchs");
                    SqlHelper.ExecuteNonQuery(strs.ToString());
                    msg = "采购发票表行插入成功";
                }
                else
                {
                    StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(InvoiceLine, "PurBillVouchs");
                    SqlHelper.ExecuteNonQuery(strs.ToString());
                    msg = "采购发票表行更新成功";
                }
                result = msg;
            }
            catch (Exception e)
            {

                result = "采购发票表行错误：" + e.Message;
            }
            return result;

        }


        /// <summary>
        /// 从nc获取销售发票数据插入到sql
        /// 创建人：lvhe
        /// 创建时间：2019-10-18 13:39:06
        /// </summary>
        /// <returns></returns>
        [WebMethod]
        private string GetSoSaleinvoiceToSql()
        {
            string result = "";
            string createSql = "";
            string tableExist = "";
            int existResult = 0;
            string msg = "";
            string sql = "";
            StringBuilder strbu = new StringBuilder();
            string strGetOracleSQLIn = "";
            try
            {
                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id( 'SaleBillVouch') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    //获取销售发票头数据
                    sql = " select A.csaleinvoiceid ID,A.vbillcode code,A.dbilldate ddate,A1.Code custcode,A.vnote remark,CASE WHEN A.ntotalastnum>0 THEN 0 ELSE 1 END AS isRed,A.modifiedtime ts from so_saleinvoice A left join bd_customer A1 on A.cinvoicecustid = A1.pk_customer where  not exists (select csaleinvoiceid from (select distinct pob.csaleinvoiceid csaleinvoiceid from so_saleinvoice_b pob left join bd_material mat on mat.pk_material = pob.cmaterialvid and nvl(mat.dr,0)=0 left join so_saleinvoice poh on poh.csaleinvoiceid = pob.csaleinvoiceid and nvl(poh.dr,0)=0 where  nvl(pob.dr,0)=0  and substr(mat.code,0,4) = '0915' and poh.PK_ORG='0001A110000000001V70'  and poh.dr != 1 AND substr(poh.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and pob.csendstordocid not in('1001A1100000000T5S5Z','1001A11000000003CYSY')) po  where po.csaleinvoiceid = A.csaleinvoiceid) and  A.PK_ORG='0001A110000000001V70' and A.dr != 1  AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "'";
                }
                else
                {
                    string delstr = "delete from SaleBillVouch where id in(select ID from SaleBillVouch where zt != 1 and ts is not null and ts !='')";
                    string delstr2 = "delete from SaleBillVouchs where id in(select ID from SaleBillVouch where zt != 1 and ts is not null and ts !='')";
                    SqlHelper.ExecuteNonQuerys(delstr);
                    SqlHelper.ExecuteNonQuerys(delstr2);
                    string str = "select id from SaleBillVouch";
                    DataSet ds = SqlHelper.ExecuteDataset(connectionString, CommandType.Text, str);
                    foreach (DataRow dr in ds.Tables[0].Rows)
                    {
                        strbu.Append(dr["id"].ToString() + ",");
                    }
                    strbu = strbu.Remove(strbu.Length - 1, 1);
                    String[] ids = strbu.ToString().Split(',');
                    strGetOracleSQLIn = getOracleSQLIn(ids, "A.csaleinvoiceid");
                    //获取采购入库头数据
                    sql = " select A.csaleinvoiceid ID,A.vbillcode code,A.dbilldate ddate,A1.Code custcode,A.vnote remark,CASE WHEN A.ntotalastnum>0 THEN 0 ELSE 1 END AS isRed,A.modifiedtime ts from so_saleinvoice A left join bd_customer A1 on A.cinvoicecustid = A1.pk_customer where  not exists (select csaleinvoiceid from (select distinct pob.csaleinvoiceid csaleinvoiceid from so_saleinvoice_b pob left join bd_material mat on mat.pk_material = pob.cmaterialvid and nvl(mat.dr,0)=0 left join so_saleinvoice poh on poh.csaleinvoiceid = pob.csaleinvoiceid and nvl(poh.dr,0)=0 where  nvl(pob.dr,0)=0  and substr(mat.code,0,4) = '0915' and poh.PK_ORG='0001A110000000001V70'  and poh.dr != 1 AND substr(poh.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and pob.csendstordocid not in('1001A1100000000T5S5Z','1001A11000000003CYSY')) po  where po.csaleinvoiceid = A.csaleinvoiceid) and  A.PK_ORG='0001A110000000001V70' and A.dr != 1  AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and " + strGetOracleSQLIn + "";
                }
                DataSet Saleinvoice = OracleHelper.ExecuteDataset(sql);


                tableExist = "if object_id( 'SaleBillVouchs') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    //获取销售发票行数据
                    sql = "select A.csaleinvoiceid ID,A1.csaleinvoicebid autoid,A1.crowno doclineno,A1.vsrccode srcdocno, A1.vsrcrowno srcdoclineno, A2.code cinvcode, NVL(A1.nastnum ,0) qty,A1.ntaxrate itaxrate, A1.nqtorigtaxprice iOriTaxCost, A1.nqtorigprice iOriCost,A1.norigtaxmny ioriSum, A1.ntax iOriTaxPrice, A1.norigmny iOriMoney, A1.ntaxmny isum,A1.nmny iMoney,(A1.ntaxmny - A1.nmny) iTaxPrice,A1.nqttaxprice iUnitCost, A1.vrownote remark,A1.csendstordocid cwhcode from so_saleinvoice A left join so_saleinvoice_b A1 on A.csaleinvoiceid = A1.csaleinvoiceid and A1.dr!=1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material where A.PK_ORG = '0001A110000000001V70' and A.dr != 1 AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and substr(A2.code,0,4) != '0915' and A1.csendstordocid not in('1001A1100000000T5S5Z','1001A11000000003CYSY')";
                }
                else
                {
                    //获取销售发票行数据
                    sql = "select A.csaleinvoiceid ID,A1.csaleinvoicebid autoid,A1.crowno doclineno,A1.vsrccode srcdocno, A1.vsrcrowno srcdoclineno, A2.code cinvcode, NVL(A1.nastnum ,0) qty,A1.ntaxrate itaxrate, A1.nqtorigtaxprice iOriTaxCost, A1.nqtorigprice iOriCost,A1.norigtaxmny ioriSum, A1.ntax iOriTaxPrice, A1.norigmny iOriMoney, A1.ntaxmny isum,A1.nmny iMoney,(A1.ntaxmny - A1.nmny) iTaxPrice,A1.nqttaxprice iUnitCost, A1.vrownote remark,A1.csendstordocid cwhcode from so_saleinvoice A left join so_saleinvoice_b A1 on A.csaleinvoiceid = A1.csaleinvoiceid and A1.dr!=1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material where A.PK_ORG = '0001A110000000001V70' and A.dr != 1 AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and substr(A2.code,0,4) != '0915' and A1.csendstordocid not in('1001A1100000000T5S5Z','1001A11000000003CYSY') and  " + strGetOracleSQLIn + "";
                }
                DataSet SaleinvoiceLine = OracleHelper.ExecuteDataset(sql);

                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id( 'SaleBillVouch') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table SaleBillVouch(ID nvarchar(30) primary key not null,code nvarchar(50),ddate nvarchar(20),custcode nvarchar(50),remark nvarchar(100),isRed bit,ts nvarchar(50),zt bit default 0,memo text)";
                    SqlHelper.ExecuteNonQuery(createSql);
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(Saleinvoice, "SaleBillVouch");
                    SqlHelper.ExecuteNonQuery(str.ToString());
                    msg = "销售发票表插入成功";
                }
                else
                {
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(Saleinvoice, "SaleBillVouch");
                    SqlHelper.ExecuteNonQuery(str.ToString());
                    msg = "销售发票表更新成功";
                }
                tableExist = "if object_id( 'SaleBillVouchs') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table SaleBillVouchs(ID nvarchar(30),autoid nvarchar(30)  primary key not null,doclineno nvarchar(50),srcdocno nvarchar(50),	srcdoclineno nvarchar(50),cinvcode nvarchar(50),qty decimal(28,8),itaxrate decimal(28,8),iOriTaxCost decimal(28,8),iOriCost decimal(28,8),ioriSum decimal(28,8),iOriTaxPrice decimal(28,8),iOriMoney decimal(28,8),isum decimal(28,8),iMoney decimal(28,8),	iTaxPrice decimal(28,8),	iUnitCost decimal(28,8),remark nvarchar(100),cwhcode nvarchar(100))";
                    SqlHelper.ExecuteNonQuery(createSql);
                    StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(SaleinvoiceLine, "SaleBillVouchs");
                    SqlHelper.ExecuteNonQuery(strs.ToString());
                    msg = "销售发票表行插入成功";
                }
                else
                {
                    StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(SaleinvoiceLine, "SaleBillVouchs");
                    SqlHelper.ExecuteNonQuery(strs.ToString());
                    msg = "销售发票表行更新成功";
                }
                result = msg;
            }
            catch (Exception e)
            {

                result = "销售发票表错误：" + e.Message;
            }
            return result;

        }


        /// <summary>
        /// 从nc获取采购入库数据插入到sql
        /// 创建人：lvhe
        /// 创建时间：2019年10月23日 00:20:34
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
            StringBuilder strbu = new StringBuilder();
            string strGetOracleSQLIn = "";
            try
            {
                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id( 'Rdrecord01') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);
                if (existResult == 0)
                {
                    //获取采购入库头数据
                    sql = " select A.cgeneralhid ID, A.vbillcode code, A.dbilldate ddate, A2.code cwhcode, A3.code cvencode,A4.code cvenclass,A3.name cvenname,A1.CODE cdepcode,CASE WHEN A.freplenishflag='N' THEN 0 ELSE 1 END AS isRed,A.vnote remark, A.modifiedtime ts FROM ic_purchasein_h A left join org_dept A1 on A1.pk_dept = A.cdptid left join bd_stordoc A2 on A2.pk_stordoc = A.cwarehouseid left join bd_supplier A3 on A3.pk_supplier = A.cvendorid left join bd_supplierclass A4 on A4.pk_supplierclass=A3.pk_supplierclass where  not exists (select cgeneralhid from (select distinct pob.cgeneralhid cgeneralhid from ic_purchasein_b pob left join bd_material mat on mat.pk_material = pob.cmaterialvid and nvl(mat.dr,0)=0 left join ic_purchasein_h poh on poh.cgeneralhid = pob.cgeneralhid and nvl(poh.dr,0)=0 where  nvl(pob.dr,0)=0 and substr(mat.code,0,4) = '0915' and poh.PK_ORG='0001A110000000001V70'  AND substr(poh.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and poh.fbillflag=3) po  where po.cgeneralhid = A.cgeneralhid) and  A.PK_ORG='0001A110000000001V70' and A.dr!=1 and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY') AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag=3";
                }
                else
                {
                    string delstr = "delete from Rdrecord01 where id in(select ID from Rdrecord01 where zt != 1 and ts is not null and ts !='')";
                    string delstr2 = "delete from Rdrecords01 where id in(select ID from Rdrecord01 where zt != 1 and ts is not null and ts !='')";
                    SqlHelper.ExecuteNonQuerys(delstr);
                    SqlHelper.ExecuteNonQuerys(delstr2);
                    string str = "select id from Rdrecord01";
                    DataSet ds = SqlHelper.ExecuteDataset(connectionString, CommandType.Text, str);
                    foreach (DataRow dr in ds.Tables[0].Rows)
                    {
                        strbu.Append(dr["id"].ToString() + ",");
                    }
                    strbu = strbu.Remove(strbu.Length - 1, 1);
                    String[] ids = strbu.ToString().Split(',');
                    strGetOracleSQLIn = getOracleSQLIn(ids, "A.cgeneralhid");
                    //获取采购入库头数据
                    sql = " select A.cgeneralhid ID, A.vbillcode code, A.dbilldate ddate, A2.code cwhcode, A3.code cvencode,A4.code cvenclass,A3.name cvenname,A1.CODE cdepcode,CASE WHEN A.freplenishflag='N' THEN 0 ELSE 1 END AS isRed,A.vnote remark, A.modifiedtime ts FROM ic_purchasein_h A left join org_dept A1 on A1.pk_dept = A.cdptid left join bd_stordoc A2 on A2.pk_stordoc = A.cwarehouseid left join bd_supplier A3 on A3.pk_supplier = A.cvendorid left join bd_supplierclass A4 on A4.pk_supplierclass=A3.pk_supplierclass where  not exists (select cgeneralhid from (select distinct pob.cgeneralhid cgeneralhid from ic_purchasein_b pob left join bd_material mat on mat.pk_material = pob.cmaterialvid and nvl(mat.dr,0)=0 left join ic_purchasein_h poh on poh.cgeneralhid = pob.cgeneralhid and nvl(poh.dr,0)=0 where  nvl(pob.dr,0)=0 and substr(mat.code,0,4) = '0915' and poh.PK_ORG='0001A110000000001V70'  AND substr(poh.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and poh.fbillflag=3) po  where po.cgeneralhid = A.cgeneralhid) and  A.PK_ORG='0001A110000000001V70' and A.dr!=1 and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY') AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag=3 and " + strGetOracleSQLIn + "";
                }
                //获取采购入库头数据
                DataSet PurchaseIn = OracleHelper.ExecuteDataset(sql);


                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id( 'Rdrecord01') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    //获取采购入库行数据
                    sql = "SELECT A.cgeneralhid ID,A1.cgeneralbid autoid,A1.crowno doclineno,A2.code cinvcode,A2.name cinvname,A3.code cinvclass,A2.materialspec cinvstd,A4.code cinvUnit,NVL(A1.nassistnum ,0) qty, A1.ntaxrate itaxrate, A1.nqtorigtaxprice iOriTaxCost, A1.nqtorigprice iOriCost,A1.norigtaxmny ioriSum, A1.ntax iOriTaxPrice, A1.norigmny iOriMoney, A1.ntaxmny isum,A1.nmny iMoney, A1.ntax iTaxPrice, A1.nqtorigtaxprice iUnitCost,A1.vnotebody remark FROM ic_purchasein_h A left join ic_purchasein_b A1 on A1.cgeneralhid = A.cgeneralhid and A1.dr!=1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc where A.PK_ORG = '0001A110000000001V70' AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag = 3 and substr(A2.code,0,4) != '0915' and A.cwarehouseid  not in('1001A1100000000T5S5Z','1001A11000000003CYSY')";
                }
                else
                {
                    //获取采购入库行数据
                    sql = "SELECT A.cgeneralhid ID,A1.cgeneralbid autoid,A1.crowno doclineno,A2.code cinvcode,A2.name cinvname,A3.code cinvclass,A2.materialspec cinvstd,A4.code cinvUnit,NVL(A1.nassistnum ,0) qty, A1.ntaxrate itaxrate, A1.nqtorigtaxprice iOriTaxCost, A1.nqtorigprice iOriCost,A1.norigtaxmny ioriSum, A1.ntax iOriTaxPrice, A1.norigmny iOriMoney, A1.ntaxmny isum,A1.nmny iMoney, A1.ntax iTaxPrice, A1.nqtorigtaxprice iUnitCost,A1.vnotebody remark FROM ic_purchasein_h A left join ic_purchasein_b A1 on A1.cgeneralhid = A.cgeneralhid and A1.dr!=1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc where A.PK_ORG = '0001A110000000001V70' AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag = 3 and substr(A2.code,0,4) != '0915' and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY') and " + strGetOracleSQLIn + "";
                }

                //获取采购入库行数据
                DataSet PurchaseInLine = OracleHelper.ExecuteDataset(sql);

                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id( 'Rdrecord01') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table Rdrecord01(ID nvarchar(30) primary key not null,code nvarchar(50),ddate nvarchar(20),cwhcode nvarchar(50),cvencode nvarchar(50),cvenclass nvarchar(50),cvenname nvarchar(50),cdepcode nvarchar(50),isRed bit,remark nvarchar(200),ts nvarchar(50),crdcode nvarchar(50) default '101',cptcode nvarchar(10) default '01',zt bit default 0,memo text)";
                    SqlHelper.ExecuteNonQuery(createSql);
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(PurchaseIn, "Rdrecord01");
                    SqlHelper.ExecuteNonQuery(str.ToString());
                    msg = "采购入库表插入成功";
                }
                else
                {
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(PurchaseIn, "Rdrecord01");
                    SqlHelper.ExecuteNonQuery(str.ToString());
                    msg = "采购入库表更新成功";
                }
                tableExist = "if object_id( 'Rdrecords01') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table Rdrecords01(ID nvarchar(30),autoid nvarchar(30)  primary key not null,doclineno bigint,cinvcode nvarchar(50),cinvname nvarchar(500),cinvclass nvarchar(500),cinvstd nvarchar(500),cinvUnit nvarchar(500),qty decimal(28, 8),itaxrate decimal(28, 8),iOriTaxCost decimal(28, 8),iOriCost decimal(28, 8),ioriSum decimal(28, 8),iOriTaxPrice decimal(28, 8),iOriMoney decimal(28, 8),isum decimal(28, 8),iMoney decimal(28, 8),iTaxPrice decimal(28, 8),iUnitCost decimal(28, 8),remark nvarchar(200))";
                    SqlHelper.ExecuteNonQuery(createSql);
                    StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(PurchaseInLine, "Rdrecords01");
                    SqlHelper.ExecuteNonQuery(strs.ToString());
                    msg = "采购入库表行插入成功";
                }
                else
                {
                    StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(PurchaseInLine, "Rdrecords01");
                    SqlHelper.ExecuteNonQuery(strs.ToString());
                    msg = "采购入库表行更新成功";
                }
                result = msg;
            }
            catch (Exception e)
            {

                result = "采购入库表错误：" + e.Message;
            }
            return result;
        }

        /// <summary>
        /// 从nc获取销售出库数据插入到sql
        /// </summary>
        /// <returns></returns>
        [WebMethod]
        private string GetSaleOutToSql()
        {
            string result = "";
            string createSql = "";
            string tableExist = "";
            int existResult = 0;
            string msg = "";
            string sql = "";
            StringBuilder strbu = new StringBuilder();
            string strGetOracleSQLIn = "";
            try
            {
                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id( 'DispatchList') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    //获取销售出库头数据
                    sql = "select A.cgeneralhid ID,A.vbillcode code,A.dbilldate ddate,A1.Code cwhcode,A5.pk_billtypecode cstcode, A2.code custcode, A3.code cdepcode, A.vnote remark,CASE WHEN A.ntotalnum > 0 THEN 0 ELSE 1 END AS isRed,A.modifiedtime ts  from ic_saleout_h A left join bd_stordoc A1 on A.cwarehouseid = A1.Pk_Stordoc left join bd_customer A2 on A.ccustomerid = A2.pk_customer left join org_dept A3 on A3.PK_DEPT = A.cdptid left join (select cgeneralhid,csourcetranstype from ic_saleout_b group by cgeneralhid,csourcetranstype) A4 on A.cgeneralhid=A4.cgeneralhid left join bd_billtype A5 on A5.pk_billtypeid=A4.csourcetranstype where  not exists (select cgeneralhid from (select distinct pob.cgeneralhid cgeneralhid from ic_saleout_b pob left join bd_material mat on mat.pk_material = pob.cmaterialvid and nvl(mat.dr,0)=0 left join ic_saleout_h poh on poh.cgeneralhid = pob.cgeneralhid and nvl(poh.dr,0)=0 where  nvl(pob.dr,0)=0 and substr(mat.code,0,4) = '0915' and poh.PK_ORG='0001A110000000001V70'  AND substr(poh.taudittime,0,10) between '" + startTime + "' and '" + endTime + "' and poh.fbillflag=3) po  where po.cgeneralhid = A.cgeneralhid) and  A.PK_ORG='0001A110000000001V70'  AND substr(A.taudittime,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag=3 and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY')";
                }
                else
                {
                    string delstr = "delete from DispatchList where id in(select ID from DispatchList where zt != 1 and ts is not null and ts !='')";
                    string delstr2 = "delete from DispatchLists where id in(select ID from DispatchList where zt != 1 and ts is not null and ts !='')";
                    SqlHelper.ExecuteNonQuerys(delstr);
                    SqlHelper.ExecuteNonQuerys(delstr2);
                    string str = "select id from DispatchList";
                    DataSet ds = SqlHelper.ExecuteDataset(connectionString, CommandType.Text, str);
                    foreach (DataRow dr in ds.Tables[0].Rows)
                    {
                        strbu.Append(dr["id"].ToString() + ",");
                    }
                    strbu = strbu.Remove(strbu.Length - 1, 1);
                    String[] ids = strbu.ToString().Split(',');
                    strGetOracleSQLIn = getOracleSQLIn(ids, "A.cgeneralhid");
                    //获取采购入库头数据
                    sql = "select A.cgeneralhid ID,A.vbillcode code,A.dbilldate ddate,A1.Code cwhcode,A5.pk_billtypecode cstcode, A2.code custcode, A3.code cdepcode, A.vnote remark,CASE WHEN A.ntotalnum > 0 THEN 0 ELSE 1 END AS isRed,A.modifiedtime ts  from ic_saleout_h A left join bd_stordoc A1 on A.cwarehouseid = A1.Pk_Stordoc left join bd_customer A2 on A.ccustomerid = A2.pk_customer left join org_dept A3 on A3.PK_DEPT = A.cdptid left join (select cgeneralhid,csourcetranstype from ic_saleout_b group by cgeneralhid,csourcetranstype) A4 on A.cgeneralhid=A4.cgeneralhid left join bd_billtype A5 on A5.pk_billtypeid=A4.csourcetranstype where  not exists (select cgeneralhid from (select distinct pob.cgeneralhid cgeneralhid from ic_saleout_b pob left join bd_material mat on mat.pk_material = pob.cmaterialvid and nvl(mat.dr,0)=0 left join ic_saleout_h poh on poh.cgeneralhid = pob.cgeneralhid and nvl(poh.dr,0)=0 where  nvl(pob.dr,0)=0 and substr(mat.code,0,4) = '0915' and poh.PK_ORG='0001A110000000001V70'  AND substr(poh.taudittime,0,10) between '" + startTime + "' and '" + endTime + "' and poh.fbillflag=3) po  where po.cgeneralhid = A.cgeneralhid) and  A.PK_ORG='0001A110000000001V70'  AND substr(A.taudittime,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag=3 and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY') and " + strGetOracleSQLIn + "";
                }
                DataSet SaleOut = OracleHelper.ExecuteDataset(sql);

                tableExist = "if object_id( 'DispatchLists') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    //获取销售出库行数据
                    sql = "select A1.cgeneralhid ID,A1.cgeneralbid autoid,A1.crowno doclineno,A2.code cinvcode,A2.name cinvname,A3.code cinvclass,A2.materialspec cinvstd,A4.code cinvUnit,NVL(A1.nassistnum ,0) qty, A1.ntaxrate itaxrate, A1.norigtaxprice iOriTaxCost,A1.nqtorigprice iOriCost,A1.norigtaxmny ioriSum, A1.norigmny iOriMoney, A1.ntaxmny isum, A1.nmny iMoney, NVL(A1.ncostprice,0) iUnitCost, A1.vnotebody remark from ic_saleout_h A left join ic_saleout_b A1 on A.cgeneralhid = A1.cgeneralhid and A1.DR != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc  where A.PK_ORG = '0001A110000000001V70' and A.DR != 1 AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag = 3 and substr(A2.code,0,4) != '0915' and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY')";
                }
                else
                {
                    sql = "select A1.cgeneralhid ID,A1.cgeneralbid autoid,A1.crowno doclineno,A2.code cinvcode,A2.name cinvname,A3.code cinvclass,A2.materialspec cinvstd,A4.code cinvUnit,NVL(A1.nassistnum ,0) qty, A1.ntaxrate itaxrate, A1.norigtaxprice iOriTaxCost,A1.nqtorigprice iOriCost,A1.norigtaxmny ioriSum, A1.norigmny iOriMoney, A1.ntaxmny isum, A1.nmny iMoney, NVL(A1.ncostprice,0) iUnitCost, A1.vnotebody remark from ic_saleout_h A left join ic_saleout_b A1 on A.cgeneralhid = A1.cgeneralhid and A1.DR != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc  where A.PK_ORG = '0001A110000000001V70' and A.DR != 1 AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag = 3 and substr(A2.code,0,4) != '0915' and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY') and " + strGetOracleSQLIn + "";
                }
                DataSet SaleOutLine = OracleHelper.ExecuteDataset(sql);

                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id( 'DispatchList') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table DispatchList(ID nvarchar(30) primary key not null,code nvarchar(50),ddate nvarchar(20),cwhcode nvarchar(100),cstcode nvarchar(20),custcode nvarchar(50),cdepcode nvarchar(50),remark nvarchar(100),isRed bit default 0,ts nvarchar(50),zt bit default 0,memo text)";
                    SqlHelper.ExecuteNonQuery(createSql);
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(SaleOut, "DispatchList");
                    SqlHelper.ExecuteNonQuery(str.ToString());
                    msg = "销售出库表插入成功";
                }
                else
                {
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(SaleOut, "DispatchList");
                    SqlHelper.ExecuteNonQuery(str.ToString());
                    msg = "销售出库表更新成功";
                }
                tableExist = "if object_id( 'DispatchLists') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table DispatchLists(ID nvarchar(30),autoid nvarchar(30)  primary key not null,doclineno nvarchar(50),cinvcode nvarchar(50),cinvname nvarchar(500),cinvclass nvarchar(500),cinvstd nvarchar(500),cinvUnit  nvarchar(500),qty decimal(28, 8),itaxrate decimal(28, 8),iOriTaxCost decimal(28, 8),iOriCost decimal(28, 8),ioriSum decimal(28, 8),	iOriMoney decimal(28, 8),	isum decimal(28, 8),iMoney decimal(28, 8),iUnitCost decimal(28, 8),remark nvarchar(100))";
                    SqlHelper.ExecuteNonQuery(createSql);
                    StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(SaleOutLine, "DispatchLists");
                    SqlHelper.ExecuteNonQuery(strs.ToString());
                    msg = "销售出库表行插入成功";
                }
                else
                {
                    StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(SaleOutLine, "DispatchLists");
                    SqlHelper.ExecuteNonQuery(strs.ToString());
                    msg = "销售出库表行更新成功";
                }
                result = msg;
            }
            catch (Exception e)
            {

                result = "销售出库表：" + e.Message;
            }
            return result;

        }


        /// <summary>
        /// 从nc获取材料出库数据插入到sql
        /// 创建人：lvhe
        /// 创建时间：2019年11月28日 15:09:23
        /// </summary>
        /// <returns></returns>
        [WebMethod]
        private string GetMaterialToSql()
        {
            string result = "";
            string createSql = "";
            string tableExist = "";
            int existResult = 0;
            string msg = "";
            string sql = "";
            StringBuilder strbu = new StringBuilder();
            string strGetOracleSQLIn = "";
            try
            {
                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id('RdRecord11') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);
                if (existResult == 0)
                {
                    //获取材料出库头数据
                    sql = "select A.cgeneralhid ID,A.vbillcode code,A.dbilldate ddate,A1.Code cwhcode,A2.pk_billtypecode crdcode, A3.code cdepcode, A.vnote remark,CASE WHEN A.ntotalnum > 0 THEN 0 ELSE 1 END AS isRed,A.modifiedtime ts from ic_material_h A left join bd_stordoc A1 on A.cwarehouseid = A1.Pk_Stordoc left join bd_billtype A2 on A.ctrantypeid = A2.pk_billtypeid left join org_dept A3 on A3.PK_DEPT = A.cdptid where  not exists (select cgeneralhid from (select distinct pob.cgeneralhid cgeneralhid from ic_material_b pob left join bd_material mat on mat.pk_material = pob.cmaterialvid and nvl(mat.dr,0)=0 left join ic_material_h poh on poh.cgeneralhid = pob.cgeneralhid and nvl(poh.dr,0)=0 where  nvl(pob.dr,0)=0 and substr(mat.code,0,4) = '0915' and poh.PK_ORG='0001A110000000001V70'  AND substr(poh.taudittime,0,10) between '" + startTime + "' and '" + endTime + "' and poh.fbillflag=3) po  where po.cgeneralhid = A.cgeneralhid) and  A.PK_ORG='0001A110000000001V70'  AND substr(A.taudittime,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag=3 and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY')";
                }
                else
                {
                    string delstr = "delete from RdRecord11 where id in(select ID from RdRecord11 where zt != 1 and ts is not null and ts !='')";
                    string delstr2 = "delete from RdRecords11 where id in(select ID from RdRecord11 where zt != 1 and ts is not null and ts !='')";
                    SqlHelper.ExecuteNonQuerys(delstr);
                    SqlHelper.ExecuteNonQuerys(delstr2);
                    string str = "select id from RdRecord11";
                    DataSet ds = SqlHelper.ExecuteDataset(connectionString, CommandType.Text, str);
                    foreach (DataRow dr in ds.Tables[0].Rows)
                    {
                        strbu.Append(dr["id"].ToString() + ",");
                    }
                    strbu = strbu.Remove(strbu.Length - 1, 1);
                    String[] ids = strbu.ToString().Split(',');
                    strGetOracleSQLIn = getOracleSQLIn(ids, "A.cgeneralhid");
                    //获取采购入库头数据
                    sql = "select A.cgeneralhid ID,A.vbillcode code,A.dbilldate ddate,A1.Code cwhcode,A2.pk_billtypecode crdcode, A3.code cdepcode, A.vnote remark,CASE WHEN A.ntotalnum > 0 THEN 0 ELSE 1 END AS isRed,A.modifiedtime ts from ic_material_h A left join bd_stordoc A1 on A.cwarehouseid = A1.Pk_Stordoc left join bd_billtype A2 on A.ctrantypeid = A2.pk_billtypeid left join org_dept A3 on A3.PK_DEPT = A.cdptid where  not exists (select cgeneralhid from (select distinct pob.cgeneralhid cgeneralhid from ic_material_b pob left join bd_material mat on mat.pk_material = pob.cmaterialvid and nvl(mat.dr,0)=0 left join ic_material_h poh on poh.cgeneralhid = pob.cgeneralhid and nvl(poh.dr,0)=0 where  nvl(pob.dr,0)=0 and substr(mat.code,0,4) = '0915' and poh.PK_ORG='0001A110000000001V70'  AND substr(poh.taudittime,0,10) between '" + startTime + "' and '" + endTime + "' and poh.fbillflag=3) po  where po.cgeneralhid = A.cgeneralhid) and  A.PK_ORG='0001A110000000001V70'  AND substr(A.taudittime,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag=3 and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY') and " + strGetOracleSQLIn + "";
                }
                DataSet Material = OracleHelper.ExecuteDataset(sql);

                tableExist = "if object_id( 'RdRecords11') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    //获取材料出库行数据
                    sql = "select A1.cgeneralhid ID,A1.cgeneralbid autoid,A1.crowno doclineno,A2.code cinvcode,NVL(A1.nassistnum ,0) qty, A1.vnotebody remark from ic_material_h A left join ic_material_b A1 on A.cgeneralhid = A1.cgeneralhid and A1.DR != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material where A.PK_ORG = '0001A110000000001V70' and A.DR != 1 AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag = 3 and substr(A2.code,0,4) != '0915' and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY')";
                }
                else
                {
                    sql = "select A1.cgeneralhid ID,A1.cgeneralbid autoid,A1.crowno doclineno,A2.code cinvcode,NVL(A1.nassistnum ,0) qty, A1.vnotebody remark from ic_material_h A left join ic_material_b A1 on A.cgeneralhid = A1.cgeneralhid and A1.DR != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material where A.PK_ORG = '0001A110000000001V70' and A.DR != 1 AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag = 3 and substr(A2.code,0,4) != '0915' and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY') and " + strGetOracleSQLIn + "";
                }
                DataSet MaterialLine = OracleHelper.ExecuteDataset(sql);

                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id('RdRecord11') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table RdRecord11(ID nvarchar(30) primary key not null,code nvarchar(50),ddate nvarchar(20),cwhcode nvarchar(100),crdcode nvarchar(20),cdepcode nvarchar(50),remark nvarchar(100),isRed bit default 0,ts nvarchar(50),zt bit default 0, memo text)";
                    SqlHelper.ExecuteNonQuery(createSql);
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(Material, "RdRecord11");
                    SqlHelper.ExecuteNonQuery(str.ToString());
                    msg = "材料出库表插入成功";
                }
                else
                {
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(Material, "RdRecord11");
                    SqlHelper.ExecuteNonQuery(str.ToString());
                    msg = "材料出库表更新成功";
                }
                tableExist = "if object_id( 'RdRecords11') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table RdRecords11(ID nvarchar(30),autoid nvarchar(30)  primary key not null,doclineno nvarchar(50),cinvcode nvarchar(50),qty decimal(28, 8),remark nvarchar(100))";
                    SqlHelper.ExecuteNonQuery(createSql);
                    StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(MaterialLine, "RdRecords11");
                    SqlHelper.ExecuteNonQuery(strs.ToString());
                    msg = "材料出库表行插入成功";
                }
                else
                {
                    StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(MaterialLine, "RdRecords11");
                    SqlHelper.ExecuteNonQuery(strs.ToString());
                    msg = "材料出库表行更新成功";
                }
                result = msg;
            }
            catch (Exception e)
            {

                result = "材料出库表错误：" + e.Message;
            }
            return result;

        }


        /// <summary>
        /// 从nc获取其他入库数据插入到sql
        /// 创建人：lvhe
        /// 创建时间：2019年12月9日 21:53:53
        /// </summary>
        /// <returns></returns>
        private string GetIAi4billToSql()
        {
            string result = "";
            string createSql = "";
            string tableExist = "";
            int existResult = 0;
            string msg = "";
            string sql = "";
            StringBuilder strbu = new StringBuilder();
            string strGetOracleSQLIn = "";
            try
            {
                //获取其他入库头数据
                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id('RdRecord08') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    sql = "SELECT A.cgeneralhid ID, A.vbillcode code, A.dbilldate ddate, A2.code cwhcode,A3.pk_billtypecode crdcode,A1.CODE cdepcode, A1.name cdepname,A.vnote remark, A.modifiedtime ts FROM ic_generalin_h A left join org_dept A1 on A1.pk_dept = A.cdptid left join bd_stordoc A2 on A2.pk_stordoc = A.cwarehouseid left join bd_billtype A3 on A3.pk_billtypeid = A.ctrantypeid where  not exists (select cgeneralhid from (select distinct pob.cgeneralhid cgeneralhid from ic_generalin_b pob left join bd_material mat on mat.pk_material = pob.cmaterialvid and nvl(mat.dr,0)=0 left join ic_generalin_h poh on poh.cgeneralhid = pob.cgeneralhid and nvl(poh.dr,0)=0 where  nvl(pob.dr,0)=0 and substr(mat.code,0,4) = '0915' and poh.PK_ORG='0001A110000000001V70'  AND substr(poh.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "') po  where po.cgeneralhid = A.cgeneralhid) and  A.cothercalbodyoid  = '0001A110000000001V70' and A.dr != 1 and (A3.billtypename  not like  '%转库入库%' and A3.billtypename not like '%形态转换入库%') AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY') ";
                }
                else
                {
                    string delstr = "delete from RdRecord08 where id in(select ID from RdRecord08 where zt != 1 and ts is not null and ts !='')";
                    string delstr2 = "delete from RdRecords08 where id in(select ID from RdRecord08 where zt != 1 and ts is not null and ts !='')";
                    SqlHelper.ExecuteNonQuerys(delstr);
                    SqlHelper.ExecuteNonQuerys(delstr2);
                    string str = "select id from RdRecord08";
                    DataSet ds = SqlHelper.ExecuteDataset(connectionString, CommandType.Text, str);
                    foreach (DataRow dr in ds.Tables[0].Rows)
                    {
                        strbu.Append(dr["id"].ToString() + ",");
                    }
                    strbu = strbu.Remove(strbu.Length - 1, 1);
                    String[] ids = strbu.ToString().Split(',');
                    strGetOracleSQLIn = getOracleSQLIn(ids, "A.cbillid");
                    //获取采购入库头数据
                    sql = "SELECT A.cgeneralhid ID, A.vbillcode code, A.dbilldate ddate, A2.code cwhcode,A3.pk_billtypecode crdcode,A1.CODE cdepcode, A1.name cdepname,A.vnote remark, A.modifiedtime ts FROM ic_generalin_h A left join org_dept A1 on A1.pk_dept = A.cdptid left join bd_stordoc A2 on A2.pk_stordoc = A.cwarehouseid left join bd_billtype A3 on A3.pk_billtypeid = A.ctrantypeid where  not exists (select cgeneralhid from (select distinct pob.cgeneralhid cgeneralhid from ic_generalin_b pob left join bd_material mat on mat.pk_material = pob.cmaterialvid and nvl(mat.dr,0)=0 left join ic_generalin_h poh on poh.cgeneralhid = pob.cgeneralhid and nvl(poh.dr,0)=0 where  nvl(pob.dr,0)=0 and substr(mat.code,0,4) = '0915' and poh.PK_ORG='0001A110000000001V70'  AND substr(poh.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "') po  where po.cgeneralhid = A.cgeneralhid) and  A.cothercalbodyoid  = '0001A110000000001V70' and A.dr != 1 and (A3.billtypename  not like  '%转库入库%' and A3.billtypename not like '%形态转换入库%') AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY')  and " + strGetOracleSQLIn + "";
                }
                DataSet IAi4bill = OracleHelper.ExecuteDataset(sql);

                tableExist = "if object_id( 'RdRecords08') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    //获取其他入库行数据
                    sql = "SELECT A.cgeneralhid ID,A1.cgeneralbid autoid,A1.crowno doclineno,A2.code cinvcode, A2.name cinvname, A3.code cinvclass, A2.materialspec cinvstd, A4.code cinvUnit, NVL(A1.nshouldassistnum  ,0) qty FROM ic_generalin_h A left join ic_generalin_b A1 on A1.cgeneralhid = A.cgeneralhid and A1.dr != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 on A3.pk_marbasclass = A2.pk_marbasclass left join bd_measdoc A4 on A4.pk_measdoc = A2.pk_measdoc left join bd_billtype A5 on A5.pk_billtypeid = A.ctrantypeid  where A.cothercalbodyoid = '0001A110000000001V70' and (A5.billtypename  not like  '%转库入库%' and A5.billtypename not like '%形态转换入库%') AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and substr(A2.code,0,4) != '0915' and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY')";
                }
                else
                {
                    sql = "SELECT A.cgeneralhid ID,A1.cgeneralbid autoid,A1.crowno doclineno,A2.code cinvcode, A2.name cinvname, A3.code cinvclass, A2.materialspec cinvstd, A4.code cinvUnit, NVL(A1.nshouldassistnum  ,0) qty FROM ic_generalin_h A left join ic_generalin_b A1 on A1.cgeneralhid = A.cgeneralhid and A1.dr != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 on A3.pk_marbasclass = A2.pk_marbasclass left join bd_measdoc A4 on A4.pk_measdoc = A2.pk_measdoc left join bd_billtype A5 on A5.pk_billtypeid = A.ctrantypeid  where A.cothercalbodyoid = '0001A110000000001V70' and (A5.billtypename  not like  '%转库入库%' and A5.billtypename not like '%形态转换入库%') AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and substr(A2.code,0,4) != '0915' and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY') and " + strGetOracleSQLIn + "";
                }
                DataSet IAi4billLine = OracleHelper.ExecuteDataset(sql);

                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id('RdRecord08') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table RdRecord08(ID nvarchar(30) primary key not null,code nvarchar(50),ddate nvarchar(20),cwhcode nvarchar(100),crdcode nvarchar(20),cdepcode nvarchar(50),cdepname nvarchar(50),remark nvarchar(100),ts nvarchar(50),isRed bit default 0,zt bit default 0,memo text)";
                    SqlHelper.ExecuteNonQuery(createSql);
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(IAi4bill, "RdRecord08");
                    SqlHelper.ExecuteNonQuery(str.ToString());
                    msg = "其他入库表插入成功";
                }
                else
                {
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(IAi4bill, "RdRecord08");
                    SqlHelper.ExecuteNonQuery(str.ToString());
                    msg = "其他入库表更新成功";
                }
                tableExist = "if object_id('RdRecords08') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table RdRecords08(ID nvarchar(30),autoid nvarchar(30)  primary key not null,doclineno nvarchar(50),cinvcode nvarchar(50),cinvname nvarchar(500),cinvclass nvarchar(500),cinvstd nvarchar(500),cinvUnit  nvarchar(500),qty decimal(28,8),remark nvarchar(100))";
                    SqlHelper.ExecuteNonQuery(createSql);
                    StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(IAi4billLine, "RdRecords08");
                    SqlHelper.ExecuteNonQuery(strs.ToString());
                    msg = "其他入库表行插入成功";
                }
                else
                {
                    StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(IAi4billLine, "RdRecords08");
                    SqlHelper.ExecuteNonQuery(strs.ToString());
                    msg = "其他入库表行更新成功";
                }
                result = msg;
            }
            catch (Exception e)
            {

                result = "其他入库表错误：" + e.Message;
            }
            return result;

        }


        /// <summary>
        /// 从nc获取其他出库数据插入到sql
        /// 创建人：lvhe
        /// 创建时间：2019年12月9日 21:53:53
        /// </summary>
        /// <returns></returns>
        private string GetIAi7billToSql()
        {
            string result = "";
            string createSql = "";
            string tableExist = "";
            int existResult = 0;
            string msg = "";
            string sql = "";
            StringBuilder strbu = new StringBuilder();
            string strGetOracleSQLIn = "";
            try
            {
                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id('RdRecord09') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    //获取其他出库头数据
                    sql = "SELECT A.cgeneralhid ID, A.vbillcode code, A.dbilldate ddate, A2.code cwhcode,A3.pk_billtypecode crdcode,A1.CODE cdepcode, A1.name cdepname,A.vnote remark, A.modifiedtime ts FROM ic_generalout_h A left join org_dept A1 on A1.pk_dept = A.cdptid left join bd_stordoc A2 on A2.pk_stordoc = A.cwarehouseid left join bd_billtype A3 on A3.pk_billtypeid = A.ctrantypeid where  not exists (select cgeneralhid from (select distinct pob.cgeneralhid cgeneralhid from ic_generalout_b pob left join bd_material mat on mat.pk_material = pob.cmaterialvid and nvl(mat.dr,0)=0 left join ic_generalout_h poh on poh.cgeneralhid = pob.cgeneralhid and nvl(poh.dr,0)=0 where  nvl(pob.dr,0)=0 and substr(mat.code,0,4) = '0915' and poh.PK_ORG='0001A110000000001V70'  AND substr(poh.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "') po  where po.cgeneralhid = A.cgeneralhid) and  A.cothercalbodyoid  = '0001A110000000001V70' and A.dr != 1 and (A3.billtypename  not like  '%转库入库%' and A3.billtypename not like '%形态转换入库%') AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A.cwarehouseid  not in('1001A1100000000T5S5Z','1001A11000000003CYSY')";
                }
                else
                {
                    string delstr = "delete from RdRecord09 where id in(select ID from RdRecord09 where zt != 1 and ts is not null and ts !='')";
                    string delstr2 = "delete from RdRecords09 where id in(select ID from RdRecord09 where zt != 1 and ts is not null and ts !='')";
                    SqlHelper.ExecuteNonQuerys(delstr);
                    SqlHelper.ExecuteNonQuerys(delstr2);
                    string str = "select id from RdRecord09";
                    DataSet ds = SqlHelper.ExecuteDataset(connectionString, CommandType.Text, str);
                    foreach (DataRow dr in ds.Tables[0].Rows)
                    {
                        strbu.Append(dr["id"].ToString() + ",");
                    }
                    strbu = strbu.Remove(strbu.Length - 1, 1);
                    String[] ids = strbu.ToString().Split(',');
                    strGetOracleSQLIn = getOracleSQLIn(ids, "A.cbillid");
                    //获取采购入库头数据
                    sql = "SELECT A.cgeneralhid ID, A.vbillcode code, A.dbilldate ddate, A2.code cwhcode,A3.pk_billtypecode crdcode,A1.CODE cdepcode, A1.name cdepname,A.vnote remark, A.modifiedtime ts FROM ic_generalout_h A left join org_dept A1 on A1.pk_dept = A.cdptid left join bd_stordoc A2 on A2.pk_stordoc = A.cwarehouseid left join bd_billtype A3 on A3.pk_billtypeid = A.ctrantypeid where  not exists (select cgeneralhid from (select distinct pob.cgeneralhid cgeneralhid from ic_generalout_b pob left join bd_material mat on mat.pk_material = pob.cmaterialvid and nvl(mat.dr,0)=0 left join ic_generalout_h poh on poh.cgeneralhid = pob.cgeneralhid and nvl(poh.dr,0)=0 where  nvl(pob.dr,0)=0 and substr(mat.code,0,4) = '0915' and poh.PK_ORG='0001A110000000001V70'  AND substr(poh.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "') po  where po.cgeneralhid = A.cgeneralhid) and  A.cothercalbodyoid  = '0001A110000000001V70' and A.dr != 1 and (A3.billtypename  not like  '%转库入库%' and A3.billtypename not like '%形态转换入库%') AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A.cwarehouseid  not in('1001A1100000000T5S5Z','1001A11000000003CYSY') and " + strGetOracleSQLIn + "";
                }
                DataSet IAi7bill = OracleHelper.ExecuteDataset(sql);

                tableExist = "if object_id( 'RdRecords09') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    //获取其他出库行数据
                    sql = "SELECT A.cgeneralhid ID,A1.cgeneralbid autoid,A1.crowno doclineno,A2.code cinvcode, A2.name cinvname, A3.code cinvclass, A2.materialspec cinvstd, A4.code cinvUnit, NVL(A1.nshouldassistnum ,0) qty FROM ic_generalout_h A left join ic_generalout_b A1 on A1.cgeneralhid = A.cgeneralhid and A1.dr != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 on A3.pk_marbasclass = A2.pk_marbasclass left join bd_measdoc A4 on A4.pk_measdoc = A2.pk_measdoc left join bd_billtype A5 on A5.pk_billtypeid = A.ctrantypeid  where A.cothercalbodyoid = '0001A110000000001V70' and (A5.billtypename  not like  '%转库入库%' and A5.billtypename not like '%形态转换入库%') AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and substr(A2.code,0,4) != '0915' and A.cwarehouseid  not in('1001A1100000000T5S5Z','1001A11000000003CYSY') ";
                }
                else
                {
                    sql = "SELECT A.cgeneralhid ID,A1.cgeneralbid autoid,A1.crowno doclineno,A2.code cinvcode, A2.name cinvname, A3.code cinvclass, A2.materialspec cinvstd, A4.code cinvUnit, NVL(A1.nshouldassistnum ,0) qty FROM ic_generalout_h A left join ic_generalout_b A1 on A1.cgeneralhid = A.cgeneralhid and A1.dr != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 on A3.pk_marbasclass = A2.pk_marbasclass left join bd_measdoc A4 on A4.pk_measdoc = A2.pk_measdoc left join bd_billtype A5 on A5.pk_billtypeid = A.ctrantypeid  where A.cothercalbodyoid = '0001A110000000001V70' and (A5.billtypename  not like  '%转库入库%' and A5.billtypename not like '%形态转换入库%') AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and substr(A2.code,0,4) != '0915' and A.cwarehouseid  not in('1001A1100000000T5S5Z','1001A11000000003CYSY')  and " + strGetOracleSQLIn + "";
                }
                DataSet IAi7billLine = OracleHelper.ExecuteDataset(sql);

                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id('RdRecord09') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table RdRecord09(ID nvarchar(30) primary key not null,code nvarchar(50),ddate nvarchar(20),cwhcode nvarchar(100),crdcode nvarchar(20),cdepcode nvarchar(50),cdepname nvarchar(50),remark nvarchar(100),ts nvarchar(50),isRed bit default 0,zt bit default 0,memo text)";
                    SqlHelper.ExecuteNonQuery(createSql);
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(IAi7bill, "RdRecord09");
                    SqlHelper.ExecuteNonQuery(str.ToString());
                    msg = "其他出库表插入成功";
                }
                else
                {
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(IAi7bill, "RdRecord09");
                    SqlHelper.ExecuteNonQuery(str.ToString());
                    msg = "其他出库表更新成功";
                }
                tableExist = "if object_id( 'RdRecords09') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table RdRecords09(ID nvarchar(30),autoid nvarchar(30)  primary key not null,doclineno nvarchar(50),cinvcode nvarchar(50),cinvname nvarchar(500),cinvclass nvarchar(500),cinvstd nvarchar(500),cinvUnit  nvarchar(500),qty decimal(28,8),remark nvarchar(100))";
                    SqlHelper.ExecuteNonQuery(createSql);
                    StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(IAi7billLine, "RdRecords09");
                    SqlHelper.ExecuteNonQuery(strs.ToString());
                    msg = "其他出库表行插入成功";
                }
                else
                {
                    StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(IAi7billLine, "RdRecords09");
                    SqlHelper.ExecuteNonQuery(strs.ToString());
                    msg = "其他出库表行更新成功";
                }
                result = msg;
            }
            catch (Exception e)
            {

                result = "其他出库表错误：" + e.Message;
            }
            return result;

        }


        /// <summary>
        /// 从nc获取产成品入库单数据插入到sql
        /// 创建人：lvhe
        /// 创建时间：2019年12月14日 17:57:01
        /// </summary>
        /// <returns></returns>
        [WebMethod]
        private string GetFinprodInToSql()
        {
            string result = "";
            string createSql = "";
            string tableExist = "";
            int existResult = 0;
            string msg = "";
            string sql = "";
            StringBuilder strbu = new StringBuilder();
            string strGetOracleSQLIn = "";
            try
            {
                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id( 'RdRecord10') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    //获取产成品入库头数据
                    sql = "SELECT A.cgeneralhid ID, A.vbillcode code, A.dbilldate ddate, A2.code cwhcode, A3.pk_billtypecode crdcode,A1.CODE cdepcode, A.vnote remark,CASE WHEN A.ntotalnum > 0 THEN 0 ELSE 1 END AS isRed, A.modifiedtime ts FROM ic_finprodin_h A left join org_dept A1 on A1.pk_dept = A.cdptid left join bd_stordoc A2 on A2.pk_stordoc = A.cwarehouseid left join bd_billtype A3 on A3.pk_billtypeid = A.ctrantypeid where  not exists (select cgeneralhid from (select distinct pob.cgeneralhid cgeneralhid from ic_finprodin_b pob left join bd_material mat on mat.pk_material = pob.cmaterialvid and nvl(mat.dr,0)=0 left join ic_finprodin_h poh on poh.cgeneralhid = pob.cgeneralhid and nvl(poh.dr,0)=0 where  nvl(pob.dr,0)=0 and substr(mat.code,0,4) = '0915' and poh.PK_ORG='0001A110000000001V70'  AND substr(poh.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and poh.fbillflag=3) po  where po.cgeneralhid = A.cgeneralhid) and  A.PK_ORG='0001A110000000001V70'  AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag=3 and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY')";
                }
                else
                {
                    string delstr = "delete from RdRecord10 where id in(select ID from RdRecord10 where zt != 1 and ts is not null and ts !='')";
                    string delstr2 = "delete from RdRecords10 where id in(select ID from RdRecord10 where zt != 1 and ts is not null and ts !='')";
                    SqlHelper.ExecuteNonQuerys(delstr);
                    SqlHelper.ExecuteNonQuerys(delstr2);
                    string str = "select id from Rdrecord01";
                    DataSet ds = SqlHelper.ExecuteDataset(connectionString, CommandType.Text, str);
                    foreach (DataRow dr in ds.Tables[0].Rows)
                    {
                        strbu.Append(dr["id"].ToString() + ",");
                    }
                    strbu = strbu.Remove(strbu.Length - 1, 1);
                    String[] ids = strbu.ToString().Split(',');
                    strGetOracleSQLIn = getOracleSQLIn(ids, "A.cgeneralhid");
                    //获取采购入库头数据
                    sql = "SELECT A.cgeneralhid ID, A.vbillcode code, A.dbilldate ddate, A2.code cwhcode, A3.pk_billtypecode crdcode,A1.CODE cdepcode, A.vnote remark,CASE WHEN A.ntotalnum > 0 THEN 0 ELSE 1 END AS isRed, A.modifiedtime ts FROM ic_finprodin_h A left join org_dept A1 on A1.pk_dept = A.cdptid left join bd_stordoc A2 on A2.pk_stordoc = A.cwarehouseid left join bd_billtype A3 on A3.pk_billtypeid = A.ctrantypeid where  not exists (select cgeneralhid from (select distinct pob.cgeneralhid cgeneralhid from ic_finprodin_b pob left join bd_material mat on mat.pk_material = pob.cmaterialvid and nvl(mat.dr,0)=0 left join ic_finprodin_h poh on poh.cgeneralhid = pob.cgeneralhid and nvl(poh.dr,0)=0 where  nvl(pob.dr,0)=0 and substr(mat.code,0,4) = '0915' and poh.PK_ORG='0001A110000000001V70'  AND substr(poh.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and poh.fbillflag=3) po  where po.cgeneralhid = A.cgeneralhid) and  A.PK_ORG='0001A110000000001V70'  AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag=3 and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY') and " + strGetOracleSQLIn + "";
                }
                DataSet FinprodIn = OracleHelper.ExecuteDataset(sql);

                tableExist = "if object_id( 'RdRecords10') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    //获取产成品入库行数据
                    sql = "SELECT A.cgeneralhid ID,A1.cgeneralbid autoid,A1.crowno doclineno,A2.code cinvcode,A2.name cinvname, A3.code cinvclass, A2.materialspec cinvstd, A4.code cinvUnit,NVL(A1.nassistnum ,0) qty, A1.vnotebody remark FROM ic_finprodin_h A left join ic_finprodin_b A1 on A1.cgeneralhid = A.cgeneralhid and A1.dr != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc  where A.PK_ORG = '0001A110000000001V70' AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag = 3 and substr(A2.code,0,4) != '0915' and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY')";
                }
                else
                {
                    sql = "SELECT A.cgeneralhid ID,A1.cgeneralbid autoid,A1.crowno doclineno,A2.code cinvcode,A2.name cinvname, A3.code cinvclass, A2.materialspec cinvstd, A4.code cinvUnit,NVL(A1.nassistnum ,0) qty, A1.vnotebody remark FROM ic_finprodin_h A left join ic_finprodin_b A1 on A1.cgeneralhid = A.cgeneralhid and A1.dr != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc  where A.PK_ORG = '0001A110000000001V70' AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag = 3 and substr(A2.code,0,4) != '0915' and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY') and " + strGetOracleSQLIn + "";
                }
                DataSet FinprodInLine = OracleHelper.ExecuteDataset(sql);

                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id( 'RdRecord10') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table RdRecord10(ID nvarchar(30) primary key not null,code nvarchar(50),ddate nvarchar(20),cwhcode nvarchar(100),crdcode nvarchar(20),cdepcode nvarchar(50),remark nvarchar(100),isRed bit default 0,ts nvarchar(50),zt bit default 0,memo text)";
                    SqlHelper.ExecuteNonQuery(createSql);
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(FinprodIn, "RdRecord10");
                    SqlHelper.ExecuteNonQuery(str.ToString());
                    msg = "产成品入库表插入成功";
                }
                else
                {
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(FinprodIn, "RdRecord10");
                    SqlHelper.ExecuteNonQuery(str.ToString());
                    msg = "产成品入库表更新成功";
                }
                tableExist = "if object_id( 'RdRecords10') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table RdRecords10(ID nvarchar(30),autoid nvarchar(30)  primary key not null,doclineno nvarchar(50),cinvcode nvarchar(50),cinvname nvarchar(500),cinvclass nvarchar(500),cinvstd nvarchar(500),cinvUnit  nvarchar(500),qty decimal(28,8),remark nvarchar(100))";
                    SqlHelper.ExecuteNonQuery(createSql);
                    StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(FinprodInLine, "RdRecords10");
                    SqlHelper.ExecuteNonQuery(strs.ToString());
                    msg = "产成品入库表行插入成功";
                }
                else
                {
                    StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(FinprodInLine, "RdRecords10");
                    SqlHelper.ExecuteNonQuery(strs.ToString());
                    msg = "产成品入库表行更新成功";
                }
                result = msg;
            }
            catch (Exception e)
            {

                result = "产成品入库错误：" + e.Message;
            }
            return result;

        }


        /// <summary>
        /// 从nc调拨单  NC 转库单数据插入到sql
        /// 创建人：lvhe
        /// 创建时间：2019-12-19 23:14:10
        /// </summary>
        /// <returns></returns>
        [WebMethod]
        private string GetIcWhstransHToSql()
        {
            string result = "";
            string createSql = "";
            string tableExist = "";
            int existResult = 0;
            string msg = "";
            string sql = "";
            StringBuilder strbu = new StringBuilder();
            string strGetOracleSQLIn = "";
            try
            {
                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id( 'TransVouch') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    //获取转库单头数据
                    sql = "SELECT A.cspecialhid ID, A.vbillcode code, A.dbilldate ddate, A1.code cowhcode,A2.code ciwhcode,A3.code codepcode, A4.code cidepcode, A.vnote remark, CASE WHEN A.ntotalnum > 0 THEN 0 ELSE 1 END AS isRed,A.modifiedtime ts FROM ic_whstrans_h A left join bd_stordoc A1 on A1.pk_stordoc = A.cwarehouseid left join bd_stordoc A2 on A2.pk_stordoc = A.cotherwhid left join org_dept_v A3 on A3.pk_vid = A.cdptvid left join org_dept_v A4 on A4.pk_vid = A.cotherdptvid where  not exists (select cspecialhid from (select distinct pob.cspecialhid cspecialhid from ic_whstrans_b pob left join bd_material mat on mat.pk_material = pob.cmaterialvid and nvl(mat.dr,0)=0 left join ic_whstrans_h poh on poh.cspecialhid = pob.cspecialhid and nvl(poh.dr,0)=0 where  nvl(pob.dr,0)=0 and substr(mat.code,0,4) = '0915' and poh.PK_ORG='0001A110000000001V70'  AND substr(poh.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and poh.fbillflag=4) po  where po.cspecialhid = A.cspecialhid) and  A.PK_ORG='0001A110000000001V70'  AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag=4 and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY')  and A.cotherwhid  not in('1001A1100000000T5S5Z','1001A11000000003CYSY')";
                }
                else
                {
                    string delstr = "delete from TransVouch where id in(select ID from TransVouch where zt != 1 and ts is not null and ts !='')";
                    string delstr2 = "delete from TransVouchs where id in(select ID from TransVouch where zt != 1 and ts is not null and ts !='')";
                    SqlHelper.ExecuteNonQuerys(delstr);
                    SqlHelper.ExecuteNonQuerys(delstr2);
                    string str = "select id from TransVouch";
                    DataSet ds = SqlHelper.ExecuteDataset(connectionString, CommandType.Text, str);
                    foreach (DataRow dr in ds.Tables[0].Rows)
                    {
                        strbu.Append(dr["id"].ToString() + ",");
                    }
                    strbu = strbu.Remove(strbu.Length - 1, 1);
                    String[] ids = strbu.ToString().Split(',');
                    strGetOracleSQLIn = getOracleSQLIn(ids, "A.cspecialhid");
                    //获取采购入库头数据
                    sql = "SELECT A.cspecialhid ID, A.vbillcode code, A.dbilldate ddate, A1.code cowhcode,A2.code ciwhcode,A3.code codepcode, A4.code cidepcode, A.vnote remark, CASE WHEN A.ntotalnum > 0 THEN 0 ELSE 1 END AS isRed,A.modifiedtime ts FROM ic_whstrans_h A left join bd_stordoc A1 on A1.pk_stordoc = A.cwarehouseid left join bd_stordoc A2 on A2.pk_stordoc = A.cotherwhid left join org_dept_v A3 on A3.pk_vid = A.cdptvid left join org_dept_v A4 on A4.pk_vid = A.cotherdptvid where  not exists (select cspecialhid from (select distinct pob.cspecialhid cspecialhid from ic_whstrans_b pob left join bd_material mat on mat.pk_material = pob.cmaterialvid and nvl(mat.dr,0)=0 left join ic_whstrans_h poh on poh.cspecialhid = pob.cspecialhid and nvl(poh.dr,0)=0 where  nvl(pob.dr,0)=0 and substr(mat.code,0,4) = '0915' and poh.PK_ORG='0001A110000000001V70'  AND substr(poh.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and poh.fbillflag=4) po  where po.cspecialhid = A.cspecialhid) and  A.PK_ORG='0001A110000000001V70'  AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag=4 and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY')  and A.cotherwhid  not in('1001A1100000000T5S5Z','1001A11000000003CYSY') and " + strGetOracleSQLIn + "";
                }
                DataSet IcWhstransH = OracleHelper.ExecuteDataset(sql);

                tableExist = "if object_id( 'TransVouchs') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    //获取转库单行数据
                    sql = "SELECT A.cspecialhid ID,A1.cspecialbid autoid,A1.crowno doclineno,A2.code cinvcode, A2.name cinvname, A3.code cinvclass, A2.materialspec cinvstd,A4.code cinvUnit,NVL(A1.nassistnum ,0) qty, A1.vnotebody remark FROM ic_whstrans_h A left join ic_whstrans_b A1 on A1.cspecialhid = A.cspecialhid and A1.dr != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc where A.PK_ORG = '0001A110000000001V70' AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag = 4 and substr(A2.code,0,4) != '0915' and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY')  and A.cotherwhid  not in('1001A1100000000T5S5Z','1001A11000000003CYSY')";
                }
                else
                {
                    sql = "SELECT A.cspecialhid ID,A1.cspecialbid autoid,A1.crowno doclineno,A2.code cinvcode, A2.name cinvname, A3.code cinvclass, A2.materialspec cinvstd,A4.code cinvUnit,NVL(A1.nassistnum ,0) qty, A1.vnotebody remark FROM ic_whstrans_h A left join ic_whstrans_b A1 on A1.cspecialhid = A.cspecialhid and A1.dr != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc where A.PK_ORG = '0001A110000000001V70' AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag = 4 and substr(A2.code,0,4) != '0915' and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY')  and A.cotherwhid  not in('1001A1100000000T5S5Z','1001A11000000003CYSY') and " + strGetOracleSQLIn + "";
                }
                DataSet IcWhstransHLine = OracleHelper.ExecuteDataset(sql);

                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id( 'TransVouch') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table TransVouch(ID nvarchar(30) primary key not null,code nvarchar(50),ddate nvarchar(20),cowhcode nvarchar(100),ciwhcode nvarchar(100),codepcode nvarchar(50),cidepcode nvarchar(50),remark nvarchar(100),isRed bit default 0,ts nvarchar(50),zt bit default 0,memo text)";
                    SqlHelper.ExecuteNonQuery(createSql);
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(IcWhstransH, "TransVouch");
                    SqlHelper.ExecuteNonQuery(str.ToString());
                    msg = "转库单表插入成功";
                }
                else
                {
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(IcWhstransH, "TransVouch");
                    SqlHelper.ExecuteNonQuery(str.ToString());
                    msg = "转库单表更新成功";
                }
                tableExist = "if object_id( 'TransVouchs') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table TransVouchs(ID nvarchar(30),autoid nvarchar(30)  primary key not null,doclineno nvarchar(50),cinvcode nvarchar(50),cinvname nvarchar(500),cinvclass nvarchar(500),cinvstd nvarchar(500),cinvUnit  nvarchar(500),qty decimal(28,8),remark nvarchar(100))";
                    SqlHelper.ExecuteNonQuery(createSql);
                    StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(IcWhstransHLine, "TransVouchs");
                    SqlHelper.ExecuteNonQuery(strs.ToString());
                    msg = "转库单表行插入成功";
                }
                else
                {
                    StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(IcWhstransHLine, "TransVouchs");
                    SqlHelper.ExecuteNonQuery(strs.ToString());
                    msg = "转库单表行更新成功";
                }
                result = msg;
            }
            catch (Exception e)
            {

                result = "转库单表错误：" + e.Message;
            }
            return result;

        }


        /// <summary>
        /// 从形态转换单单数据插入到sql
        /// 创建人：lvhe
        /// 创建时间：2019-12-19 23:56:56
        /// </summary>
        /// <returns></returns>
        [WebMethod]
        private string GetIcTransformHToSql()
        {
            string result = "";
            string createSql = "";
            string tableExist = "";
            int existResult = 0;
            string msg = "";
            string sql = "";
            StringBuilder strbu = new StringBuilder();
            string strGetOracleSQLIn = "";
            try
            {
                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id( 'AssemVouch') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    //获取形态转换单单头数据
                    sql = " SELECT A.cspecialhid ID, A.vbillcode code, A.dbilldate ddate,A1.code cdepcode,A.vnote remark,0 isRed,A.modifiedtime ts FROM ic_transform_h A left join org_dept_v A1 on A1.pk_vid = A.cdptvid where  not exists (select cspecialhid from (select distinct pob.cspecialhid cspecialhid from ic_transform_b pob left join bd_material mat on mat.pk_material = pob.cmaterialvid and nvl(mat.dr,0)=0 left join ic_transform_h poh on poh.cspecialhid = pob.cspecialhid and nvl(poh.dr,0)=0 where  nvl(pob.dr,0)=0 and pob.cbodywarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY') and substr(mat.code,0,4) = '0915' and poh.PK_ORG='0001A110000000001V70'  AND substr(poh.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "') po  where po.cspecialhid = A.cspecialhid) and  A.PK_ORG='0001A110000000001V70'  AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' ";
                }
                else
                {
                    string delstr = "delete from AssemVouch where id in(select ID from AssemVouch where zt != 1 and ts is not null and ts !='')";
                    string delstr2 = "delete from AssemVouchs where id in(select ID from AssemVouch where zt != 1 and ts is not null and ts !='')";
                    SqlHelper.ExecuteNonQuerys(delstr);
                    SqlHelper.ExecuteNonQuerys(delstr2);
                    string str = "select id from AssemVouch";
                    DataSet ds = SqlHelper.ExecuteDataset(connectionString, CommandType.Text, str);
                    foreach (DataRow dr in ds.Tables[0].Rows)
                    {
                        strbu.Append(dr["id"].ToString() + ",");
                    }
                    strbu = strbu.Remove(strbu.Length - 1, 1);
                    String[] ids = strbu.ToString().Split(',');
                    strGetOracleSQLIn = getOracleSQLIn(ids, "A.cspecialhid");
                    //获取形态转换单单头数据
                    sql = " SELECT A.cspecialhid ID, A.vbillcode code, A.dbilldate ddate,A1.code cdepcode,A.vnote remark,0 isRed,A.modifiedtime ts FROM ic_transform_h A left join org_dept_v A1 on A1.pk_vid = A.cdptvid where  not exists (select cspecialhid from (select distinct pob.cspecialhid cspecialhid from ic_transform_b pob left join bd_material mat on mat.pk_material = pob.cmaterialvid and nvl(mat.dr,0)=0 left join ic_transform_h poh on poh.cspecialhid = pob.cspecialhid and nvl(poh.dr,0)=0 where  nvl(pob.dr,0)=0 and pob.cbodywarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY') and substr(mat.code,0,4) = '0915' and poh.PK_ORG='0001A110000000001V70'  AND substr(poh.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "') po  where po.cspecialhid = A.cspecialhid) and  A.PK_ORG='0001A110000000001V70'  AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and " + strGetOracleSQLIn + "";
                }
                DataSet IcTransformH = OracleHelper.ExecuteDataset(sql);

                tableExist = "if object_id( 'AssemVouchs') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    //获取形态转换单单行数据
                    sql = " SELECT A.cspecialhid ID,A1.cspecialbid autoid,CASE WHEN A1.fbillrowflag = 2 THEN '转换前' WHEN A1.fbillrowflag = 3 THEN '转换后' ELSE '' END AS bavtype,CASE WHEN (A1.cbeforebid is not null and A1.cbeforebid !='') THEN A1.cbeforebid  ELSE A1.cspecialbid END AS igroupno,A5.code cwhcode,A1.crowno doclineno,A2.code cinvcode, A2.name cinvname, A3.code cinvclass, A2.materialspec cinvstd,A4.code cinvUnit,NVL(A1.nassistnum,0) qty, A1.vnotebody remark FROM ic_transform_h A left join ic_transform_b A1 on A1.cspecialhid = A.cspecialhid and A1.dr != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc left join bd_stordoc A5 on A5.pk_stordoc = A1.cbodywarehouseid where A.PK_ORG = '0001A110000000001V70' AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and substr(A2.code,0,4) != '0915' and A5.code not in('1001A1100000000T5S5Z','1001A11000000003CYSY')";
                }
                else
                {
                    sql = " SELECT A.cspecialhid ID,A1.cspecialbid autoid,CASE WHEN A1.fbillrowflag = 2 THEN '转换前' WHEN A1.fbillrowflag = 3 THEN '转换后' ELSE '' END AS bavtype,CASE WHEN (A1.cbeforebid is not null and A1.cbeforebid !='') THEN A1.cbeforebid  ELSE A1.cspecialbid END AS igroupno,A5.code cwhcode,A1.crowno doclineno,A2.code cinvcode, A2.name cinvname, A3.code cinvclass, A2.materialspec cinvstd,A4.code cinvUnit,NVL(A1.nassistnum,0) qty, A1.vnotebody remark FROM ic_transform_h A left join ic_transform_b A1 on A1.cspecialhid = A.cspecialhid and A1.dr != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc left join bd_stordoc A5 on A5.pk_stordoc = A1.cbodywarehouseid where A.PK_ORG = '0001A110000000001V70' AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and substr(A2.code,0,4) != '0915' and A5.code not in('1001A1100000000T5S5Z','1001A11000000003CYSY') and " + strGetOracleSQLIn + "";
                }
                DataSet IcTransformHLine = OracleHelper.ExecuteDataset(sql);

                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id( 'AssemVouch') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table AssemVouch(ID nvarchar(30),code nvarchar(50),ddate nvarchar(20),cdepcode nvarchar(50),remark nvarchar(100),isRed bit default 0,ts nvarchar(50),zt bit default 0,memo text)";
                    SqlHelper.ExecuteNonQuery(createSql);
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(IcTransformH, "AssemVouch");
                    SqlHelper.ExecuteNonQuery(str.ToString());
                    msg = "形态转换表插入成功";
                }
                else
                {
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(IcTransformH, "AssemVouch");
                    SqlHelper.ExecuteNonQuery(str.ToString());
                    msg = "形态转换表更新成功";
                }
                tableExist = "if object_id( 'AssemVouchs') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table AssemVouchs(ID nvarchar(30),autoid nvarchar(30),bavtype nvarchar(50),igroupno nvarchar(50),cwhcode nvarchar(50),doclineno nvarchar(50),cinvcode nvarchar(50),cinvname nvarchar(500),cinvclass nvarchar(500),cinvstd nvarchar(500),cinvUnit  nvarchar(500),qty decimal(28,8),remark nvarchar(100))";
                    SqlHelper.ExecuteNonQuery(createSql);
                    StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(IcTransformHLine, "AssemVouchs");
                    SqlHelper.ExecuteNonQuery(strs.ToString());
                    msg = "形态转换表行插入成功";
                }
                else
                {
                    StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(IcTransformHLine, "AssemVouchs");
                    SqlHelper.ExecuteNonQuery(strs.ToString());
                    msg = "形态转换表行更新成功";
                }
                result = msg;
            }
            catch (Exception e)
            {

                result = "形态转换表错误：" + e.Message;
            }
            return result;

        }


        /// <summary>
        /// 处理 Oracle SQL in 超过1000 的解决方案
        /// 创建人：lvhe
        /// 创建时间：2019年12月31日 01:20:36
        /// </summary>
        /// <param name="sqlParam"></param>
        /// <param name="columnName"></param>
        /// <returns></returns>
        private string getOracleSQLIn(string[] ids, string field)
        {
            int count = Math.Min(ids.Length, 1000);
            int len = ids.Length;
            int size = len % count;
            if (size == 0)
            {
                size = len / count;
            }
            else
            {
                size = (len / count) + 1;
            }
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < size; i++)
            {
                int fromIndex = i * count;
                int toIndex = Math.Min(fromIndex + count, len);
                string productId = string.Join("','", getArrayValues(fromIndex, toIndex, ids).ToArray());
                if (i != 0)
                {
                    builder.Append(" and ");
                }
                builder.Append(field).Append(" not in ('").Append(productId).Append("')");
            }
            return builder.ToString();
        }


        /// <summary>
        /// 处理 Oracle SQL in 超过1000 的解决方案
        /// 创建人：lvhe
        /// 创建时间：2019年12月31日 01:20:36
        /// </summary>
        /// <param name="sqlParam"></param>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public List<string> getArrayValues(int fromindex, int toindex, string[] array)
        {
            List<string> listret = new List<string>();
            for (int i = fromindex; i < toindex; i++)
            {
                listret.Add(array[i]);
            }
            return listret;
        }
    }
}
