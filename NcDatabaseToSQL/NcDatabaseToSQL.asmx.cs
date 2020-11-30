using ERP8.Common;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Web;
using System.Web.Services;

namespace NcDatabaseToSQL
{

    //git管理测试
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

        private static string connNCSubjectEncoding = ConfigurationManager.ConnectionStrings["NCSubjectEncoding"].ToString();

        private static string u8SVApiUrl = ConfigurationManager.ConnectionStrings["U8SVApiUrl"].ToString();
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
                case "wlqd":
                    msg = GetBomToSql();
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
                case "pzb":
                    msg = GetGlVoucherToSql();
                    break;
                case "wuliao":
                    msg = GetInventoryToSql();
                    break;
                case "gys":
                    msg = GetSupplierToSql();
                    break;
                case "kehu":
                    msg = GetCustomerToSql();
                    break;
                case "djsh":
                    msg = DoApprove();
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
            int updateCount = 0;
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
                    string delstr = "delete from PurBillVouch where id in(select ID from PurBillVouch where zt != 1 )";
                    string delstr2 = "delete from PurBillVouchs where id in(select ID from PurBillVouch where zt != 1 )";
                    SqlHelper.ExecuteNonQuerys(delstr2);
                    SqlHelper.ExecuteNonQuerys(delstr);
                    string str = "select id from PurBillVouch";
                    DataSet ds = SqlHelper.ExecuteDataset(connectionString, CommandType.Text, str);
                    if (ds.Tables[0].Rows.Count > 0)
                    {
                        updateCount = ds.Tables[0].Rows.Count;
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
                    else
                    {
                        updateCount = 0;
                        sql = " select A.PK_INVOICE ID,A.vbillcode code,A.DBILLDATE ddate,A.darrivedate darrivedate,A1.code cvencode,A2.CODE cdepcode,CASE WHEN A.ntotalastnum>0 THEN 0 ELSE 1 END AS isRed,A.VMEMO remark,A.modifiedtime ts from po_invoice A left join org_dept A2 on A2.PK_DEPT=A.PK_DEPT  and nvl(A2.dr,0)=0 left join bd_supplier A1 on A.PK_SUPPLIER=A1.PK_SUPPLIER and nvl(A1.dr,0)=0 where  not exists (select pk_invoice from (select distinct pob.pk_invoice pk_invoice from po_invoice_b pob left join bd_material mat on mat.pk_material = pob.Pk_material and nvl(mat.dr,0)=0 left join po_invoice poh on poh.PK_INVOICE = pob.PK_INVOICE and nvl(poh.dr,0)=0 where  nvl(pob.dr,0)=0 and substr(mat.code,0,4) = '0915' and poh.PK_ORG='0001A110000000001V70'  AND substr(poh.taudittime,0,10) between '" + startTime + "' and '" + endTime + "' and poh.fbillstatus=3) po  where po.pk_invoice = A.pk_invoice) and  A.PK_ORG='0001A110000000001V70'  AND substr(A.taudittime,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillstatus=3";
                    }
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
                    if (updateCount > 0)
                    {
                        sql = "select A.PK_INVOICE ID,A1.pk_invoice_b autoid,A1.vsourcecode srcdocno,A1.crowno doclineno,A1.vsourcerowno srcdoclineno,A2.code cinvcode,NVL(A1.nastnum ,0) qty,A1.ntaxrate itaxrate, A1.nastorigtaxprice iOriTaxCost, A1.nastorigprice iOriCost, A1.norigtaxmny ioriSum,A1.nnosubtax iOriTaxPrice, A1.norigmny iOriMoney, A1.ntaxmny isum, A1.nmny iMoney, A1.ntax iTaxPrice,A1.nastorigtaxprice iUnitCost, A1.vmemob remark from po_invoice A left join po_invoice_b A1 on A.PK_INVOICE = A1.PK_INVOICE and A1.DR!=1  left join bd_material A2 on A1.pk_material = A2.pk_material where A.PK_ORG = '0001A110000000001V70' and A.DR != 1 AND substr(A.taudittime,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillstatus = 3 and substr(A2.code,0,4) != '0915' and " + strGetOracleSQLIn + "";
                    }
                    else
                    {
                        sql = "select A.PK_INVOICE ID,A1.pk_invoice_b autoid,A1.vsourcecode srcdocno,A1.crowno doclineno,A1.vsourcerowno srcdoclineno,A2.code cinvcode,NVL(A1.nastnum ,0) qty,A1.ntaxrate itaxrate, A1.nastorigtaxprice iOriTaxCost, A1.nastorigprice iOriCost, A1.norigtaxmny ioriSum,A1.nnosubtax iOriTaxPrice, A1.norigmny iOriMoney, A1.ntaxmny isum, A1.nmny iMoney, A1.ntax iTaxPrice,A1.nastorigtaxprice iUnitCost, A1.vmemob remark from po_invoice A left join po_invoice_b A1 on A.PK_INVOICE = A1.PK_INVOICE and A1.DR!=1  left join bd_material A2 on A1.pk_material = A2.pk_material where A.PK_ORG = '0001A110000000001V70' and A.DR != 1 AND substr(A.taudittime,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillstatus = 3 and substr(A2.code,0,4) != '0915'";
                    }
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
                    if (!string.IsNullOrEmpty(str.ToString()))
                    {
                        SqlHelper.ExecuteNonQuery(str.ToString());
                        msg = "采购发票表更新成功";
                    }
                    else
                    {
                        msg = "采购发票表暂无无可更新数据";
                    }
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
                    if (!string.IsNullOrEmpty(strs.ToString()))
                    {
                        SqlHelper.ExecuteNonQuery(strs.ToString());
                        msg = "采购发票表行更新成功";
                    }
                    else
                    {
                        msg = "采购发票表暂无可更新数据";
                    }
                }
                GetU8SVApiUrlApi("cgfpapi");
                result = msg;
            }
            catch (Exception e)
            {

                result = "采购发票表行错误：" + e.Message;
            }
            return result;

        }


        /// <summary>
        /// 从nc获取bom数据插入到sql
        /// 创建人：lvhe
        /// 创建时间：2019年10月13日 22:59:30
        /// </summary>
        /// <returns></returns>
        [WebMethod]
        private string GetBomToSql()
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
            int updateCount = 0;
            try
            {
                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id( 'Bom') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    //获取采购发票头数据
                    sql = "select distinct A.cbomid ID,A2.code cinvcode,A2.name cinvname, A3.code cinvclass,A3.name cinvclassname,A2.materialspec cinvstd, A4.code cinvUnit,A.creationtime ddate,A.hversion bomversion,A.Hvdef1 bomversionexplain,A.DR dr,A.modifiedtime ts from bd_bom A left join bd_bom_b A1 on A.cbomid=A1.cbomid left join bd_material A2 on A2.pk_material=A.hcmaterialid left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc  where A.PK_ORG='0001A110000000001V70' and A.hversion not in('1.0','1.1')";
                }
                else
                {
                    string delstr = "delete from Bom where id in(select ID from Bom where zt != 1 )";
                    string delstr2 = "delete from Boms where id in(select ID from Bom where zt != 1 )";
                    SqlHelper.ExecuteNonQuerys(delstr2);
                    SqlHelper.ExecuteNonQuerys(delstr);
                    string str = "select id from Bom";
                    DataSet ds = SqlHelper.ExecuteDataset(connectionString, CommandType.Text, str);
                    if (ds.Tables[0].Rows.Count > 0)
                    {
                        updateCount = ds.Tables[0].Rows.Count;
                        foreach (DataRow dr in ds.Tables[0].Rows)
                        {
                            strbu.Append(dr["id"].ToString() + ",");
                        }
                        strbu = strbu.Remove(strbu.Length - 1, 1);
                        String[] ids = strbu.ToString().Split(',');
                        strGetOracleSQLIn = getOracleSQLIn(ids, "A.cbomid");
                        //获取采购发票头数据
                        sql = "select distinct A.cbomid ID,A2.code cinvcode,A2.name cinvname, A3.code cinvclass,A3.name cinvclassname,A2.materialspec cinvstd, A4.code cinvUnit,A.creationtime ddate,A.hversion bomversion,A.Hvdef1 bomversionexplain,A.DR dr,A.modifiedtime ts from bd_bom A left join bd_bom_b A1 on A.cbomid=A1.cbomid left join bd_material A2 on A2.pk_material=A.hcmaterialid left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc  where A.PK_ORG='0001A110000000001V70' and A.hversion not in('1.0','1.1') and " + strGetOracleSQLIn + "";
                    }
                    else
                    {
                        updateCount = 0;
                        sql = "select distinct A.cbomid ID,A2.code cinvcode,A2.name cinvname, A3.code cinvclass,A3.name cinvclassname,A2.materialspec cinvstd, A4.code cinvUnit,A.creationtime ddate,A.hversion bomversion,A.Hvdef1 bomversionexplain,A.DR dr,A.modifiedtime ts from bd_bom A left join bd_bom_b A1 on A.cbomid=A1.cbomid left join bd_material A2 on A2.pk_material=A.hcmaterialid left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc  where A.PK_ORG='0001A110000000001V70' and A.hversion not in('1.0','1.1')";
                    }
                }
                DataSet Bom = OracleHelper.ExecuteDataset(sql);

                //获取采购发票行数据
                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id( 'Boms') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    sql = "select A.cbomid ID,A1.cbom_bid autoid,A1.vrowno doclineno,A2.code cinvcode,A2.name cinvname, A3.code cinvclass,A3.name cinvclassname,A2.materialspec cinvstd, A4.code cinvUnit,A1.nassitemnum inassitemnum,A1.ibasenum ibasenum,A1.DR dr,case when A1.bdeliver='Y' then '是' else '否' end ibdeliver,case when A1.fsupplymode=1 then '一般发料' else '定量发料' end ifsupplymode,case when A1.fbackflushtype=1 then '不倒冲' when A1.fbackflushtype=2 then '自动倒冲' else '交互式倒冲' end ifbackflushtype,case when A1.fbackflushtime=1 then '产品完工' else '工序完工' end ifbackflushtime,case when A1.bbchkitemforwr='Y' then '是' else '否' end ibbchkitemforwr,A1.cbeginperiod icbeginperiod,A1.cendperiod icendperiod,A1.vdef3 ideptcode,case when A1.vdef4='Y' then '是' else '否' end isxxjs from bd_bom A left join bd_bom_b A1 on A.cbomid=A1.cbomid left join bd_material A2 on A2.pk_material=A1.cmaterialid  left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc  where A.PK_ORG='0001A110000000001V70' and A.hversion not in('1.0','1.1') ";
                }
                else
                {
                    if (updateCount > 0)
                    {
                        sql = "select A.cbomid ID,A1.cbom_bid autoid,A1.vrowno doclineno,A2.code cinvcode,A2.name cinvname, A3.code cinvclass,A3.name cinvclassname,A2.materialspec cinvstd, A4.code cinvUnit,A1.nassitemnum inassitemnum,A1.ibasenum ibasenum,A1.DR dr,case when A1.bdeliver='Y' then '是' else '否' end ibdeliver,case when A1.fsupplymode=1 then '一般发料' else '定量发料' end ifsupplymode,case when A1.fbackflushtype=1 then '不倒冲' when A1.fbackflushtype=2 then '自动倒冲' else '交互式倒冲' end ifbackflushtype,case when A1.fbackflushtime=1 then '产品完工' else '工序完工' end ifbackflushtime,case when A1.bbchkitemforwr='Y' then '是' else '否' end ibbchkitemforwr,A1.cbeginperiod icbeginperiod,A1.cendperiod icendperiod,A1.vdef3 ideptcode,case when A1.vdef4='Y' then '是' else '否' end isxxjs from bd_bom A left join bd_bom_b A1 on A.cbomid=A1.cbomid left join bd_material A2 on A2.pk_material=A1.cmaterialid  left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc  where A.PK_ORG='0001A110000000001V70' and A.hversion not in('1.0','1.1') AND " + strGetOracleSQLIn + "";
                    }
                    else
                    {
                        sql = "select A.cbomid ID,A1.cbom_bid autoid,A1.vrowno doclineno,A2.code cinvcode,A2.name cinvname, A3.code cinvclass,A3.name cinvclassname,A2.materialspec cinvstd, A4.code cinvUnit,A1.nassitemnum inassitemnum,A1.ibasenum ibasenum,A1.DR dr,case when A1.bdeliver='Y' then '是' else '否' end ibdeliver,case when A1.fsupplymode=1 then '一般发料' else '定量发料' end ifsupplymode,case when A1.fbackflushtype=1 then '不倒冲' when A1.fbackflushtype=2 then '自动倒冲' else '交互式倒冲' end ifbackflushtype,case when A1.fbackflushtime=1 then '产品完工' else '工序完工' end ifbackflushtime,case when A1.bbchkitemforwr='Y' then '是' else '否' end ibbchkitemforwr,A1.cbeginperiod icbeginperiod,A1.cendperiod icendperiod,A1.vdef3 ideptcode,case when A1.vdef4='Y' then '是' else '否' end isxxjs from bd_bom A left join bd_bom_b A1 on A.cbomid=A1.cbomid left join bd_material A2 on A2.pk_material=A1.cmaterialid  left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc  where A.PK_ORG='0001A110000000001V70' and A.hversion not in('1.0','1.1')";
                    }
                }
                DataSet Boms = OracleHelper.ExecuteDataset(sql);

                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id( 'Bom') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table Bom(ID nvarchar(30) primary key not null,cinvcode nvarchar(50),cinvname nvarchar(500),cinvclass nvarchar(500),cinvclassname nvarchar(500),cinvstd nvarchar(500),cinvUnit  nvarchar(500),ddate nvarchar(20),bomversion nvarchar(50),bomversionexplain nvarchar(50),remark nvarchar(100),ts nvarchar(50),dr nvarchar(50),zt bit default 0,memo text)";
                    SqlHelper.ExecuteNonQuery(createSql);
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(Bom, "Bom");
                    SqlHelper.ExecuteNonQuery(str.ToString());
                    msg = "物料清单表插入成功";
                }
                else
                {
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(Bom, "Bom");
                    if (!string.IsNullOrEmpty(str.ToString()))
                    {
                        SqlHelper.ExecuteNonQuery(str.ToString());
                        msg = "物料清单表更新成功";
                    }
                    else
                    {
                        msg = "物料清单表暂无无可更新数据";
                    }
                }
                tableExist = "if object_id( 'Boms') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table Boms(ID nvarchar(30),autoid nvarchar(30) primary key not null,doclineno nvarchar(30),cinvcode nvarchar(50),cinvname nvarchar(500),cinvclass nvarchar(500),cinvclassname nvarchar(500),cinvstd nvarchar(500),cinvUnit  nvarchar(500),inassitemnum decimal(28, 8),ibasenum decimal(28, 8),ibdeliver nvarchar(50),ifsupplymode nvarchar(50),ifbackflushtype nvarchar(50),ifbackflushtime nvarchar(50),ibbchkitemforwr nvarchar(50),icbeginperiod nvarchar(50),icendperiod nvarchar(50),ideptcode nvarchar(50),isxxjs nvarchar(50),dr nvarchar(50))";
                    SqlHelper.ExecuteNonQuery(createSql);
                    StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(Boms, "Boms");
                    SqlHelper.ExecuteNonQuery(strs.ToString());
                    msg = "物料清单表行插入成功";
                }
                else
                {
                    StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(Boms, "Boms");
                    if (!string.IsNullOrEmpty(strs.ToString()))
                    {
                        SqlHelper.ExecuteNonQuery(strs.ToString());
                        msg = "物料清单表行更新成功";
                    }
                    else
                    {
                        msg = "物料清单表暂无可更新数据";
                    }
                }
                GetU8SVApiUrlApi("wlqdApi");
                result = msg;
            }
            catch (Exception e)
            {

                result = "物料清单表行错误：" + e.Message;
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
            int updateCount = 0;
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
                    string delstr = "delete from SaleBillVouch where id in(select ID from SaleBillVouch where zt != 1 )";
                    string delstr2 = "delete from SaleBillVouchs where id in(select ID from SaleBillVouch where zt != 1 )";
                    SqlHelper.ExecuteNonQuerys(delstr2);
                    SqlHelper.ExecuteNonQuerys(delstr);
                    string str = "select id from SaleBillVouch";
                    DataSet ds = SqlHelper.ExecuteDataset(connectionString, CommandType.Text, str);
                    if (ds.Tables[0].Rows.Count > 0)
                    {
                        updateCount = ds.Tables[0].Rows.Count;
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
                    else
                    {
                        updateCount = 0;
                        sql = " select A.csaleinvoiceid ID,A.vbillcode code,A.dbilldate ddate,A1.Code custcode,A.vnote remark,CASE WHEN A.ntotalastnum>0 THEN 0 ELSE 1 END AS isRed,A.modifiedtime ts from so_saleinvoice A left join bd_customer A1 on A.cinvoicecustid = A1.pk_customer where  not exists (select csaleinvoiceid from (select distinct pob.csaleinvoiceid csaleinvoiceid from so_saleinvoice_b pob left join bd_material mat on mat.pk_material = pob.cmaterialvid and nvl(mat.dr,0)=0 left join so_saleinvoice poh on poh.csaleinvoiceid = pob.csaleinvoiceid and nvl(poh.dr,0)=0 where  nvl(pob.dr,0)=0  and substr(mat.code,0,4) = '0915' and poh.PK_ORG='0001A110000000001V70'  and poh.dr != 1 AND substr(poh.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and pob.csendstordocid not in('1001A1100000000T5S5Z','1001A11000000003CYSY')) po  where po.csaleinvoiceid = A.csaleinvoiceid) and  A.PK_ORG='0001A110000000001V70' and A.dr != 1  AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "'";
                    }
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
                    if (updateCount > 0)
                    {
                        //获取销售发票行数据
                        sql = "select A.csaleinvoiceid ID,A1.csaleinvoicebid autoid,A1.crowno doclineno,A1.vsrccode srcdocno, A1.vsrcrowno srcdoclineno, A2.code cinvcode, NVL(A1.nastnum ,0) qty,A1.ntaxrate itaxrate, A1.nqtorigtaxprice iOriTaxCost, A1.nqtorigprice iOriCost,A1.norigtaxmny ioriSum, A1.ntax iOriTaxPrice, A1.norigmny iOriMoney, A1.ntaxmny isum,A1.nmny iMoney,(A1.ntaxmny - A1.nmny) iTaxPrice,A1.nqttaxprice iUnitCost, A1.vrownote remark,A1.csendstordocid cwhcode from so_saleinvoice A left join so_saleinvoice_b A1 on A.csaleinvoiceid = A1.csaleinvoiceid and A1.dr!=1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material where A.PK_ORG = '0001A110000000001V70' and A.dr != 1 AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and substr(A2.code,0,4) != '0915' and A1.csendstordocid not in('1001A1100000000T5S5Z','1001A11000000003CYSY') and  " + strGetOracleSQLIn + "";
                    }
                    else
                    {
                        sql = "select A.csaleinvoiceid ID,A1.csaleinvoicebid autoid,A1.crowno doclineno,A1.vsrccode srcdocno, A1.vsrcrowno srcdoclineno, A2.code cinvcode, NVL(A1.nastnum ,0) qty,A1.ntaxrate itaxrate, A1.nqtorigtaxprice iOriTaxCost, A1.nqtorigprice iOriCost,A1.norigtaxmny ioriSum, A1.ntax iOriTaxPrice, A1.norigmny iOriMoney, A1.ntaxmny isum,A1.nmny iMoney,(A1.ntaxmny - A1.nmny) iTaxPrice,A1.nqttaxprice iUnitCost, A1.vrownote remark,A1.csendstordocid cwhcode from so_saleinvoice A left join so_saleinvoice_b A1 on A.csaleinvoiceid = A1.csaleinvoiceid and A1.dr!=1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material where A.PK_ORG = '0001A110000000001V70' and A.dr != 1 AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and substr(A2.code,0,4) != '0915' and A1.csendstordocid not in('1001A1100000000T5S5Z','1001A11000000003CYSY')";
                    }
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
                    if (!string.IsNullOrEmpty(str.ToString()))
                    {
                        SqlHelper.ExecuteNonQuery(str.ToString());
                        msg = "销售发票表更新成功";
                    }
                    else
                    {
                        msg = "销售发票表暂无可更新数据";
                    }
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
                    if (!string.IsNullOrEmpty(strs.ToString()))
                    {
                        SqlHelper.ExecuteNonQuery(strs.ToString());
                        msg = "销售发票表行更新成功";
                    }
                    else
                    {
                        msg = "销售发票表暂无可更新数据";
                    }
                }
                GetU8SVApiUrlApi("xsfpapi");
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
            int updateCount = 0;
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
                    string delstr = "delete from Rdrecord01 where id in(select ID from Rdrecord01 where zt != 1 )";
                    string delstr2 = "delete from Rdrecords01 where id in(select ID from Rdrecord01 where zt != 1 )";
                    SqlHelper.ExecuteNonQuerys(delstr2);
                    SqlHelper.ExecuteNonQuerys(delstr);
                    string str = "select id from Rdrecord01";
                    DataSet ds = SqlHelper.ExecuteDataset(connectionString, CommandType.Text, str);
                    if (ds.Tables[0].Rows.Count > 0)
                    {
                        updateCount = ds.Tables[0].Rows.Count;
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
                    else
                    {
                        updateCount = 0;
                        sql = " select A.cgeneralhid ID, A.vbillcode code, A.dbilldate ddate, A2.code cwhcode, A3.code cvencode,A4.code cvenclass,A3.name cvenname,A1.CODE cdepcode,CASE WHEN A.freplenishflag='N' THEN 0 ELSE 1 END AS isRed,A.vnote remark, A.modifiedtime ts FROM ic_purchasein_h A left join org_dept A1 on A1.pk_dept = A.cdptid left join bd_stordoc A2 on A2.pk_stordoc = A.cwarehouseid left join bd_supplier A3 on A3.pk_supplier = A.cvendorid left join bd_supplierclass A4 on A4.pk_supplierclass=A3.pk_supplierclass where  not exists (select cgeneralhid from (select distinct pob.cgeneralhid cgeneralhid from ic_purchasein_b pob left join bd_material mat on mat.pk_material = pob.cmaterialvid and nvl(mat.dr,0)=0 left join ic_purchasein_h poh on poh.cgeneralhid = pob.cgeneralhid and nvl(poh.dr,0)=0 where  nvl(pob.dr,0)=0 and substr(mat.code,0,4) = '0915' and poh.PK_ORG='0001A110000000001V70'  AND substr(poh.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and poh.fbillflag=3) po  where po.cgeneralhid = A.cgeneralhid) and  A.PK_ORG='0001A110000000001V70' and A.dr!=1 and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY') AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag=3";
                    }
                }
                //获取采购入库头数据
                DataSet PurchaseIn = OracleHelper.ExecuteDataset(sql);


                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id( 'Rdrecord01') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    //获取采购入库行数据
                    sql = "SELECT A.cgeneralhid ID,A1.cgeneralbid autoid,A1.crowno doclineno,A2.code cinvcode,A2.name cinvname,A3.code cinvclass,A3.name cinvclassname,A2.materialspec cinvstd,A4.code cinvUnit,NVL(A1.nassistnum ,0) qty, A1.ntaxrate itaxrate, A1.nqtorigtaxprice iOriTaxCost, A1.nqtorigprice iOriCost,A1.norigtaxmny ioriSum, A1.ntax iOriTaxPrice, A1.norigmny iOriMoney, A1.ntaxmny isum,A1.nmny iMoney, A1.ntax iTaxPrice, A1.nqtprice iUnitCost,A1.vnotebody remark FROM ic_purchasein_h A left join ic_purchasein_b A1 on A1.cgeneralhid = A.cgeneralhid and A1.dr!=1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc where A.PK_ORG = '0001A110000000001V70' AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag = 3 and substr(A2.code,0,4) != '0915' and A.cwarehouseid  not in('1001A1100000000T5S5Z','1001A11000000003CYSY')";
                }
                else
                {
                    if (updateCount > 0)
                    {
                        //获取采购入库行数据
                        sql = "SELECT A.cgeneralhid ID,A1.cgeneralbid autoid,A1.crowno doclineno,A2.code cinvcode,A2.name cinvname,A3.code cinvclass,A3.name cinvclassname,A2.materialspec cinvstd,A4.code cinvUnit,NVL(A1.nassistnum ,0) qty, A1.ntaxrate itaxrate, A1.nqtorigtaxprice iOriTaxCost, A1.nqtorigprice iOriCost,A1.norigtaxmny ioriSum, A1.ntax iOriTaxPrice, A1.norigmny iOriMoney, A1.ntaxmny isum,A1.nmny iMoney, A1.ntax iTaxPrice, A1.nqtprice iUnitCost,A1.vnotebody remark FROM ic_purchasein_h A left join ic_purchasein_b A1 on A1.cgeneralhid = A.cgeneralhid and A1.dr!=1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc where A.PK_ORG = '0001A110000000001V70' AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag = 3 and substr(A2.code,0,4) != '0915' and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY') and " + strGetOracleSQLIn + "";
                    }
                    else
                    {
                        sql = "SELECT A.cgeneralhid ID,A1.cgeneralbid autoid,A1.crowno doclineno,A2.code cinvcode,A2.name cinvname,A3.code cinvclass,A3.name cinvclassname,A2.materialspec cinvstd,A4.code cinvUnit,NVL(A1.nassistnum ,0) qty, A1.ntaxrate itaxrate, A1.nqtorigtaxprice iOriTaxCost, A1.nqtorigprice iOriCost,A1.norigtaxmny ioriSum, A1.ntax iOriTaxPrice, A1.norigmny iOriMoney, A1.ntaxmny isum,A1.nmny iMoney, A1.ntax iTaxPrice, A1.nqtprice iUnitCost,A1.vnotebody remark FROM ic_purchasein_h A left join ic_purchasein_b A1 on A1.cgeneralhid = A.cgeneralhid and A1.dr!=1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc where A.PK_ORG = '0001A110000000001V70' AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag = 3 and substr(A2.code,0,4) != '0915' and A.cwarehouseid  not in('1001A1100000000T5S5Z','1001A11000000003CYSY')";
                    }
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
                    if (!string.IsNullOrEmpty(str.ToString()))
                    {
                        SqlHelper.ExecuteNonQuery(str.ToString());
                        msg = "采购入库表更新成功";
                    }
                    else
                    {
                        msg = "采购入库表暂无可更新数据";
                    }
                }
                tableExist = "if object_id( 'Rdrecords01') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table Rdrecords01(ID nvarchar(30),autoid nvarchar(30)  primary key not null,doclineno bigint,cinvcode nvarchar(50),cinvname nvarchar(500),cinvclass nvarchar(500),cinvclassname nvarchar(500),cinvstd nvarchar(500),cinvUnit nvarchar(500),qty decimal(28, 8),itaxrate decimal(28, 8),iOriTaxCost decimal(28, 8),iOriCost decimal(28, 8),ioriSum decimal(28, 8),iOriTaxPrice decimal(28, 8),iOriMoney decimal(28, 8),isum decimal(28, 8),iMoney decimal(28, 8),iTaxPrice decimal(28, 8),iUnitCost decimal(28, 8),remark nvarchar(200))";
                    SqlHelper.ExecuteNonQuery(createSql);
                    StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(PurchaseInLine, "Rdrecords01");
                    SqlHelper.ExecuteNonQuery(strs.ToString());
                    msg = "采购入库表行插入成功";
                }
                else
                {
                    StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(PurchaseInLine, "Rdrecords01");
                    if (!string.IsNullOrEmpty(strs.ToString()))
                    {
                        SqlHelper.ExecuteNonQuery(strs.ToString());
                        msg = "采购入库表行更新成功";
                    }
                    else
                    {
                        msg = "采购入库表暂无可更新数据";
                    }
                }
                GetU8SVApiUrlApi("cgrkapi");
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
            int updateCount = 0;
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
                    sql = "select A.cgeneralhid ID,A.vbillcode code,A.dbilldate ddate,A1.Code cwhcode,A5.pk_billtypecode cstcode, A2.code custcode,A2.name custname,A3.code cdepcode,A5.code cCCCode, A.vnote remark,CASE WHEN A.ntotalnum > 0 THEN 0 ELSE 1 END AS isRed,A.modifiedtime ts  from ic_saleout_h A left join bd_stordoc A1 on A.cwarehouseid = A1.Pk_Stordoc left join bd_customer A2 on A.ccustomerid = A2.pk_customer left join org_dept A3 on A3.PK_DEPT = A.cdptid left join bd_customer A4 on A4.pk_customer=A.ccustomerid left join bd_custclass A5 on A5.pk_custclass=A4.pk_custclass left join (select cgeneralhid,csourcetranstype from ic_saleout_b group by cgeneralhid,csourcetranstype) A4 on A.cgeneralhid=A4.cgeneralhid left join bd_billtype A5 on A5.pk_billtypeid=A4.csourcetranstype where  not exists (select cgeneralhid from (select distinct pob.cgeneralhid cgeneralhid from ic_saleout_b pob left join bd_material mat on mat.pk_material = pob.cmaterialvid and nvl(mat.dr,0)=0 left join ic_saleout_h poh on poh.cgeneralhid = pob.cgeneralhid and nvl(poh.dr,0)=0 where  nvl(pob.dr,0)=0 and substr(mat.code,0,4) = '0915' and poh.PK_ORG='0001A110000000001V70'  AND substr(poh.taudittime,0,10) between '" + startTime + "' and '" + endTime + "' and poh.fbillflag=3) po  where po.cgeneralhid = A.cgeneralhid) and  A.PK_ORG='0001A110000000001V70'  AND substr(A.taudittime,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag=3 and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY')";
                }
                else
                {
                    string delstr = "delete from DispatchList where id in(select ID from DispatchList where zt != 1 )";
                    string delstr2 = "delete from DispatchLists where id in(select ID from DispatchList where zt != 1 )";
                    SqlHelper.ExecuteNonQuerys(delstr2);
                    SqlHelper.ExecuteNonQuerys(delstr);
                    string str = "select id from DispatchList";
                    DataSet ds = SqlHelper.ExecuteDataset(connectionString, CommandType.Text, str);

                    if (ds.Tables[0].Rows.Count > 0)
                    {
                        updateCount = ds.Tables[0].Rows.Count;
                        foreach (DataRow dr in ds.Tables[0].Rows)
                        {
                            strbu.Append(dr["id"].ToString() + ",");
                        }
                        strbu = strbu.Remove(strbu.Length - 1, 1);
                        String[] ids = strbu.ToString().Split(',');
                        strGetOracleSQLIn = getOracleSQLIn(ids, "A.cgeneralhid");
                        //获取采购入库头数据
                        sql = "select A.cgeneralhid ID,A.vbillcode code,A.dbilldate ddate,A1.Code cwhcode,A5.pk_billtypecode cstcode, A2.code custcode,A2.name custname,A3.code cdepcode,A5.code cCCCode, A.vnote remark,CASE WHEN A.ntotalnum > 0 THEN 0 ELSE 1 END AS isRed,A.modifiedtime ts  from ic_saleout_h A left join bd_stordoc A1 on A.cwarehouseid = A1.Pk_Stordoc left join bd_customer A2 on A.ccustomerid = A2.pk_customer left join org_dept A3 on A3.PK_DEPT = A.cdptid left join bd_customer A4 on A4.pk_customer=A.ccustomerid left join bd_custclass A5 on A5.pk_custclass=A4.pk_custclass left join (select cgeneralhid,csourcetranstype from ic_saleout_b group by cgeneralhid,csourcetranstype) A4 on A.cgeneralhid=A4.cgeneralhid left join bd_billtype A5 on A5.pk_billtypeid=A4.csourcetranstype where  not exists (select cgeneralhid from (select distinct pob.cgeneralhid cgeneralhid from ic_saleout_b pob left join bd_material mat on mat.pk_material = pob.cmaterialvid and nvl(mat.dr,0)=0 left join ic_saleout_h poh on poh.cgeneralhid = pob.cgeneralhid and nvl(poh.dr,0)=0 where  nvl(pob.dr,0)=0 and substr(mat.code,0,4) = '0915' and poh.PK_ORG='0001A110000000001V70'  AND substr(poh.taudittime,0,10) between '" + startTime + "' and '" + endTime + "' and poh.fbillflag=3) po  where po.cgeneralhid = A.cgeneralhid) and  A.PK_ORG='0001A110000000001V70'  AND substr(A.taudittime,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag=3 and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY') and " + strGetOracleSQLIn + "";

                    }
                    else
                    {
                        updateCount = 0;
                        sql = "select A.cgeneralhid ID,A.vbillcode code,A.dbilldate ddate,A1.Code cwhcode,A5.pk_billtypecode cstcode, A2.code custcode,A2.name custname,A3.code cdepcode,A5.code cCCCode, A.vnote remark,CASE WHEN A.ntotalnum > 0 THEN 0 ELSE 1 END AS isRed,A.modifiedtime ts  from ic_saleout_h A left join bd_stordoc A1 on A.cwarehouseid = A1.Pk_Stordoc left join bd_customer A2 on A.ccustomerid = A2.pk_customer left join org_dept A3 on A3.PK_DEPT = A.cdptid left join bd_customer A4 on A4.pk_customer=A.ccustomerid left join bd_custclass A5 on A5.pk_custclass=A4.pk_custclass left join (select cgeneralhid,csourcetranstype from ic_saleout_b group by cgeneralhid,csourcetranstype) A4 on A.cgeneralhid=A4.cgeneralhid left join bd_billtype A5 on A5.pk_billtypeid=A4.csourcetranstype where  not exists (select cgeneralhid from (select distinct pob.cgeneralhid cgeneralhid from ic_saleout_b pob left join bd_material mat on mat.pk_material = pob.cmaterialvid and nvl(mat.dr,0)=0 left join ic_saleout_h poh on poh.cgeneralhid = pob.cgeneralhid and nvl(poh.dr,0)=0 where  nvl(pob.dr,0)=0 and substr(mat.code,0,4) = '0915' and poh.PK_ORG='0001A110000000001V70'  AND substr(poh.taudittime,0,10) between '" + startTime + "' and '" + endTime + "' and poh.fbillflag=3) po  where po.cgeneralhid = A.cgeneralhid) and  A.PK_ORG='0001A110000000001V70'  AND substr(A.taudittime,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag=3 and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY')";
                    }
                }
                DataSet SaleOut = OracleHelper.ExecuteDataset(sql);

                tableExist = "if object_id( 'DispatchLists') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    //获取销售出库行数据
                    sql = "select A1.cgeneralhid ID,A1.cgeneralbid autoid,A1.crowno doclineno,A2.code cinvcode,A2.name cinvname,A3.code cinvclass,A3.Name cinvclassname,A2.materialspec cinvstd,A4.code cinvUnit,NVL(A1.nassistnum ,0) qty, A1.ntaxrate itaxrate, A1.norigtaxprice iOriTaxCost,A1.nqtorigprice iOriCost,A1.norigtaxmny ioriSum, A1.norigmny iOriMoney, A1.ntaxmny isum, A1.nmny iMoney, NVL(A1.nqtprice,0) iUnitCost, A1.vnotebody remark from ic_saleout_h A left join ic_saleout_b A1 on A.cgeneralhid = A1.cgeneralhid and A1.DR != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc  where A.PK_ORG = '0001A110000000001V70' and A.DR != 1 AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag = 3 and substr(A2.code,0,4) != '0915' and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY')";
                }
                else
                {
                    if (updateCount > 0)
                    {
                        sql = "select A1.cgeneralhid ID,A1.cgeneralbid autoid,A1.crowno doclineno,A2.code cinvcode,A2.name cinvname,A3.code cinvclass,A3.Name cinvclassname,A2.materialspec cinvstd,A4.code cinvUnit,NVL(A1.nassistnum ,0) qty, A1.ntaxrate itaxrate, A1.norigtaxprice iOriTaxCost,A1.nqtorigprice iOriCost,A1.norigtaxmny ioriSum, A1.norigmny iOriMoney, A1.ntaxmny isum, A1.nmny iMoney, NVL(A1.nqtprice,0) iUnitCost, A1.vnotebody remark from ic_saleout_h A left join ic_saleout_b A1 on A.cgeneralhid = A1.cgeneralhid and A1.DR != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc  where A.PK_ORG = '0001A110000000001V70' and A.DR != 1 AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag = 3 and substr(A2.code,0,4) != '0915' and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY') and " + strGetOracleSQLIn + "";
                    }
                    else
                    {
                        sql = "select A1.cgeneralhid ID,A1.cgeneralbid autoid,A1.crowno doclineno,A2.code cinvcode,A2.name cinvname,A3.code cinvclass,A3.Name cinvclassname,A2.materialspec cinvstd,A4.code cinvUnit,NVL(A1.nassistnum ,0) qty, A1.ntaxrate itaxrate, A1.norigtaxprice iOriTaxCost,A1.nqtorigprice iOriCost,A1.norigtaxmny ioriSum, A1.norigmny iOriMoney, A1.ntaxmny isum, A1.nmny iMoney, NVL(A1.nqtprice,0) iUnitCost, A1.vnotebody remark from ic_saleout_h A left join ic_saleout_b A1 on A.cgeneralhid = A1.cgeneralhid and A1.DR != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc  where A.PK_ORG = '0001A110000000001V70' and A.DR != 1 AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag = 3 and substr(A2.code,0,4) != '0915' and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY')";
                    }
                }
                DataSet SaleOutLine = OracleHelper.ExecuteDataset(sql);

                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id( 'DispatchList') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table DispatchList(ID nvarchar(30) primary key not null,code nvarchar(50),ddate nvarchar(20),cwhcode nvarchar(100),cstcode nvarchar(200),custcode nvarchar(200),custname nvarchar(200),cdepcode nvarchar(200),cCCCode nvarchar(200),remark nvarchar(500),isRed bit default 0,ts nvarchar(50),zt bit default 0,memo text)";
                    SqlHelper.ExecuteNonQuery(createSql);
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(SaleOut, "DispatchList");
                    SqlHelper.ExecuteNonQuery(str.ToString());
                    msg = "销售出库表插入成功";
                }
                else
                {
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(SaleOut, "DispatchList");
                    if (!string.IsNullOrEmpty(str.ToString()))
                    {
                        SqlHelper.ExecuteNonQuery(str.ToString());
                        msg = "销售出库表更新成功";
                    }
                    else
                    {
                        msg = "销售出库表暂无可更新数据";
                    }
                }
                tableExist = "if object_id( 'DispatchLists') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table DispatchLists(ID nvarchar(30),autoid nvarchar(30)  primary key not null,doclineno nvarchar(50),cinvcode nvarchar(50),cinvname nvarchar(500),cinvclass nvarchar(500),cinvclassname nvarchar(500),cinvstd nvarchar(500),cinvUnit  nvarchar(500),qty decimal(28, 8),itaxrate decimal(28, 8),iOriTaxCost decimal(28, 8),iOriCost decimal(28, 8),ioriSum decimal(28, 8),	iOriMoney decimal(28, 8),	isum decimal(28, 8),iMoney decimal(28, 8),iUnitCost decimal(28, 8),remark nvarchar(100))";
                    SqlHelper.ExecuteNonQuery(createSql);
                    StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(SaleOutLine, "DispatchLists");
                    SqlHelper.ExecuteNonQuery(strs.ToString());
                    msg = "销售出库表行插入成功";
                }
                else
                {
                    StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(SaleOutLine, "DispatchLists");
                    if (!string.IsNullOrEmpty(strs.ToString()))
                    {
                        SqlHelper.ExecuteNonQuery(strs.ToString());
                        msg = "销售出库表行更新成功";
                    }
                    else
                    {
                        msg = "销售出库表暂无可更新数据";
                    }
                }
                GetU8SVApiUrlApi("fhdapi");
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
            int updateCount = 0;
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
                    string delstr = "delete from RdRecord11 where id in(select ID from RdRecord11 where zt != 1 )";
                    string delstr2 = "delete from RdRecords11 where id in(select ID from RdRecord11 where zt != 1 )";
                    SqlHelper.ExecuteNonQuerys(delstr2);
                    SqlHelper.ExecuteNonQuerys(delstr);
                    string str = "select id from RdRecord11";
                    DataSet ds = SqlHelper.ExecuteDataset(connectionString, CommandType.Text, str);
                    if (ds.Tables[0].Rows.Count > 0)
                    {
                        updateCount = ds.Tables[0].Rows.Count;
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
                    else
                    {
                        updateCount = 0;
                        sql = "select A.cgeneralhid ID,A.vbillcode code,A.dbilldate ddate,A1.Code cwhcode,A2.pk_billtypecode crdcode, A3.code cdepcode, A.vnote remark,CASE WHEN A.ntotalnum > 0 THEN 0 ELSE 1 END AS isRed,A.modifiedtime ts from ic_material_h A left join bd_stordoc A1 on A.cwarehouseid = A1.Pk_Stordoc left join bd_billtype A2 on A.ctrantypeid = A2.pk_billtypeid left join org_dept A3 on A3.PK_DEPT = A.cdptid where  not exists (select cgeneralhid from (select distinct pob.cgeneralhid cgeneralhid from ic_material_b pob left join bd_material mat on mat.pk_material = pob.cmaterialvid and nvl(mat.dr,0)=0 left join ic_material_h poh on poh.cgeneralhid = pob.cgeneralhid and nvl(poh.dr,0)=0 where  nvl(pob.dr,0)=0 and substr(mat.code,0,4) = '0915' and poh.PK_ORG='0001A110000000001V70'  AND substr(poh.taudittime,0,10) between '" + startTime + "' and '" + endTime + "' and poh.fbillflag=3) po  where po.cgeneralhid = A.cgeneralhid) and  A.PK_ORG='0001A110000000001V70'  AND substr(A.taudittime,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag=3 and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY')";
                    }
                }
                DataSet Material = OracleHelper.ExecuteDataset(sql);

                tableExist = "if object_id( 'RdRecords11') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    //获取材料出库行数据
                    sql = "select A1.cgeneralhid ID,A1.cgeneralbid autoid,A1.crowno doclineno,A2.code cinvcode,A2.Name cinvname,A3.code cinvclass,A3.Name cinvclassname,A2.materialspec cinvstd,A4.code cinvUnit,NVL(A1.nassistnum ,0) qty, A1.vnotebody remark from ic_material_h A left join ic_material_b A1 on A.cgeneralhid = A1.cgeneralhid and A1.DR != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 on A2.pk_marbasclass=A3.pk_marbasclass left join bd_measdoc A4 on A4.pk_measdoc=A2.pk_measdoc where A.PK_ORG = '0001A110000000001V70' and A.DR != 1 AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "'  and A.fbillflag = 3 and substr(A2.code,0,4) != '0915' and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY')";
                }
                else
                {
                    if (updateCount > 0)
                    {
                        sql = "select A1.cgeneralhid ID,A1.cgeneralbid autoid,A1.crowno doclineno,A2.code cinvcode,A2.Name cinvname,A3.code cinvclass,A3.Name cinvclassname,A2.materialspec cinvstd,A4.code cinvUnit,NVL(A1.nassistnum ,0) qty, A1.vnotebody remark from ic_material_h A left join ic_material_b A1 on A.cgeneralhid = A1.cgeneralhid and A1.DR != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 on A2.pk_marbasclass=A3.pk_marbasclass left join bd_measdoc A4 on A4.pk_measdoc=A2.pk_measdoc where A.PK_ORG = '0001A110000000001V70' and A.DR != 1 AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "'  and A.fbillflag = 3 and substr(A2.code,0,4) != '0915' and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY') and " + strGetOracleSQLIn + "";
                    }
                    else
                    {
                        sql = "select A1.cgeneralhid ID,A1.cgeneralbid autoid,A1.crowno doclineno,A2.code cinvcode,A2.Name cinvname,A3.code cinvclass,A3.Name cinvclassname,A2.materialspec cinvstd,A4.code cinvUnit,NVL(A1.nassistnum ,0) qty, A1.vnotebody remark from ic_material_h A left join ic_material_b A1 on A.cgeneralhid = A1.cgeneralhid and A1.DR != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 on A2.pk_marbasclass=A3.pk_marbasclass left join bd_measdoc A4 on A4.pk_measdoc=A2.pk_measdoc where A.PK_ORG = '0001A110000000001V70' and A.DR != 1 AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "'  and A.fbillflag = 3 and substr(A2.code,0,4) != '0915' and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY')";
                    }
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
                    if (!string.IsNullOrEmpty(str.ToString()))
                    {
                        SqlHelper.ExecuteNonQuery(str.ToString());
                        msg = "材料出库表更新成功";
                    }
                    else
                    {
                        msg = "材料出库表暂无可更新数据";
                    }
                }
                tableExist = "if object_id( 'RdRecords11') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table RdRecords11(ID nvarchar(30),autoid nvarchar(30)  primary key not null,doclineno nvarchar(50),cinvcode nvarchar(50),cinvname nvarchar(50),cinvclass nvarchar(50),cinvclassname nvarchar(50),cinvstd nvarchar(50),cinvUnit nvarchar(50),qty decimal(28, 8),remark nvarchar(100))";
                    SqlHelper.ExecuteNonQuery(createSql);
                    StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(MaterialLine, "RdRecords11");
                    SqlHelper.ExecuteNonQuery(strs.ToString());
                    msg = "材料出库表行插入成功";
                }
                else
                {
                    StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(MaterialLine, "RdRecords11");
                    if (!string.IsNullOrEmpty(strs.ToString()))
                    {
                        SqlHelper.ExecuteNonQuery(strs.ToString());
                        msg = "材料出库表行更新成功";
                    }
                    else
                    {
                        msg = "材料出库表暂无可更新数据";
                    }
                }
                GetU8SVApiUrlApi("clckapi");
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
            int updateCount = 0;
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
                    sql = "SELECT distinct A.cgeneralhid ID, A.vbillcode code, A.dbilldate ddate, A2.code cwhcode,A3.pk_billtypecode crdcode,A1.CODE cdepcode, A1.name cdepname,A.vnote remark,A.modifiedtime ts FROM ic_generalin_h A left join org_dept A1 on A1.pk_dept = A.cdptid left join bd_stordoc A2 on A2.pk_stordoc = A.cwarehouseid left join bd_billtype A3 on A3.pk_billtypeid = A.ctrantypeid INNER join ic_generalin_b A4 on A.cgeneralhid=A4.cgeneralhid LEFT JOIN bd_stordoc A5 on A5.pk_stordoc=A.cwarehouseid left join bd_material mat on mat.pk_material = A4.cmaterialvid where substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A2.CODE NOT IN('PPG','STJGC') AND A.vtrantypecode not IN ('4A-02','4A-06') AND MAT.CODE NOT LIKE '0915%' and A.PK_ORG='0001A110000000001V70' AND A.DR=0";
                }
                else
                {
                    string delstr = "delete from RdRecord08 where id in(select ID from RdRecord08 where zt != 1 )";
                    string delstr2 = "delete from RdRecords08 where id in(select ID from RdRecord08 where zt != 1 )";
                    SqlHelper.ExecuteNonQuerys(delstr2);
                    SqlHelper.ExecuteNonQuerys(delstr);
                    string str = "select id from RdRecord08";
                    DataSet ds = SqlHelper.ExecuteDataset(connectionString, CommandType.Text, str);
                    if (ds.Tables[0].Rows.Count > 0)
                    {
                        updateCount = ds.Tables[0].Rows.Count;
                        foreach (DataRow dr in ds.Tables[0].Rows)
                        {
                            strbu.Append(dr["id"].ToString() + ",");
                        }
                        strbu = strbu.Remove(strbu.Length - 1, 1);
                        String[] ids = strbu.ToString().Split(',');
                        strGetOracleSQLIn = getOracleSQLIn(ids, "A.cgeneralhid");
                        //获取采购入库头数据
                        sql = "SELECT distinct A.cgeneralhid ID, A.vbillcode code, A.dbilldate ddate, A2.code cwhcode,A3.pk_billtypecode crdcode,A1.CODE cdepcode, A1.name cdepname,A.vnote remark,A.modifiedtime ts FROM ic_generalin_h A left join org_dept A1 on A1.pk_dept = A.cdptid left join bd_stordoc A2 on A2.pk_stordoc = A.cwarehouseid left join bd_billtype A3 on A3.pk_billtypeid = A.ctrantypeid INNER join ic_generalin_b A4 on A.cgeneralhid=A4.cgeneralhid LEFT JOIN bd_stordoc A5 on A5.pk_stordoc=A.cwarehouseid left join bd_material mat on mat.pk_material = A4.cmaterialvid where substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A2.CODE NOT IN('PPG','STJGC') AND A.vtrantypecode not IN ('4A-02','4A-06') AND MAT.CODE NOT LIKE '0915%' and A.PK_ORG='0001A110000000001V70' AND A.DR=0  and " + strGetOracleSQLIn + "";

                    }
                    else
                    {
                        updateCount = 0;
                        sql = "SELECT distinct A.cgeneralhid ID, A.vbillcode code, A.dbilldate ddate, A2.code cwhcode,A3.pk_billtypecode crdcode,A1.CODE cdepcode, A1.name cdepname,A.vnote remark,A.modifiedtime ts FROM ic_generalin_h A left join org_dept A1 on A1.pk_dept = A.cdptid left join bd_stordoc A2 on A2.pk_stordoc = A.cwarehouseid left join bd_billtype A3 on A3.pk_billtypeid = A.ctrantypeid INNER join ic_generalin_b A4 on A.cgeneralhid=A4.cgeneralhid LEFT JOIN bd_stordoc A5 on A5.pk_stordoc=A.cwarehouseid left join bd_material mat on mat.pk_material = A4.cmaterialvid where substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A2.CODE NOT IN('PPG','STJGC') AND A.vtrantypecode not IN ('4A-02','4A-06') AND MAT.CODE NOT LIKE '0915%' and A.PK_ORG='0001A110000000001V70' AND A.DR=0";
                    }
                }
                DataSet IAi4bill = OracleHelper.ExecuteDataset(sql);

                tableExist = "if object_id( 'RdRecords08') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    //获取其他入库行数据
                    sql = "SELECT A.cgeneralhid ID,A1.cgeneralbid autoid,A1.crowno doclineno,A2.code cinvcode, A2.name cinvname, A3.code cinvclass,A3.name cinvclassname,A2.materialspec cinvstd, A4.code cinvUnit,NVL(A1.nnum,0) qty FROM ic_generalin_h A left join ic_generalin_b A1 on A1.cgeneralhid = A.cgeneralhid and A1.dr != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 on A3.pk_marbasclass = A2.pk_marbasclass left join bd_measdoc A4 on A4.pk_measdoc = A2.pk_measdoc left join bd_billtype A5 on A5.pk_billtypeid = A.ctrantypeid left join bd_stordoc A6 on A6.pk_stordoc = A.cwarehouseid where  (A.vtrantypecode not IN ('4A-02','4A-06')) AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A2.CODE NOT LIKE '0915%' and A6.CODE NOT IN('PPG','STJGC') and A.PK_ORG='0001A110000000001V70'";
                }
                else
                {
                    if (updateCount > 0)
                    {
                        sql = "SELECT A.cgeneralhid ID,A1.cgeneralbid autoid,A1.crowno doclineno,A2.code cinvcode, A2.name cinvname, A3.code cinvclass,A3.name cinvclassname,A2.materialspec cinvstd, A4.code cinvUnit,NVL(A1.nnum,0) qty FROM ic_generalin_h A left join ic_generalin_b A1 on A1.cgeneralhid = A.cgeneralhid and A1.dr != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 on A3.pk_marbasclass = A2.pk_marbasclass left join bd_measdoc A4 on A4.pk_measdoc = A2.pk_measdoc left join bd_billtype A5 on A5.pk_billtypeid = A.ctrantypeid left join bd_stordoc A6 on A6.pk_stordoc = A.cwarehouseid where  (A.vtrantypecode not IN ('4A-02','4A-06')) AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A2.CODE NOT LIKE '0915%' and A6.CODE NOT IN('PPG','STJGC') and A.PK_ORG='0001A110000000001V70' and " + strGetOracleSQLIn + "";
                    }
                    else
                    {
                        sql = "SELECT A.cgeneralhid ID,A1.cgeneralbid autoid,A1.crowno doclineno,A2.code cinvcode, A2.name cinvname, A3.code cinvclass,A3.name cinvclassname,A2.materialspec cinvstd, A4.code cinvUnit,NVL(A1.nnum,0) qty FROM ic_generalin_h A left join ic_generalin_b A1 on A1.cgeneralhid = A.cgeneralhid and A1.dr != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 on A3.pk_marbasclass = A2.pk_marbasclass left join bd_measdoc A4 on A4.pk_measdoc = A2.pk_measdoc left join bd_billtype A5 on A5.pk_billtypeid = A.ctrantypeid left join bd_stordoc A6 on A6.pk_stordoc = A.cwarehouseid where  (A.vtrantypecode not IN ('4A-02','4A-06')) AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A2.CODE NOT LIKE '0915%' and A6.CODE NOT IN('PPG','STJGC') and A.PK_ORG='0001A110000000001V70'";
                    }
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
                    if (!string.IsNullOrEmpty(str.ToString()))
                    {
                        SqlHelper.ExecuteNonQuery(str.ToString());
                        msg = "其他入库表更新成功";
                    }
                    else
                    {
                        msg = "其他入库表暂无可更新数据";
                    }
                }
                tableExist = "if object_id('RdRecords08') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table RdRecords08(ID nvarchar(30),autoid nvarchar(30)  primary key not null,doclineno nvarchar(50),cinvcode nvarchar(50),cinvname nvarchar(500),cinvclass nvarchar(500),cinvclassname nvarchar(500),cinvstd nvarchar(500),cinvUnit  nvarchar(500),qty decimal(28,8),remark nvarchar(100))";
                    SqlHelper.ExecuteNonQuery(createSql);
                    StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(IAi4billLine, "RdRecords08");
                    SqlHelper.ExecuteNonQuery(strs.ToString());
                    msg = "其他入库表行插入成功";
                }
                else
                {
                    StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(IAi4billLine, "RdRecords08");
                    if (!string.IsNullOrEmpty(strs.ToString()))
                    {
                        SqlHelper.ExecuteNonQuery(strs.ToString());
                        msg = "其他入库表行更新成功";
                    }
                    else
                    {
                        msg = "其他入库表暂无可更新数据";
                    }
                }
                GetU8SVApiUrlApi("qtrkdapi");
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
            int updateCount = 0;
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
                    sql = "SELECT distinct A.cgeneralhid ID, A.vbillcode code, A.dbilldate ddate, A2.code cwhcode,A3.pk_billtypecode crdcode,A1.CODE cdepcode, A1.name cdepname,A.vnote remark,A.modifiedtime ts FROM ic_generalout_h A left join org_dept A1 on A1.pk_dept = A.cdptid left join bd_stordoc A2 on A2.pk_stordoc = A.cwarehouseid left join bd_billtype A3 on A3.pk_billtypeid = A.ctrantypeid INNER join ic_generalout_b A4 on A.cgeneralhid=A4.cgeneralhid LEFT JOIN bd_stordoc A5 on A5.pk_stordoc=A.cwarehouseid left join bd_material mat on mat.pk_material = A4.cmaterialvid where substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A2.CODE NOT IN('PPG','STJGC') AND A.vtrantypecode not IN ('4I-02','4I-06') AND MAT.CODE NOT LIKE '0915%' and A.PK_ORG='0001A110000000001V70' AND A.DR=0";
                }
                else
                {
                    string delstr = "delete from RdRecord09 where id in(select ID from RdRecord09 where zt != 1 )";
                    string delstr2 = "delete from RdRecords09 where id in(select ID from RdRecord09 where zt != 1 )";
                    SqlHelper.ExecuteNonQuerys(delstr2);
                    SqlHelper.ExecuteNonQuerys(delstr);
                    string str = "select id from RdRecord09";
                    DataSet ds = SqlHelper.ExecuteDataset(connectionString, CommandType.Text, str);
                    if (ds.Tables[0].Rows.Count > 0)
                    {
                        updateCount = ds.Tables[0].Rows.Count;
                        foreach (DataRow dr in ds.Tables[0].Rows)
                        {
                            strbu.Append(dr["id"].ToString() + ",");
                        }
                        strbu = strbu.Remove(strbu.Length - 1, 1);
                        String[] ids = strbu.ToString().Split(',');
                        strGetOracleSQLIn = getOracleSQLIn(ids, "A.cgeneralhid");
                        //获取采购入库头数据
                        sql = "SELECT distinct A.cgeneralhid ID, A.vbillcode code, A.dbilldate ddate, A2.code cwhcode,A3.pk_billtypecode crdcode,A1.CODE cdepcode, A1.name cdepname,A.vnote remark,A.modifiedtime ts FROM ic_generalout_h A left join org_dept A1 on A1.pk_dept = A.cdptid left join bd_stordoc A2 on A2.pk_stordoc = A.cwarehouseid left join bd_billtype A3 on A3.pk_billtypeid = A.ctrantypeid INNER join ic_generalout_b A4 on A.cgeneralhid=A4.cgeneralhid LEFT JOIN bd_stordoc A5 on A5.pk_stordoc=A.cwarehouseid left join bd_material mat on mat.pk_material = A4.cmaterialvid where substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A2.CODE NOT IN('PPG','STJGC') AND A.vtrantypecode not IN ('4I-02','4I-06') AND MAT.CODE NOT LIKE '0915%' and A.PK_ORG='0001A110000000001V70' AND A.DR=0 and " + strGetOracleSQLIn + "";

                    }
                    else
                    {
                        updateCount = 0;
                        sql = "SELECT distinct A.cgeneralhid ID, A.vbillcode code, A.dbilldate ddate, A2.code cwhcode,A3.pk_billtypecode crdcode,A1.CODE cdepcode, A1.name cdepname,A.vnote remark,A.modifiedtime ts FROM ic_generalout_h A left join org_dept A1 on A1.pk_dept = A.cdptid left join bd_stordoc A2 on A2.pk_stordoc = A.cwarehouseid left join bd_billtype A3 on A3.pk_billtypeid = A.ctrantypeid INNER join ic_generalout_b A4 on A.cgeneralhid=A4.cgeneralhid LEFT JOIN bd_stordoc A5 on A5.pk_stordoc=A.cwarehouseid left join bd_material mat on mat.pk_material = A4.cmaterialvid where substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A2.CODE NOT IN('PPG','STJGC') AND A.vtrantypecode not IN ('4I-02','4I-06') AND MAT.CODE NOT LIKE '0915%' and A.PK_ORG='0001A110000000001V70' AND A.DR=0";
                    }
                }
                DataSet IAi7bill = OracleHelper.ExecuteDataset(sql);

                tableExist = "if object_id( 'RdRecords09') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    //获取其他出库行数据
                    sql = "SELECT A.cgeneralhid ID,A1.cgeneralbid autoid,A1.crowno doclineno,A2.code cinvcode, A2.name cinvname, A3.code cinvclass,A3.name cinvclassname, A2.materialspec cinvstd, A4.code cinvUnit,NVL(A1.nshouldassistnum,0) qty FROM ic_generalout_h A left join ic_generalout_b A1 on A1.cgeneralhid = A.cgeneralhid and A1.dr != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 on A3.pk_marbasclass = A2.pk_marbasclass left join bd_measdoc A4 on A4.pk_measdoc = A2.pk_measdoc left join bd_billtype A5 on A5.pk_billtypeid = A.ctrantypeid left join bd_stordoc A6 on A6.pk_stordoc = A.cwarehouseid where  (A.vtrantypecode not IN ('4I-02','4I-06')) AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A2.CODE NOT LIKE '0915%' and A6.CODE NOT IN('PPG','STJGC') and A.PK_ORG='0001A110000000001V70'";
                }
                else
                {
                    if (updateCount > 0)
                    {
                        sql = "SELECT A.cgeneralhid ID,A1.cgeneralbid autoid,A1.crowno doclineno,A2.code cinvcode, A2.name cinvname, A3.code cinvclass,A3.name cinvclassname, A2.materialspec cinvstd, A4.code cinvUnit,NVL(A1.nnum,0) qty FROM ic_generalout_h A left join ic_generalout_b A1 on A1.cgeneralhid = A.cgeneralhid and A1.dr != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 on A3.pk_marbasclass = A2.pk_marbasclass left join bd_measdoc A4 on A4.pk_measdoc = A2.pk_measdoc left join bd_billtype A5 on A5.pk_billtypeid = A.ctrantypeid left join bd_stordoc A6 on A6.pk_stordoc = A.cwarehouseid where  (A.vtrantypecode not IN ('4I-02','4I-06')) AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A2.CODE NOT LIKE '0915%' and A6.CODE NOT IN('PPG','STJGC') and A.PK_ORG='0001A110000000001V70' and " + strGetOracleSQLIn + "";
                    }
                    else
                    {
                        sql = "SELECT A.cgeneralhid ID,A1.cgeneralbid autoid,A1.crowno doclineno,A2.code cinvcode, A2.name cinvname, A3.code cinvclass,A3.name cinvclassname, A2.materialspec cinvstd, A4.code cinvUnit,NVL(A1.nnum,0) qty FROM ic_generalout_h A left join ic_generalout_b A1 on A1.cgeneralhid = A.cgeneralhid and A1.dr != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 on A3.pk_marbasclass = A2.pk_marbasclass left join bd_measdoc A4 on A4.pk_measdoc = A2.pk_measdoc left join bd_billtype A5 on A5.pk_billtypeid = A.ctrantypeid left join bd_stordoc A6 on A6.pk_stordoc = A.cwarehouseid where  (A.vtrantypecode not IN ('4I-02','4I-06')) AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A2.CODE NOT LIKE '0915%' and A6.CODE NOT IN('PPG','STJGC') and A.PK_ORG='0001A110000000001V70'";
                    }
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
                    if (!string.IsNullOrEmpty(str.ToString()))
                    {
                        SqlHelper.ExecuteNonQuery(str.ToString());
                        msg = "其他出库表更新成功";
                    }
                    else
                    {
                        msg = "其他出库表暂无可更新数据";
                    }
                }
                tableExist = "if object_id( 'RdRecords09') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table RdRecords09(ID nvarchar(30),autoid nvarchar(30)  primary key not null,doclineno nvarchar(50),cinvcode nvarchar(50),cinvname nvarchar(500),cinvclass nvarchar(500),cinvclassname nvarchar(500),cinvstd nvarchar(500),cinvUnit  nvarchar(500),qty decimal(28,8),remark nvarchar(100))";
                    SqlHelper.ExecuteNonQuery(createSql);
                    StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(IAi7billLine, "RdRecords09");
                    SqlHelper.ExecuteNonQuery(strs.ToString());
                    msg = "其他出库表行插入成功";
                }
                else
                {
                    StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(IAi7billLine, "RdRecords09");
                    if (!string.IsNullOrEmpty(strs.ToString()))
                    {
                        SqlHelper.ExecuteNonQuery(strs.ToString());
                        msg = "其他出库表行更新成功";
                    }
                    else
                    {
                        msg = "其他出库表暂无可更新数据";
                    }
                }
                GetU8SVApiUrlApi("qtckdapi");
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
            int updateCount = 0;
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
                    sql = "SELECT A.cgeneralhid ID, A.vbillcode code, A.dbilldate ddate, A2.code cwhcode, A3.pk_billtypecode crdcode,A1.CODE cdepcode, A.vnote remark,CASE WHEN A.ntotalnum > 0 THEN 0 ELSE 1 END AS isRed, A.modifiedtime ts FROM ic_finprodin_h A left join org_dept_v A1 on A1.pk_vid = A.cdptvid left join bd_stordoc A2 on A2.pk_stordoc = A.cwarehouseid left join bd_billtype A3 on A3.pk_billtypeid = A.ctrantypeid where  not exists (select cgeneralhid from (select distinct pob.cgeneralhid cgeneralhid from ic_finprodin_b pob left join bd_material mat on mat.pk_material = pob.cmaterialvid and nvl(mat.dr,0)=0 left join ic_finprodin_h poh on poh.cgeneralhid = pob.cgeneralhid and nvl(poh.dr,0)=0 where  nvl(pob.dr,0)=0 and substr(mat.code,0,4) = '0915' and poh.PK_ORG='0001A110000000001V70'  AND substr(poh.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and poh.fbillflag=3) po  where po.cgeneralhid = A.cgeneralhid) and  A.PK_ORG='0001A110000000001V70'  AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag=3 and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY')";
                }
                else
                {
                    string delstr = "delete from RdRecord10 where id in(select ID from RdRecord10 where zt != 1 )";
                    string delstr2 = "delete from RdRecords10 where id in(select ID from RdRecord10 where zt != 1 )";
                    SqlHelper.ExecuteNonQuerys(delstr2);
                    SqlHelper.ExecuteNonQuerys(delstr);
                    string str = "select id from RdRecord10";
                    DataSet ds = SqlHelper.ExecuteDataset(connectionString, CommandType.Text, str);
                    if (ds.Tables[0].Rows.Count > 0)
                    {
                        updateCount = ds.Tables[0].Rows.Count;
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
                    else
                    {
                        updateCount = 0;
                        sql = "SELECT A.cgeneralhid ID, A.vbillcode code, A.dbilldate ddate, A2.code cwhcode, A3.pk_billtypecode crdcode,A1.CODE cdepcode, A.vnote remark,CASE WHEN A.ntotalnum > 0 THEN 0 ELSE 1 END AS isRed, A.modifiedtime ts FROM ic_finprodin_h A left join org_dept A1 on A1.pk_dept = A.cdptid left join bd_stordoc A2 on A2.pk_stordoc = A.cwarehouseid left join bd_billtype A3 on A3.pk_billtypeid = A.ctrantypeid where  not exists (select cgeneralhid from (select distinct pob.cgeneralhid cgeneralhid from ic_finprodin_b pob left join bd_material mat on mat.pk_material = pob.cmaterialvid and nvl(mat.dr,0)=0 left join ic_finprodin_h poh on poh.cgeneralhid = pob.cgeneralhid and nvl(poh.dr,0)=0 where  nvl(pob.dr,0)=0 and substr(mat.code,0,4) = '0915' and poh.PK_ORG='0001A110000000001V70'  AND substr(poh.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and poh.fbillflag=3) po  where po.cgeneralhid = A.cgeneralhid) and  A.PK_ORG='0001A110000000001V70'  AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag=3 and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY')";
                    }
                }
                DataSet FinprodIn = OracleHelper.ExecuteDataset(sql);

                tableExist = "if object_id( 'RdRecords10') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    //获取产成品入库行数据
                    sql = "SELECT A.cgeneralhid ID,A1.cgeneralbid autoid,A1.crowno doclineno,A2.code cinvcode,A2.name cinvname, A3.code cinvclass,A3.name cinvclassname, A2.materialspec cinvstd, A4.code cinvUnit,NVL(A1.nassistnum ,0) qty, A1.vnotebody remark FROM ic_finprodin_h A left join ic_finprodin_b A1 on A1.cgeneralhid = A.cgeneralhid and A1.dr != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc  where A.PK_ORG = '0001A110000000001V70' AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag = 3 and substr(A2.code,0,4) != '0915' and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY')";
                }
                else
                {
                    if (updateCount > 0)
                    {
                        sql = "SELECT A.cgeneralhid ID,A1.cgeneralbid autoid,A1.crowno doclineno,A2.code cinvcode,A2.name cinvname, A3.code cinvclass,A3.name cinvclassname, A2.materialspec cinvstd, A4.code cinvUnit,NVL(A1.nassistnum ,0) qty, A1.vnotebody remark FROM ic_finprodin_h A left join ic_finprodin_b A1 on A1.cgeneralhid = A.cgeneralhid and A1.dr != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc  where A.PK_ORG = '0001A110000000001V70' AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag = 3 and substr(A2.code,0,4) != '0915' and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY') and " + strGetOracleSQLIn + "";
                    }
                    else
                    {
                        sql = "SELECT A.cgeneralhid ID,A1.cgeneralbid autoid,A1.crowno doclineno,A2.code cinvcode,A2.name cinvname, A3.code cinvclass,A3.name cinvclassname, A2.materialspec cinvstd, A4.code cinvUnit,NVL(A1.nassistnum ,0) qty, A1.vnotebody remark FROM ic_finprodin_h A left join ic_finprodin_b A1 on A1.cgeneralhid = A.cgeneralhid and A1.dr != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc  where A.PK_ORG = '0001A110000000001V70' AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag = 3 and substr(A2.code,0,4) != '0915' and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY')";
                    }
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
                    if (!string.IsNullOrEmpty(str.ToString()))
                    {
                        SqlHelper.ExecuteNonQuery(str.ToString());
                        msg = "产成品入库表更新成功";
                    }
                    else
                    {
                        msg = "产成品入库表暂无可更新数据";
                    }
                }
                tableExist = "if object_id( 'RdRecords10') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table RdRecords10(ID nvarchar(30),autoid nvarchar(30)  primary key not null,doclineno nvarchar(50),cinvcode nvarchar(50),cinvname nvarchar(500),cinvclass nvarchar(500),cinvclassname nvarchar(500),cinvstd nvarchar(500),cinvUnit  nvarchar(500),qty decimal(28,8),remark nvarchar(100))";
                    SqlHelper.ExecuteNonQuery(createSql);
                    StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(FinprodInLine, "RdRecords10");
                    SqlHelper.ExecuteNonQuery(strs.ToString());
                    msg = "产成品入库表行插入成功";
                }
                else
                {
                    StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(FinprodInLine, "RdRecords10");
                    if (!string.IsNullOrEmpty(strs.ToString()))
                    {
                        SqlHelper.ExecuteNonQuery(strs.ToString());
                        msg = "产成品入库表行更新成功";
                    }
                    else
                    {
                        msg = "产成品入库表暂无可更新数据";
                    }
                }
                GetU8SVApiUrlApi("ccprkapi");
                result = msg;
            }
            catch (Exception e)
            {

                result = "产成品入库错误：" + e.Message;
            }
            return result;

        }


        /// <summary>
        /// 批量更新产成品入库项目号
        /// 创建人：lvhe
        /// 创建时间：2020-4-9 22:44:27
        /// </summary>
        /// <returns></returns>
        private string BatchUpdateFinprodInCode()
        {
            string msg = GetU8SVApiUrlApi("plgxccprkapi");
            return msg;
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
            int updateCount = 0;
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
                    string delstr = "delete from TransVouch where id in(select ID from TransVouch where zt != 1 )";
                    string delstr2 = "delete from TransVouchs where id in(select ID from TransVouch where zt != 1 )";
                    SqlHelper.ExecuteNonQuerys(delstr2);
                    SqlHelper.ExecuteNonQuerys(delstr);
                    string str = "select id from TransVouch";
                    DataSet ds = SqlHelper.ExecuteDataset(connectionString, CommandType.Text, str);
                    if (ds.Tables[0].Rows.Count > 0)
                    {
                        updateCount = ds.Tables[0].Rows.Count;
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
                    else
                    {
                        updateCount = 0;
                        sql = "SELECT A.cspecialhid ID, A.vbillcode code, A.dbilldate ddate, A1.code cowhcode,A2.code ciwhcode,A3.code codepcode, A4.code cidepcode, A.vnote remark, CASE WHEN A.ntotalnum > 0 THEN 0 ELSE 1 END AS isRed,A.modifiedtime ts FROM ic_whstrans_h A left join bd_stordoc A1 on A1.pk_stordoc = A.cwarehouseid left join bd_stordoc A2 on A2.pk_stordoc = A.cotherwhid left join org_dept_v A3 on A3.pk_vid = A.cdptvid left join org_dept_v A4 on A4.pk_vid = A.cotherdptvid where  not exists (select cspecialhid from (select distinct pob.cspecialhid cspecialhid from ic_whstrans_b pob left join bd_material mat on mat.pk_material = pob.cmaterialvid and nvl(mat.dr,0)=0 left join ic_whstrans_h poh on poh.cspecialhid = pob.cspecialhid and nvl(poh.dr,0)=0 where  nvl(pob.dr,0)=0 and substr(mat.code,0,4) = '0915' and poh.PK_ORG='0001A110000000001V70'  AND substr(poh.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and poh.fbillflag=4) po  where po.cspecialhid = A.cspecialhid) and  A.PK_ORG='0001A110000000001V70'  AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag=4 and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY')  and A.cotherwhid  not in('1001A1100000000T5S5Z','1001A11000000003CYSY')";
                    }
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
                    if (updateCount > 0)
                    {
                        sql = "SELECT A.cspecialhid ID,A1.cspecialbid autoid,A1.crowno doclineno,A2.code cinvcode, A2.name cinvname, A3.code cinvclass, A2.materialspec cinvstd,A4.code cinvUnit,NVL(A1.nassistnum ,0) qty, A1.vnotebody remark FROM ic_whstrans_h A left join ic_whstrans_b A1 on A1.cspecialhid = A.cspecialhid and A1.dr != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc where A.PK_ORG = '0001A110000000001V70' AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag = 4 and substr(A2.code,0,4) != '0915' and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY')  and A.cotherwhid  not in('1001A1100000000T5S5Z','1001A11000000003CYSY') and " + strGetOracleSQLIn + "";
                    }
                    else
                    {
                        sql = "SELECT A.cspecialhid ID,A1.cspecialbid autoid,A1.crowno doclineno,A2.code cinvcode, A2.name cinvname, A3.code cinvclass, A2.materialspec cinvstd,A4.code cinvUnit,NVL(A1.nassistnum ,0) qty, A1.vnotebody remark FROM ic_whstrans_h A left join ic_whstrans_b A1 on A1.cspecialhid = A.cspecialhid and A1.dr != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc where A.PK_ORG = '0001A110000000001V70' AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and A.fbillflag = 4 and substr(A2.code,0,4) != '0915' and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY')  and A.cotherwhid  not in('1001A1100000000T5S5Z','1001A11000000003CYSY')";
                    }
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
                    if (!string.IsNullOrEmpty(str.ToString()))
                    {
                        SqlHelper.ExecuteNonQuery(str.ToString());
                        msg = "转库单表更新成功";
                    }
                    else
                    {
                        msg = "转库单表暂无可更新数据";
                    }
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
                    if (!string.IsNullOrEmpty(strs.ToString()))
                    {
                        SqlHelper.ExecuteNonQuery(strs.ToString());
                        msg = "转库单表行更新成功";
                    }
                    else
                    {
                        msg = "转库单表暂无可更新数据";
                    }

                }
                GetU8SVApiUrlApi("dbdapi");
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
            int updateCount = 0;
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
                    sql = " SELECT A.cspecialhid ID, A.vbillcode code, A.dbilldate ddate,A1.code cdepcode,A.vnote remark,0 isRed,A.modifiedtime ts FROM ic_transform_h A left join org_dept_v A1 on A1.pk_vid = A.cdptvid where  not exists (select cspecialhid from (select distinct pob.cspecialhid cspecialhid from ic_transform_b pob left join bd_material mat on mat.pk_material = pob.cmaterialvid and nvl(mat.dr,0)=0 left join ic_transform_h poh on poh.cspecialhid = pob.cspecialhid and nvl(poh.dr,0)=0 where  nvl(pob.dr,0)=0 and pob.cbodywarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY') and substr(mat.code,0,4) = '0915' and poh.PK_ORG='0001A110000000001V70'  AND substr(poh.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "') po  where po.cspecialhid = A.cspecialhid) and A.dr != 1  AND  A.PK_ORG='0001A110000000001V70'  AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' ";
                }
                else
                {
                    string delstr = "delete from AssemVouch where id in(select ID from AssemVouch where zt != 1 )";
                    string delstr2 = "delete from AssemVouchs where id in(select ID from AssemVouch where zt != 1 )";
                    SqlHelper.ExecuteNonQuerys(delstr2);
                    SqlHelper.ExecuteNonQuerys(delstr);
                    string str = "select id from AssemVouch";
                    DataSet ds = SqlHelper.ExecuteDataset(connectionString, CommandType.Text, str);
                    if (ds.Tables[0].Rows.Count > 0)
                    {
                        updateCount = ds.Tables[0].Rows.Count;
                        foreach (DataRow dr in ds.Tables[0].Rows)
                        {
                            strbu.Append(dr["id"].ToString() + ",");
                        }
                        strbu = strbu.Remove(strbu.Length - 1, 1);
                        String[] ids = strbu.ToString().Split(',');
                        strGetOracleSQLIn = getOracleSQLIn(ids, "A.cspecialhid");
                        //获取形态转换单单头数据
                        sql = " SELECT A.cspecialhid ID, A.vbillcode code, A.dbilldate ddate,A1.code cdepcode,A.vnote remark,0 isRed,A.modifiedtime ts FROM ic_transform_h A left join org_dept_v A1 on A1.pk_vid = A.cdptvid where  not exists (select cspecialhid from (select distinct pob.cspecialhid cspecialhid from ic_transform_b pob left join bd_material mat on mat.pk_material = pob.cmaterialvid and nvl(mat.dr,0)=0 left join ic_transform_h poh on poh.cspecialhid = pob.cspecialhid and nvl(poh.dr,0)=0 where  nvl(pob.dr,0)=0 and pob.cbodywarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY') and substr(mat.code,0,4) = '0915' and poh.PK_ORG='0001A110000000001V70'  AND substr(poh.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "') po  where po.cspecialhid = A.cspecialhid) and A.dr != 1  AND A.PK_ORG='0001A110000000001V70'  AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and " + strGetOracleSQLIn + "";
                    }
                    else
                    {
                        updateCount = 0;
                        sql = " SELECT A.cspecialhid ID, A.vbillcode code, A.dbilldate ddate,A1.code cdepcode,A.vnote remark,0 isRed,A.modifiedtime ts FROM ic_transform_h A left join org_dept_v A1 on A1.pk_vid = A.cdptvid where  not exists (select cspecialhid from (select distinct pob.cspecialhid cspecialhid from ic_transform_b pob left join bd_material mat on mat.pk_material = pob.cmaterialvid and nvl(mat.dr,0)=0 left join ic_transform_h poh on poh.cspecialhid = pob.cspecialhid and nvl(poh.dr,0)=0 where  nvl(pob.dr,0)=0 and pob.cbodywarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY') and substr(mat.code,0,4) = '0915' and poh.PK_ORG='0001A110000000001V70'  AND substr(poh.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "') po  where po.cspecialhid = A.cspecialhid) and A.dr != 1  AND A.PK_ORG='0001A110000000001V70'  AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' ";
                    }
                }
                DataSet IcTransformH = OracleHelper.ExecuteDataset(sql);

                tableExist = "if object_id( 'AssemVouchs') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    //获取形态转换单单行数据
                    sql = " SELECT A.cspecialhid ID,A1.cspecialbid autoid,CASE WHEN A1.fbillrowflag = 2 THEN '转换前' WHEN A1.fbillrowflag = 3 THEN '转换后' ELSE '' END AS bavtype,CASE WHEN (A1.cbeforebid is not null and A1.cbeforebid ='~') THEN A1.cspecialbid  ELSE A1.cbeforebid END AS igroupno,A5.code cwhcode,A1.crowno doclineno,A2.code cinvcode, A2.name cinvname, A3.code cinvclass, A2.materialspec cinvstd,A4.code cinvUnit,NVL(A1.nassistnum,0) qty, A1.vnotebody remark FROM ic_transform_h A left join ic_transform_b A1 on A1.cspecialhid = A.cspecialhid and A1.dr != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc left join bd_stordoc A5 on A5.pk_stordoc = A1.cbodywarehouseid where A.PK_ORG = '0001A110000000001V70' AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and substr(A2.code,0,4) != '0915' and A5.code not in('1001A1100000000T5S5Z','1001A11000000003CYSY')";
                }
                else
                {
                    if (updateCount > 0)
                    {
                        sql = " SELECT A.cspecialhid ID,A1.cspecialbid autoid,CASE WHEN A1.fbillrowflag = 2 THEN '转换前' WHEN A1.fbillrowflag = 3 THEN '转换后' ELSE '' END AS bavtype,CASE WHEN (A1.cbeforebid is not null and A1.cbeforebid ='~') THEN A1.cspecialbid  ELSE A1.cbeforebid END AS igroupno,A5.code cwhcode,A1.crowno doclineno,A2.code cinvcode, A2.name cinvname, A3.code cinvclass, A2.materialspec cinvstd,A4.code cinvUnit,NVL(A1.nassistnum,0) qty, A1.vnotebody remark FROM ic_transform_h A left join ic_transform_b A1 on A1.cspecialhid = A.cspecialhid and A1.dr != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc left join bd_stordoc A5 on A5.pk_stordoc = A1.cbodywarehouseid where A.PK_ORG = '0001A110000000001V70' AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and substr(A2.code,0,4) != '0915' and A5.code not in('1001A1100000000T5S5Z','1001A11000000003CYSY') and " + strGetOracleSQLIn + "";
                    }
                    else
                    {
                        sql = " SELECT A.cspecialhid ID,A1.cspecialbid autoid,CASE WHEN A1.fbillrowflag = 2 THEN '转换前' WHEN A1.fbillrowflag = 3 THEN '转换后' ELSE '' END AS bavtype,CASE WHEN (A1.cbeforebid is not null and A1.cbeforebid ='~') THEN A1.cspecialbid  ELSE A1.cbeforebid END AS igroupno,A5.code cwhcode,A1.crowno doclineno,A2.code cinvcode, A2.name cinvname, A3.code cinvclass, A2.materialspec cinvstd,A4.code cinvUnit,NVL(A1.nassistnum,0) qty, A1.vnotebody remark FROM ic_transform_h A left join ic_transform_b A1 on A1.cspecialhid = A.cspecialhid and A1.dr != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc left join bd_stordoc A5 on A5.pk_stordoc = A1.cbodywarehouseid where A.PK_ORG = '0001A110000000001V70' AND substr(A.dbilldate,0,10) between '" + startTime + "' and '" + endTime + "' and substr(A2.code,0,4) != '0915' and A5.code not in('1001A1100000000T5S5Z','1001A11000000003CYSY')";
                    }
                }
                DataSet IcTransformHLine = OracleHelper.ExecuteDataset(sql);

                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id( 'AssemVouch') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table AssemVouch(ID nvarchar(30),code nvarchar(50),ddate nvarchar(20),cdepcode nvarchar(50),remark nvarchar(100),isRed bit default 0,ts nvarchar(50),zt bit default 0,memo text)";
                    SqlHelper.ExecuteNonQuery(createSql);
                    if (IcTransformH.Tables.Count > 0 && IcTransformH.Tables[0].Rows.Count > 0)
                    {
                        StringBuilder str = DataSetToArrayList.DataSetToArrayLists(IcTransformH, "AssemVouch");
                        SqlHelper.ExecuteNonQuery(str.ToString());
                        msg = "形态转换表插入成功";
                    }
                    else
                    {
                        msg = "形态转换表上月无数据";
                    }
                }
                else
                {
                    if (IcTransformH.Tables.Count > 0 && IcTransformH.Tables[0].Rows.Count > 0)
                    {
                        StringBuilder str = DataSetToArrayList.DataSetToArrayLists(IcTransformH, "AssemVouch");
                        if (!string.IsNullOrEmpty(str.ToString()))
                        {
                            SqlHelper.ExecuteNonQuery(str.ToString());
                            msg = "形态转换表更新成功";
                        }
                        else
                        {
                            msg = "形态转换表暂无可更新数据";
                        }
                    }
                    else
                    {
                        msg = "形态转换表上月无数据";
                    }
                }
                tableExist = "if object_id( 'AssemVouchs') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table AssemVouchs(ID nvarchar(30),autoid nvarchar(30),bavtype nvarchar(50),igroupno nvarchar(50),cwhcode nvarchar(50),doclineno nvarchar(50),cinvcode nvarchar(50),cinvname nvarchar(500),cinvclass nvarchar(500),cinvstd nvarchar(500),cinvUnit  nvarchar(500),qty decimal(28,8),remark nvarchar(100))";
                    SqlHelper.ExecuteNonQuery(createSql);
                    if (IcTransformHLine.Tables.Count > 0 && IcTransformHLine.Tables[0].Rows.Count > 0)
                    {
                        StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(IcTransformHLine, "AssemVouchs");
                        if (!string.IsNullOrEmpty(strs.ToString()))
                        {
                            SqlHelper.ExecuteNonQuery(strs.ToString());
                            msg = "形态转换表行插入成功";
                        }
                        else
                        {
                            msg = "形态转换表暂无可更新数据";
                        }
                    }
                    else
                    {
                        msg = "形态转换表上月无数据";
                    }
                }
                else
                {
                    if (IcTransformHLine.Tables.Count > 0 && IcTransformHLine.Tables[0].Rows.Count > 0)
                    {
                        StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(IcTransformHLine, "AssemVouchs");
                        if (!string.IsNullOrEmpty(strs.ToString()))
                        {
                            SqlHelper.ExecuteNonQuery(strs.ToString());
                            msg = "形态转换表行更新成功";
                        }
                        else
                        {
                            msg = "形态转换表暂无可更新数据";
                        }
                    }
                    else
                    {
                        msg = "形态转换表上月无数据";
                    }
                }
                GetU8SVApiUrlApi("xtzhdapi");
                result = msg;
            }
            catch (Exception e)
            {

                result = "形态转换表错误：" + e.Message;
            }
            return result;

        }


        /// <summary>
        /// 从nc获取凭证数据插入到sql
        /// 创建人：lvhe
        /// 创建时间：2020-2-28 15:04:55
        /// </summary>
        /// <returns></returns>
        [WebMethod]
        private string GetGlVoucherToSql()
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
                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id( 'gl_voucher') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    //获取凭证单单头数据
                    sql = " select distinct A.pk_voucher ID,A1.name csign,A.prepareddate ddate,A.num ino_id,A.attachment idoc,(A.year || A.period) iyperiod,A.modifiedtime ts from　gl_voucher A left join bd_vouchertype A1 on A1.pk_vouchertype=A.pk_vouchertype left join gl_detail A2 on A2.Pk_Voucher =A.Pk_Voucher left join bd_accasoa A3 on A3.pk_accasoa=A2.pk_accasoa left join bd_account A4 on A4.pk_account=A3.pk_account where substr(A.prepareddate,0,10) between '" + startTime + "' and '" + endTime + "' and A.pk_org='0001A110000000001V70' and A.DR!=1 and A2.direction='D' and A4.code in(" + connNCSubjectEncoding + ") ";
                }
                else
                {
                    string delstr = "delete from gl_voucher";
                    string delstr2 = "delete from gl_vouchers";
                    //string delstr = "delete from gl_voucher where id in(select ID from gl_voucher where zt != 1 )";
                    //string delstr2 = "delete from gl_vouchers where id in(select ID from gl_voucher where zt != 1 )";
                    SqlHelper.ExecuteNonQuerys(delstr2);
                    SqlHelper.ExecuteNonQuerys(delstr);
                    string str = "select id from gl_voucher";
                    DataSet ds = SqlHelper.ExecuteDataset(connectionString, CommandType.Text, str);
                    if (ds.Tables[0].Rows.Count > 0)
                    {
                        updateCount = ds.Tables[0].Rows.Count;
                        foreach (DataRow dr in ds.Tables[0].Rows)
                        {
                            strbu.Append(dr["id"].ToString() + ",");
                        }
                        strbu = strbu.Remove(strbu.Length - 1, 1);
                        String[] ids = strbu.ToString().Split(',');
                        strGetOracleSQLIn = getOracleSQLIn(ids, "A.pk_voucher");
                        //获取凭证单单头数据
                        sql = " select distinct A.pk_voucher ID,A1.name csign,A.prepareddate ddate,A.num ino_id,A.attachment idoc,(A.year || A.period) iyperiod,A.modifiedtime ts from　gl_voucher A left join bd_vouchertype A1 on A1.pk_vouchertype=A.pk_vouchertype left join gl_detail A2 on A2.Pk_Voucher =A.Pk_Voucher left join bd_accasoa A3 on A3.pk_accasoa=A2.pk_accasoa left join bd_account A4 on A4.pk_account=A3.pk_account where substr(A.prepareddate,0,10) between '" + startTime + "' and '" + endTime + "' and A.pk_org='0001A110000000001V70' and A.DR!=1 and A2.direction='D' and A4.code in(" + connNCSubjectEncoding + ")  and " + strGetOracleSQLIn + "";
                    }
                    else
                    {
                        updateCount = 0;
                        sql = " select distinct A.pk_voucher ID,A1.name csign,A.prepareddate ddate,A.num ino_id,A.attachment idoc,(A.year || A.period) iyperiod,A.modifiedtime ts from　gl_voucher A left join bd_vouchertype A1 on A1.pk_vouchertype=A.pk_vouchertype left join gl_detail A2 on A2.Pk_Voucher =A.Pk_Voucher left join bd_accasoa A3 on A3.pk_accasoa=A2.pk_accasoa left join bd_account A4 on A4.pk_account=A3.pk_account where substr(A.prepareddate,0,10) between '" + startTime + "' and '" + endTime + "' and A.pk_org='0001A110000000001V70' and A.DR!=1 and A2.direction='D' and A4.code in(" + connNCSubjectEncoding + ") ";
                    }
                }
                DataSet GlVoucher = OracleHelper.ExecuteDataset(sql);

                tableExist = "if object_id( 'gl_vouchers') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    //获取凭证单单行数据
                    sql = " select A.pk_voucher ID,A2.pk_detail autoid,A2.explanation cdigest,A4.code ccode,A5.name cexch_name,A2.detailindex inid,A2.localdebitamount md,A7.code cdept_id from　gl_voucher A left join bd_vouchertype A1 on A1.pk_vouchertype=A.pk_vouchertype left join gl_detail A2 on A2.Pk_Voucher =A.Pk_Voucher left join bd_accasoa A3 on A3.pk_accasoa=A2.pk_accasoa left join bd_account A4 on A4.pk_account=A3.pk_account left join bd_currtype A5 on A5.pk_currtype=A2.pk_currtype left join gl_docfree1 A6 on A6.ASSID=A2.ASSID left join org_dept A7 on A7.PK_DEPT=A6.F1 where substr(A.prepareddate,0,10) between '" + startTime + "' and '" + endTime + "' and  A.pk_org='0001A110000000001V70' and A.DR!=1 and A2.direction='D' and A4.code in(" + connNCSubjectEncoding + ")";
                }
                else
                {
                    if (updateCount > 0)
                    {
                        sql = " select A.pk_voucher ID,A2.pk_detail autoid,A2.explanation cdigest,A4.code ccode,A5.name cexch_name,A2.detailindex inid,A2.localdebitamount md,A7.code cdept_id from　gl_voucher A left join bd_vouchertype A1 on A1.pk_vouchertype=A.pk_vouchertype left join gl_detail A2 on A2.Pk_Voucher =A.Pk_Voucher left join bd_accasoa A3 on A3.pk_accasoa=A2.pk_accasoa left join bd_account A4 on A4.pk_account=A3.pk_account left join bd_currtype A5 on A5.pk_currtype=A2.pk_currtype left join gl_docfree1 A6 on A6.ASSID=A2.ASSID left join org_dept A7 on A7.PK_DEPT=A6.F1 where substr(A.prepareddate,0,10) between '" + startTime + "' and '" + endTime + "' and  A.pk_org='0001A110000000001V70' and A.DR!=1 and A2.direction='D' and A4.code in(" + connNCSubjectEncoding + ") and " + strGetOracleSQLIn + "";
                    }
                    else
                    {
                        sql = " select A.pk_voucher ID,A2.pk_detail autoid,A2.explanation cdigest,A4.code ccode,A5.name cexch_name,A2.detailindex inid,A2.localdebitamount md,A7.code cdept_id from　gl_voucher A left join bd_vouchertype A1 on A1.pk_vouchertype=A.pk_vouchertype left join gl_detail A2 on A2.Pk_Voucher =A.Pk_Voucher left join bd_accasoa A3 on A3.pk_accasoa=A2.pk_accasoa left join bd_account A4 on A4.pk_account=A3.pk_account left join bd_currtype A5 on A5.pk_currtype=A2.pk_currtype left join gl_docfree1 A6 on A6.ASSID=A2.ASSID left join org_dept A7 on A7.PK_DEPT=A6.F1 where substr(A.prepareddate,0,10) between '" + startTime + "' and '" + endTime + "' and  A.pk_org='0001A110000000001V70' and A.DR!=1 and A2.direction='D' and A4.code in(" + connNCSubjectEncoding + ")";
                    }
                }
                DataSet GlVoucherLine = OracleHelper.ExecuteDataset(sql);

                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id( 'gl_voucher') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table gl_voucher(ID nvarchar(30) primary key,csign nvarchar(50),ddate nvarchar(20),ino_id smallint,idoc smallint,iyperiod nvarchar(20),ts nvarchar(50),zt bit default 0,memo text)";
                    SqlHelper.ExecuteNonQuery(createSql);
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(GlVoucher, "gl_voucher");
                    SqlHelper.ExecuteNonQuery(str.ToString());
                    msg = "凭证表插入成功";
                }
                else
                {
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(GlVoucher, "gl_voucher");
                    if (!string.IsNullOrEmpty(str.ToString()))
                    {
                        SqlHelper.ExecuteNonQuery(str.ToString());
                        msg = "凭证表更新成功";
                    }
                    else
                    {
                        msg = "凭证表暂无可更新数据";
                    }
                }
                tableExist = "if object_id( 'gl_vouchers') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table gl_vouchers(ID nvarchar(30),autoid nvarchar(30) primary key,cdigest nvarchar(500),ccode nvarchar(50),cexch_name nvarchar(50),inid smallint,md decimal(18,2),cdept_id  nvarchar(500))";
                    SqlHelper.ExecuteNonQuery(createSql);
                    StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(GlVoucherLine, "gl_vouchers");
                    SqlHelper.ExecuteNonQuery(strs.ToString());
                    msg = "凭证表行插入成功";
                }
                else
                {
                    StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(GlVoucherLine, "gl_vouchers");
                    if (!string.IsNullOrEmpty(strs.ToString()))
                    {
                        SqlHelper.ExecuteNonQuery(strs.ToString());
                        msg = "凭证表行更新成功";
                    }
                    else
                    {
                        msg = "凭证表暂无可更新数据";
                    }
                }
                GetU8SVApiUrlApi("pingzApi");
                result = msg;
            }
            catch (Exception e)
            {

                result = "凭证表错误：" + e.Message;
            }
            return result;

        }



        /// <summary>
        /// 从nc获取物料档案插入到sql
        /// 创建人：lvhe
        /// 创建时间：2020-3-29 23:20:27
        /// </summary>
        /// <returns></returns>
        [WebMethod]
        private string GetInventoryToSql()
        {
            string result = "";
            string createSql = "";
            string tableExist = "";
            int existResult = 0;
            string msg = "";
            string sql = "";
            StringBuilder strbu = new StringBuilder();
            try
            {
                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id( 'CBO_Inventory') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    //获取物料档案数据
                    sql = "select code,name from bd_material A left join bd_marorg A1 on A1.pk_material = A.pk_material where  A1.pk_org='0001A110000000001V70'";
                }
                else
                {
                    string delstr = "delete from CBO_Inventory";
                    SqlHelper.ExecuteNonQuerys(delstr);
                    sql = "select code,name from bd_material A left join bd_marorg A1 on A1.pk_material = A.pk_material where  A1.pk_org='0001A110000000001V70'";
                }
                DataSet Inventory = OracleHelper.ExecuteDataset(sql);

                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id( 'CBO_Inventory') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table CBO_Inventory(code nvarchar(500),name nvarchar(500))";
                    SqlHelper.ExecuteNonQuery(createSql);
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(Inventory, "CBO_Inventory");
                    SqlHelper.ExecuteNonQuery(str.ToString());
                    msg = "物料档案表插入成功";
                }
                else
                {
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(Inventory, "CBO_Inventory");
                    if (!string.IsNullOrEmpty(str.ToString()))
                    {
                        SqlHelper.ExecuteNonQuery(str.ToString());
                        msg = "物料档案表更新成功";
                    }
                }
                GetU8SVApiUrlApi("wulApi");
                result = msg;
            }
            catch (Exception e)
            {

                result = "物料档案表：" + e.Message;
            }
            return result;
        }

        /// <summary>
        /// 从nc获取供应商插入到sql
        /// 创建人：lvhe
        /// 创建时间：2020-3-29 23:20:27
        /// </summary>
        /// <returns></returns>
        [WebMethod]
        private string GetSupplierToSql()
        {
            string result = "";
            string createSql = "";
            string tableExist = "";
            int existResult = 0;
            string msg = "";
            string sql = "";
            StringBuilder strbu = new StringBuilder();
            try
            {
                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id( 'CBO_Supplier') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    //获取供应商档案数据
                    sql = "select code,name from bd_Supplier A left join bd_suporg A1 on A1.pk_supplier = A. pk_supplier where A1.pk_org='0001A110000000001V70'";
                }
                else
                {
                    string delstr = "delete from CBO_Supplier";
                    SqlHelper.ExecuteNonQuerys(delstr);
                    sql = "select code,name from bd_Supplier A left join bd_suporg A1 on A1.pk_supplier = A. pk_supplier where A1.pk_org='0001A110000000001V70'";
                }
                DataSet Supplier = OracleHelper.ExecuteDataset(sql);

                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id( 'CBO_Supplier') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table CBO_Supplier(code nvarchar(50),name nvarchar(50))";
                    SqlHelper.ExecuteNonQuery(createSql);
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(Supplier, "CBO_Supplier");
                    SqlHelper.ExecuteNonQuery(str.ToString());
                    msg = "供应商档案表插入成功";
                }
                else
                {
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(Supplier, "CBO_Supplier");
                    if (!string.IsNullOrEmpty(str.ToString()))
                    {
                        SqlHelper.ExecuteNonQuery(str.ToString());
                        msg = "供应商档案表更新成功";
                    }
                }
                GetU8SVApiUrlApi("gysApi");
                result = msg;
            }
            catch (Exception e)
            {

                result = "供应商档案表：" + e.Message;
            }
            return result;
        }

        /// <summary>
        /// 从nc获取客户档案插入到sql
        /// 创建人：lvhe
        /// 创建时间：2020-3-29 23:20:27
        /// </summary>
        /// <returns></returns>
        [WebMethod]
        private string GetCustomerToSql()
        {
            string result = "";
            string createSql = "";
            string tableExist = "";
            int existResult = 0;
            string msg = "";
            string sql = "";
            StringBuilder strbu = new StringBuilder();
            try
            {
                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id( 'CBO_Customer') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    //获取客户档案数据
                    sql = "select code,name from bd_customer A left join bd_custorg A1 on A1.pk_customer=A.pk_customer where A1.pk_org='0001A110000000001V70'";
                }
                else
                {
                    string delstr = "delete from CBO_Customer";
                    SqlHelper.ExecuteNonQuerys(delstr);
                    sql = "select code,name from bd_customer A left join bd_custorg A1 on A1.pk_customer=A.pk_customer where A1.pk_org='0001A110000000001V70'";
                }
                DataSet Customer = OracleHelper.ExecuteDataset(sql);

                //判断当前表是否存在 1存在 0 不存在
                tableExist = "if object_id( 'CBO_Customer') is not null select 1 else select 0";
                existResult = SqlHelper.ExecuteNonQuerys(tableExist);

                if (existResult == 0)
                {
                    createSql = "create table CBO_Customer(code nvarchar(50),name nvarchar(50))";
                    SqlHelper.ExecuteNonQuery(createSql);
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(Customer, "CBO_Customer");
                    SqlHelper.ExecuteNonQuery(str.ToString());
                    msg = "客户档案表插入成功";
                }
                else
                {
                    StringBuilder str = DataSetToArrayList.DataSetToArrayLists(Customer, "CBO_Customer");
                    if (!string.IsNullOrEmpty(str.ToString()))
                    {
                        SqlHelper.ExecuteNonQuery(str.ToString());
                        msg = "客户档案表更新成功";
                    }
                }
                GetU8SVApiUrlApi("kehuApi");
                result = msg;
            }
            catch (Exception e)
            {

                result = "客户档案表：" + e.Message;
            }
            return result;
        }

        /// <summary>
        /// 单据审核接口
        /// 创建人：lvhe
        /// 创建时间：2020-8-21 16:40:37
        /// </summary>
        /// <returns></returns>
        [WebMethod]
        private string DoApprove()
        {
            string result = "";
            try
            {
                GetU8SVApiUrlApi("djshapi");
                result = "审核成功";
            }
            catch (Exception e)
            {
                result = "单据审核：" + e.Message;
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


        /// <summary>
        /// 调用凭证Api接口
        /// 创建人:lvhe
        /// 创建时间：2020-3-10 14:13:51
        /// </summary>
        /// <returns></returns>
        private string GetU8SVApiUrlApi(string apiParams)
        {
            string interfaceParameters = apiParams;

            string U8SVApiUrl = u8SVApiUrl;
            string webApiUrl = ""; //调用url
            string webApiType = "Get";//请求方式
            string webApiParam = "";//调用参数
            switch (interfaceParameters)
            {
                //查询
                //case "chaxu":
                //    webApiUrl = "http://erp.test.cvming.com/u8sv/ERPU8/Rdrecord01Add";
                //    break;
                //采购入库单接口
                case "cgrkapi":
                    webApiUrl = U8SVApiUrl + "ERPU8/Rdrecord01Add";
                    webApiType = "Post";
                    webApiParam = "cgrkapi";
                    break;
                //采购发票接口
                case "cgfpapi":
                    webApiUrl = U8SVApiUrl + "ERPU8/PurBillVouchAdd";
                    webApiType = "Post";
                    webApiParam = "cgfpapi";
                    break;
                //销售发票接口
                case "xsfpapi":
                    webApiUrl = U8SVApiUrl + "ERPU8/SaleBillVouchAdd";
                    webApiType = "Post";
                    webApiParam = "xsfpapi";
                    break;
                //发货单接口
                case "fhdapi":
                    webApiUrl = U8SVApiUrl + "ERPU8/DispatchListAdd";
                    webApiType = "Post";
                    webApiParam = "fhdapi";
                    break;
                //材料出库接口
                case "clckapi":
                    webApiUrl = U8SVApiUrl + "ERPU8/RdRecord11Add";
                    webApiType = "Post";
                    webApiParam = "clckapi";
                    break;
                //产成品入库接口
                case "ccprkapi":
                    webApiUrl = U8SVApiUrl + "ERPU8/RdRecord10Add";
                    webApiType = "Post";
                    webApiParam = "ccprkapi";
                    break;
                //其他入库单接口
                case "qtrkdapi":
                    webApiUrl = U8SVApiUrl + "ERPU8/RdRecord08Add";
                    webApiType = "Post";
                    webApiParam = "qtrkdapi";
                    break;
                //其他出库单接口
                case "qtckdapi":
                    webApiUrl = U8SVApiUrl + "ERPU8/RdRecord09Add";
                    webApiType = "Post";
                    webApiParam = "qtckdapi";
                    break;
                //调拨单接口
                case "dbdapi":
                    webApiUrl = U8SVApiUrl + "ERPU8/TransVouchAdd";
                    webApiType = "Post";
                    webApiParam = "dbdapi";
                    break;
                //形态转换单单接口
                case "xtzhdapi":
                    webApiUrl = U8SVApiUrl + "ERPU8/AssemVouchAdd";
                    webApiType = "Post";
                    webApiParam = "xtzhdapi";
                    break;
                //凭证单接口
                case "pingzApi":
                    webApiUrl = U8SVApiUrl + "ERPU8/CreateGL";
                    webApiType = "Post";
                    webApiParam = "pingzApi";
                    break;
                //物料接口
                case "wulApi":
                    webApiUrl = U8SVApiUrl + "ERPU8/InventoryUpdate";
                    webApiType = "Post";
                    webApiParam = "wulApi";
                    break;
                //供应商接口
                case "gysApi":
                    webApiUrl = U8SVApiUrl + "ERPU8/SupplierUpdate";
                    webApiType = "Post";
                    webApiParam = "gysApi";
                    break;
                //客户接口
                case "kehuApi":
                    webApiUrl = U8SVApiUrl + "ERPU8/CustomerUpdate";
                    webApiType = "Post";
                    webApiParam = "kehuApi";
                    break;
                //批量更新产成品入库api接口
                case "plgxccprkapi":
                    webApiUrl = U8SVApiUrl + "ERPU8/RdRecord10ItemUpdate";
                    webApiType = "Post";
                    webApiParam = "plgxccprkapi";
                    break;
                case "djshapi":
                    webApiUrl = U8SVApiUrl + "ERPU8/DoApprove";
                    webApiType = "Post";
                    webApiParam = "djshapi";
                    break;
                case "wlqdApi":
                    webApiUrl = U8SVApiUrl + "ERPU8/CreateBOM";
                    webApiType = "Post";
                    webApiParam = "wlqdApi";
                    break;
                default:
                    webApiUrl = "请检查参数是否正确,没有找到输入参数对应的接口信息！";
                    break;
            }
            //接口返回信息
            string msg = "";
            if (!string.IsNullOrEmpty(webApiUrl) && webApiUrl != "请检查参数是否正确,没有找到输入参数对应的接口信息！")
            {
                try
                {
                    msg = HttpApi(webApiUrl, "{}", webApiType, webApiParam);
                }
                catch (Exception e)
                {
                    msg = e.Message;
                }
            }
            else
            {
                msg = "请检查参数是否正确,没有找到输入参数对应的接口信息！";
            }
            return msg;
        }


        #region <<调用webApi接口>>
        /// <summary>
        /// 调用api返回json
        /// </summary>
        /// <param name="url">api地址</param>
        /// <param name="jsonstr">接收参数</param>
        /// <param name="type">类型</param>
        /// <returns></returns>
        public static string HttpApi(string url, string jsonstr, string type, string webApiParam)
        {
            string result = "";
            if (type.ToUpper().ToString() == "POST")
            {
                result = HttpPost(url, jsonstr, type, webApiParam);
            }
            else
            {
                result = HttpGet(url, webApiParam);
            }
            return result;
        }

        public static string HttpGet(string url, string webApiParam)
        {
            string msg = "";
            switch (webApiParam)
            {
                //查询
                //case "chaxu":
                //    webApiUrl = "http://erp.test.cvming.com/u8sv/ERPU8/Rdrecord01Add";
                //    break;
                //采购入库单接口
                case "cgrkapi":
                    msg = "采购入库单接口";
                    break;
                //采购发票接口
                case "cgfpapi":
                    msg = "采购发票接口";
                    break;
                //销售发票接口
                case "xsfpapi":
                    msg = "销售发票接口";
                    break;
                //发货单接口
                case "fhdapi":
                    msg = "发货单接口";
                    break;
                //材料出库接口
                case "clckapi":
                    msg = "材料出库接口";
                    break;
                //产成品入库接口
                case "ccprkapi":
                    msg = "产成品入库接口";
                    break;
                //其他入库单接口
                case "qtrkdapi":
                    msg = "销售发票接口";
                    break;
                //其他出库单接口
                case "qtckdapi":
                    msg = "其他出库单接口";
                    break;
                //调拨单接口
                case "dbdapi":
                    msg = "调拨单接口";
                    break;
                //形态转换单单接口
                case "xtzhdapi":
                    msg = "形态转换单单接口";
                    break;
                case "wulApi":
                    msg = "物料档案接口";
                    break;
                case "gysApi":
                    msg = "供应商档案接口";
                    break;
                case "kehuApi":
                    msg = "客户档案接口";
                    break;
                case "plgxccprkapi":
                    msg = "批量更新产成品项目号接口";
                    break;
                case "djshapi":
                    msg = "单据审核接口";
                    break;
                case "wlqdApi":
                    msg = "物料订单接口";
                    break;
                default:
                    msg = "请检查参数是否正确,没有找到输入参数对应的接口信息！";
                    break;
            }
            try
            {
                //ServicePointManager.ServerCertificateValidationCallback = new RemoteCertificateValidationCallback(CheckValidationResult);
                Encoding encoding = Encoding.UTF8;
                ServicePointManager.ServerCertificateValidationCallback = new System.Net.Security.RemoteCertificateValidationCallback(CheckValidationResult);//验证服务器证书回调自动验证
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
                request.Method = "GET";
                request.Accept = "text/html, application/xhtml+xml, */*";
                request.ContentType = "application/json";

                HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                using (StreamReader reader = new StreamReader(response.GetResponseStream(), Encoding.UTF8))
                {
                    msg = msg + "调用成功";
                    return msg;
                }
            }
            catch (Exception ex)
            {
                msg = msg + "调用失败：" + ex.Message;
                return msg;
            }

        }

        public static string HttpPost(string url, string jsonstr, string type, string webApiParam)
        {
            string msg = "";
            switch (webApiParam)
            {
                //查询
                //case "chaxu":
                //    webApiUrl = "http://erp.test.cvming.com/u8sv/ERPU8/Rdrecord01Add";
                //    break;
                //采购入库单接口
                case "cgrkapi":
                    msg = "采购入库单接口";
                    break;
                //采购发票接口
                case "cgfpapi":
                    msg = "采购发票接口";
                    break;
                //销售发票接口
                case "xsfpapi":
                    msg = "销售发票接口";
                    break;
                //发货单接口
                case "fhdapi":
                    msg = "发货单接口";
                    break;
                //材料出库接口
                case "clckapi":
                    msg = "材料出库接口";
                    break;
                //产成品入库接口
                case "ccprkapi":
                    msg = "产成品入库接口";
                    break;
                //其他入库单接口
                case "qtrkdapi":
                    msg = "销售发票接口";
                    break;
                //其他出库单接口
                case "qtckdapi":
                    msg = "其他出库单接口";
                    break;
                //调拨单接口
                case "dbdapi":
                    msg = "调拨单接口";
                    break;
                //形态转换单单接口
                case "xtzhdapi":
                    msg = "形态转换单单接口";
                    break;
                case "pingzApi":
                    msg = "凭证单单接口";
                    break;
                case "wulApi":
                    msg = "物料档案接口";
                    break;
                case "gysApi":
                    msg = "供应商档案接口";
                    break;
                case "kehuApi":
                    msg = "客户档案接口";
                    break;
                case "plgxccprkapi":
                    msg = "批量更新产成品项目号接口";
                    break;
                case "djshapi":
                    msg = "单据审核接口";
                    break;
                case "wlqdApi":
                    msg = "物料清单接口";
                    break;
                default:
                    msg = "请检查参数是否正确,没有找到输入参数对应的接口信息！";
                    break;
            }
            try
            {
                Encoding encoding = Encoding.UTF8;
                ServicePointManager.ServerCertificateValidationCallback = new System.Net.Security.RemoteCertificateValidationCallback(CheckValidationResult);//验证服务器证书回调自动验证
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);//webrequest请求api地址
                request.Accept = "text/html,application/xhtml+xml,*/*";
                request.ContentType = "application/json";
                request.Method = type.ToUpper().ToString();//get或者post
                byte[] buffer = encoding.GetBytes(jsonstr);
                request.ContentLength = buffer.Length;
                request.GetRequestStream().Write(buffer, 0, buffer.Length);
                HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                using (StreamReader reader = new StreamReader(response.GetResponseStream(), Encoding.UTF8))
                {
                    msg = msg + "调用成功";
                    return msg;
                }
            }

            catch (Exception ex)
            {
                msg = msg + "调用失败：" + ex.Message;
                return msg;
            }
        }


        public static bool CheckValidationResult(object sender, X509Certificate certificate, X509Chain chain, SslPolicyErrors errors)
        {   // 总是接受  
            return true;
        }
        #endregion

    }
}
