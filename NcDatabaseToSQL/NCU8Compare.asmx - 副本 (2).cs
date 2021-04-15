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
using static NcDatabaseToSQL.SqlHelperForU8;

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


		private static string connU8KmdzCode = ConfigurationManager.ConnectionStrings["U8KmdzCode"].ToString();
		private static string connNCKmdzCode = ConfigurationManager.ConnectionStrings["NCKmdzCode"].ToString();
		//string startTime = "2019-09-01";
		//string endTime = "2019-09-30";
		//[WebMethod]
		//public string HelloWorld()
		//{
		//    return "Hello World";
		//}

		string queryDate = "";
		int state = 0;
		string year = "";
		string month = "";
		string date = "";
		bool ResponseBool = true;
		string ResponseMsg = "获取成功";
		/// <summary>
		/// 公共调用方法
		/// 创建人：吕贺
		/// 创建时间：2019年10月23日 13:55:45
		/// </summary>
		/// <returns></returns>
		[WebMethod]
		public string NcInsertToSql(string queryDateParameters, string dataType)
		{
			string msg = "";
			queryDate = queryDateParameters;

			year = queryDate.Substring(0, 4);
			month = queryDate.Substring(queryDate.Length - 2, 2);

			date = year + month;

			if (string.IsNullOrEmpty(queryDate))
			{
				string delstrApps = "truncate table Spl_NCU8Compare";
				SqlHelperForApps.ExecuteNonQuerys(delstrApps);
				msg = "请先选择查询条件";
			}
			else
			{
				if (dataType == "出入库对比")
				{
					GetPurchaseinToSql();
					GetDispatchListToSql();
					GetWhstransToSql();
					GetGeneraloutToSql();
					GetGeneralinToSql();
					GetTransformToSql();
					GetFinprodInToSql();//产成品入库
					GetMaterialToSql();//材料出库
					msg = Ncu8Compare();
				}
				if (dataType == "采购入库")
				{

				}
				if (dataType == "采购发票")
				{
					GetPurBillVouchToSql();
					msg = Ncu8CompareForCGFP();
				}
				if (dataType == "销售发票")
				{
					GetSaleBillVouchToSql();
					msg = Ncu8CompareForXSFP();
				}
				if (dataType == "科目余额")
				{
					GetGlDetailVouchToSql();
					msg = Ncu8CompareForPz();
				}
				if (dataType == "生产领料")
				{
					msg = WarehousingCompareMaterialOut();
				}
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
					sql = "select '采购入库' cbustype,A.cCode docNo,A1.cInvCode cInvCode,isnull(SUM(A1.iQuantity),0) iQuantity,A.cWhCode publicsec1,A2.cWhName publicsec2,GETDATE() as CreateTime from RdRecord01 A left join RdRecords01 A1 on A.ID=A1.ID left join Warehouse A2 on A2.cWhCode=A.cWhCode WHERE Convert(nvarchar(7),A.dDate,121)='" + queryDate + "' GROUP BY A1.cInvCode,A.cWhCode,A2.cWhName,A.cCode";
				}
				else
				{
					string delstr = "delete from Spl_U8DBData where cbustype != '采购发票' and cbustype != '销售发票'";
					//delete from [Spl_NCU8Compare] where cbustype = '采购入库'
					SqlHelper.ExecuteNonQuerys(delstr);
					sql = "select '采购入库' cbustype,A.cCode docNo,A1.cInvCode cInvCode,isnull(SUM(A1.iQuantity),0) iQuantity,A.cWhCode publicsec1,A2.cWhName publicsec2,GETDATE() as CreateTime from RdRecord01 A left join RdRecords01 A1 on A.ID=A1.ID left join Warehouse A2 on A2.cWhCode=A.cWhCode WHERE Convert(nvarchar(7),A.dDate,121)='" + queryDate + "' GROUP BY A1.cInvCode,A.cWhCode,A2.cWhName,A.cCode";
				}
				//获取采购入库头数据
				DataSet PurchaseIn = SqlHelperForU8.ExecuteDataset(conneU8ctionString, CommandType.Text, sql);


				//判断当前表是否存在 1存在 0 不存在
				tableExist = "if object_id( 'Spl_NCDBData') is not null select 1 else select 0";
				existResult = SqlHelper.ExecuteNonQuerys(tableExist);

				if (existResult == 0)
				{
					//获取采购入库行数据
					sql = "SELECT '采购入库' cbustype,A.vbillcode docNo,A2.code cInvCode,nvl(sum(A1.nassistnum),0) iQuantity,A5.code publicsec1,A5.name publicsec2,to_char(sysdate,'yyyy-mm-dd hh24:mi:ss')  as CreateTime  FROM ic_purchasein_h A left join ic_purchasein_b A1 on A1.cgeneralhid = A.cgeneralhid and A1.dr!=1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc left join bd_stordoc A5 ON A5.Pk_Stordoc=A.cwarehouseid where A.PK_ORG = '0001A110000000001V70' AND substr(A.dbilldate,0,7)= '" + queryDate + "' and A.fbillflag = 3 and substr(A2.code,0,4) != '0915' and A.cwarehouseid  not in('1001A1100000000T5S5Z','1001A11000000003CYSY') group by A2.code,A.Vtrantypecode,A5.code,A5.name,A.vbillcode";
				}
				else
				{
					//string delstr = "DROP table Spl_NCDBData";
					string delstr = "delete from Spl_NCDBData where cbustype != '采购发票' and cbustype != '销售发票'";
					SqlHelper.ExecuteNonQuerys(delstr);
					sql = "SELECT '采购入库' cbustype,A.vbillcode docNo,A2.code cInvCode,nvl(sum(A1.nassistnum),0) iQuantity,A5.code publicsec1,A5.name publicsec2,to_char(sysdate,'yyyy-mm-dd hh24:mi:ss')  as CreateTime  FROM ic_purchasein_h A left join ic_purchasein_b A1 on A1.cgeneralhid = A.cgeneralhid and A1.dr!=1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc left join bd_stordoc A5 ON A5.Pk_Stordoc=A.cwarehouseid where A.PK_ORG = '0001A110000000001V70' AND substr(A.dbilldate,0,7)= '" + queryDate + "' and A.fbillflag = 3 and substr(A2.code,0,4) != '0915' and A.cwarehouseid  not in('1001A1100000000T5S5Z','1001A11000000003CYSY') group by A2.code,A.Vtrantypecode,A5.code,A5.name,A.vbillcode";
				}

				//获取采购入库行数据
				DataSet PurchaseInLine = OracleHelper.ExecuteDataset(sql);

				//判断当前表是否存在 1存在 0 不存在
				tableExist = "if object_id( 'Spl_U8DBData') is not null select 1 else select 0";
				existResult = SqlHelper.ExecuteNonQuerys(tableExist);

				if (existResult == 0)
				{
					createSql = "CREATE TABLE [dbo].[Spl_U8DBData]([Id] [int] IDENTITY (1, 1) NOT NULL,[cbustype] [varchar](200) NULL,[docNo] [varchar](200) NULL,[cRdCode] [varchar](200) NULL,[cInvCode] [varchar](200) NULL,[iQuantity] [decimal](18, 2) NULL,[publicsec1] [varchar](200) NULL,[publicsec2] [varchar](200) NULL,[publicsec3] [varchar](200) NULL,[publicsec4] [varchar](200) NULL,[CreateTime] [datetime] NOT NULL)";
					SqlHelper.ExecuteNonQuery(createSql);
					//StringBuilder str = DataSetToArrayList.DataSetToArrayLists(PurchaseIn, "Spl_U8DBData");
					//SqlHelper.ExecuteNonQuery(str.ToString());

					SqlBulkCopyHelper.ImportTempTableDataIndexForCrk(PurchaseIn, "Spl_U8DBData");

					//msg = "采购入库U8数据插入成功";
				}
				else
				{
					//StringBuilder str = DataSetToArrayList.DataSetToArrayLists(PurchaseIn, "Spl_U8DBData");
					//SqlHelper.ExecuteNonQuery(str.ToString());

					SqlBulkCopyHelper.ImportTempTableDataIndexForCrk(PurchaseIn, "Spl_U8DBData");
					//msg = "采购入库U8数据更新成功";
				}

				tableExist = "if object_id( 'Spl_NCDBData') is not null select 1 else select 0";
				existResult = SqlHelper.ExecuteNonQuerys(tableExist);

				if (existResult == 0)
				{
					createSql = "CREATE TABLE [dbo].[Spl_NCDBData]([Id] [int] IDENTITY (1, 1) NOT NULL,[cbustype] [varchar](200) NULL,[docNo] [varchar](200) NULL,[cRdCode] [varchar](200) NULL,[cInvCode] [varchar](200) NULL,[iQuantity] [decimal](18, 2) NULL,[publicsec1] [varchar](200) NULL,[publicsec2] [varchar](200) NULL,[publicsec3] [varchar](200) NULL,[publicsec4] [varchar](200) NULL,[CreateTime] [datetime] NOT NULL)";
					SqlHelper.ExecuteNonQuery(createSql);
					//StringBuilder str = DataSetToArrayList.DataSetToArrayLists(PurchaseInLine, "Spl_NCDBData");
					//SqlHelper.ExecuteNonQuery(str.ToString());


					SqlBulkCopyHelper.ImportTempTableDataIndexForCrk(PurchaseInLine, "Spl_NCDBData");
					// msg = "采购入库NC数据插入成功";
				}
				else
				{
					//StringBuilder str = DataSetToArrayList.DataSetToArrayLists(PurchaseInLine, "Spl_NCDBData");
					//SqlHelper.ExecuteNonQuery(str.ToString());

					SqlBulkCopyHelper.ImportTempTableDataIndexForCrk(PurchaseInLine, "Spl_NCDBData");
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
				sql = "select '销售出库' cbustype,A.cDLCode docNo,A1.cInvCode cInvCode,isnull(SUM(A1.iQuantity),0) iQuantity,A1.cWhCode publicsec1,A2.cWhName publicsec2,GETDATE() as CreateTime from DispatchList A left join DispatchLists A1 on A.DLID=A1.DLID left join Warehouse A2 on A2.cWhCode=A1.cWhCode WHERE Convert(nvarchar(7),A.dDate,121)='" + queryDate + "' GROUP BY A1.cInvCode,A1.cWhCode,A2.cWhName,A.cDLCode";

				//获取销售出库数据
				DataSet Dispatch = SqlHelperForU8.ExecuteDataset(conneU8ctionString, CommandType.Text, sql);

				//获取销售出库数据
				sql = "select'销售出库' cbustype,A.vbillcode docNo,A2.code cInvCode,nvl(sum(A1.nassistnum),0) iQuantity,A6.code publicsec1,A6.Name publicsec2,to_char(sysdate,'yyyy-mm-dd hh24:mi:ss')  as CreateTime from ic_saleout_h A left join ic_saleout_b A1 on A.cgeneralhid = A1.cgeneralhid and A1.DR != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc left join bd_billtype A5 on A5.pk_billtypeid =A.ctrantypeid left join bd_stordoc A6 on A6.pk_stordoc =A.cwarehouseid  where A.PK_ORG = '0001A110000000001V70' and A.DR != 1 AND substr(A.dbilldate,0,7)= '" + queryDate + "'  and A.fbillflag = 3 and substr(A2.code,0,4) != '0915' and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY') group by A2.code,A5.pk_billtypecode,A6.code,A6.Name,A.vbillcode";

				//获取销售出库数据
				DataSet Dispatchs = OracleHelper.ExecuteDataset(sql);

				//StringBuilder str = DataSetToArrayList.DataSetToArrayLists(Dispatch, "Spl_U8DBData");
				//SqlHelper.ExecuteNonQuery(str.ToString());

				SqlBulkCopyHelper.ImportTempTableDataIndexForCrk(Dispatch, "Spl_U8DBData");

				//StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(Dispatchs, "Spl_NCDBData");
				//SqlHelper.ExecuteNonQuery(strs.ToString());

				SqlBulkCopyHelper.ImportTempTableDataIndexForCrk(Dispatchs, "Spl_NCDBData");
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
			state = 1;
			string result = "";
			string sql = "";
			string tableExist = "";
			string createSql = "";
			int existResult = 0;
			StringBuilder strbu = new StringBuilder();
			try
			{
				//获取采购发票数据
				sql = "select '采购发票' cbustype,ISNULL(A.cDefine11,A.cPBVCode) docNo,ISNULL(A.cDefine11,A.cPBVCode) publicsec4,A1.cInvCode cInvCode,A.cVenCode publicsec1,A2.cVenName publicsec3,isnull(SUM(A1.iPBVQuantity),0) iQuantity,Convert(decimal(18,2),isnull(SUM(A1.iSum),0)) publicsec2,GETDATE() as CreateTime from PurBillVouch A left join PurBillVouchs A1 on A.PBVID=A1.PBVID left join Vendor A2 on A.cVenCode=A2.cVenCode WHERE Convert(nvarchar(7),A.dPBVDate,121)='" + queryDate + "' GROUP BY A1.cInvCode,A.cPBVBillType,A.cVenCode,A2.cVenName,A.cDefine11,A.cPBVCode";

				//获取采购发票数据
				DataSet PurBillVouch = SqlHelperForU8.ExecuteDataset(conneU8ctionString, CommandType.Text, sql);

				//获取采购发票数据
				sql = "select '采购发票' cbustype,A.vbillcode docNo,A.vbillcode publicsec4,A2.code cInvCode,A3.code publicsec1,A3.Name publicsec3,nvl(sum(A1.nastnum),0) iQuantity,decode(nvl(sum(A1.ntaxmny),0),0,'0.00',trim(to_char(nvl(sum(A1.ntaxmny),0),'9999999.99'))) publicsec2,to_char(sysdate,'yyyy-mm-dd hh24:mi:ss')  as CreateTime from po_invoice A left join po_invoice_b A1 on A.PK_INVOICE = A1.PK_INVOICE and A1.DR!=1 left join bd_material A2 on A1.pk_material = A2.pk_material left join bd_supplier A3 on A.PK_SUPPLIER=A3.PK_SUPPLIER where A.PK_ORG = '0001A110000000001V70' and A.DR != 1 AND substr(A.taudittime,0,7)= '" + queryDate + "' and A.fbillstatus = 3 and substr(A2.code,0,4) != '0915' group by A2.code,A.Vtrantypecode,A3.code,A3.Name,A.vbillcode";
				//获取采购发票数据
				DataSet PurBillVouchs = OracleHelper.ExecuteDataset(sql);

				//判断当前表是否存在 1存在 0 不存在
				tableExist = "if object_id( 'Spl_U8DBData') is not null select 1 else select 0";
				existResult = SqlHelper.ExecuteNonQuerys(tableExist);

				if (existResult == 0)
				{
					createSql = "CREATE TABLE [dbo].[Spl_U8DBData]([Id] [int] IDENTITY (1, 1) NOT NULL,[cbustype] [varchar](200) NULL,[docNo] [varchar](200) NULL,[cRdCode] [varchar](200) NULL,[cInvCode] [varchar](200) NULL,[iQuantity] [decimal](18, 2) NULL,[publicsec1] [varchar](200) NULL,[publicsec2] [varchar](200) NULL,[publicsec3] [varchar](200) NULL,[publicsec4] [varchar](200) NULL,[CreateTime] [datetime] NOT NULL)";
					SqlHelper.ExecuteNonQuery(createSql);
					//StringBuilder strp = DataSetToArrayList.DataSetToArrayLists(PurBillVouch, "Spl_U8DBData");
					//SqlHelper.ExecuteNonQuery(strp.ToString());

					SqlBulkCopyHelper.ImportTempTableDataIndexForCG(PurBillVouch, "Spl_U8DBData");
					//msg = "采购入库U8数据插入成功";
				}
				else
				{
					string delstr = "delete from Spl_U8DBData where cbustype = '采购发票'";
					//delete from [Spl_NCU8Compare] where cbustype = '采购入库'
					SqlHelper.ExecuteNonQuerys(delstr);
					//StringBuilder strps = DataSetToArrayList.DataSetToArrayLists(PurBillVouch, "Spl_U8DBData");
					//SqlHelper.ExecuteNonQuery(strps.ToString());

					SqlBulkCopyHelper.ImportTempTableDataIndexForCG(PurBillVouch, "Spl_U8DBData");
					//msg = "采购入库U8数据更新成功";
				}

				tableExist = "if object_id( 'Spl_NCDBData') is not null select 1 else select 0";
				existResult = SqlHelper.ExecuteNonQuerys(tableExist);

				if (existResult == 0)
				{
					createSql = "CREATE TABLE [dbo].[Spl_NCDBData]([Id] [int] IDENTITY (1, 1) NOT NULL,[cbustype] [varchar](200) NULL,[docNo] [varchar](200) NULL,[cRdCode] [varchar](200) NULL,[cInvCode] [varchar](200) NULL,[iQuantity] [decimal](18, 2) NULL,[publicsec1] [varchar](200) NULL,[publicsec2] [varchar](200) NULL,[publicsec3] [varchar](200) NULL,[publicsec4] [varchar](200) NULL,[CreateTime] [datetime] NOT NULL)";
					SqlHelper.ExecuteNonQuery(createSql);
					//StringBuilder strp = DataSetToArrayList.DataSetToArrayLists(PurBillVouchs, "Spl_NCDBData");
					//SqlHelper.ExecuteNonQuery(strp.ToString());

					SqlBulkCopyHelper.ImportTempTableDataIndexForCG(PurBillVouchs, "Spl_NCDBData");
					// msg = "采购入库NC数据插入成功";
				}
				else
				{
					string delstr = "delete from Spl_NCDBData where cbustype = '采购发票'";
					//delete from [Spl_NCU8Compare] where cbustype = '采购入库'
					SqlHelper.ExecuteNonQuerys(delstr);
					//StringBuilder strps = DataSetToArrayList.DataSetToArrayLists(PurBillVouchs, "Spl_NCDBData");
					//SqlHelper.ExecuteNonQuery(strps.ToString());

					SqlBulkCopyHelper.ImportTempTableDataIndexForCG(PurBillVouchs, "Spl_NCDBData");
					// msg = "采购入库NC数据更新成功";
				}


				//StringBuilder str = DataSetToArrayList.DataSetToArrayLists(PurBillVouch, "Spl_U8DBData");
				//SqlHelper.ExecuteNonQuery(str.ToString());

				//StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(PurBillVouchs, "Spl_NCDBData");
				//SqlHelper.ExecuteNonQuery(strs.ToString());
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
			state = 1;
			string result = "";
			string sql = "";
			string tableExist = "";
			string createSql = "";
			int existResult = 0;
			StringBuilder strbu = new StringBuilder();
			try
			{
				//获取采购发票数据
				sql = "select '销售发票' cbustype,A.cSBVCode docNo,A.cSBVCode publicsec4,A1.cInvCode cInvCode,A.cCusCode publicsec1,A.cCusName publicsec3,isnull(SUM(A1.iQuantity),0) iQuantity,Convert(decimal(18,2),isnull(SUM(A1.iSum),0)) publicsec2,GETDATE() as CreateTime from SaleBillVouch A left join SaleBillVouchs A1 on A1.SBVID=A.SBVID WHERE Convert(nvarchar(7),A.dDate,121)='" + queryDate + "' GROUP BY A1.cInvCode,A.cCusCode,A.cCusName,A.cSBVCode";

				//获取采购发票数据
				DataSet PurBillVouch = SqlHelperForU8.ExecuteDataset(conneU8ctionString, CommandType.Text, sql);

				//获取采购发票数据
				sql = "select '销售发票' cbustype,A.vbillcode docNo,A.vbillcode publicsec4,A2.code cInvCode,A3.Code publicsec1,A3.Name publicsec3,nvl(sum(A1.nnum),0) iQuantity,decode(nvl(sum(A1.ntaxmny),0),0,'0.00',trim(to_char(nvl(sum(A1.ntaxmny),0),'9999999.99'))) publicsec2,to_char(sysdate,'yyyy-mm-dd hh24:mi:ss')  as CreateTime from so_saleinvoice A left join so_saleinvoice_b A1 on A.csaleinvoiceid = A1.csaleinvoiceid and A1.dr!=1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_customer A3 on  A.cinvoicecustid=A3.pk_customer where A.PK_ORG = '0001A110000000001V70' and A.dr != 1 AND substr(A.dbilldate,0,7)= '" + queryDate + "' and substr(A2.code,0,4) != '0915' and A1.csendstordocid not in('1001A1100000000T5S5Z','1001A11000000003CYSY') GROUP BY A2.code,A3.Code,A3.Name,A.vbillcode";

				//获取采购发票数据
				DataSet PurBillVouchs = OracleHelper.ExecuteDataset(sql);


				//判断当前表是否存在 1存在 0 不存在
				tableExist = "if object_id( 'Spl_U8DBData') is not null select 1 else select 0";
				existResult = SqlHelper.ExecuteNonQuerys(tableExist);

				if (existResult == 0)
				{
					createSql = "CREATE TABLE [dbo].[Spl_U8DBData]([Id] [int] IDENTITY (1, 1) NOT NULL,[cbustype] [varchar](200) NULL,[docNo] [varchar](200) NULL,[cRdCode] [varchar](200) NULL,[cInvCode] [varchar](200) NULL,[iQuantity] [decimal](18, 2) NULL,[publicsec1] [varchar](200) NULL,[publicsec2] [varchar](200) NULL,[publicsec3] [varchar](200) NULL,[publicsec4] [varchar](200) NULL,[CreateTime] [datetime] NOT NULL)";
					SqlHelper.ExecuteNonQuery(createSql);
					//StringBuilder strp = DataSetToArrayList.DataSetToArrayLists(PurBillVouch, "Spl_U8DBData");
					//SqlHelper.ExecuteNonQuery(strp.ToString());

					SqlBulkCopyHelper.ImportTempTableDataIndexForXs(PurBillVouch, "Spl_U8DBData");
					//msg = "采购入库U8数据插入成功";
				}
				else
				{
					string delstr = "delete from Spl_U8DBData where cbustype = '销售发票'";
					//delete from [Spl_NCU8Compare] where cbustype = '采购入库'
					SqlHelper.ExecuteNonQuerys(delstr);
					//StringBuilder strps = DataSetToArrayList.DataSetToArrayLists(PurBillVouch, "Spl_U8DBData");
					//SqlHelper.ExecuteNonQuery(strps.ToString());

					SqlBulkCopyHelper.ImportTempTableDataIndexForXs(PurBillVouch, "Spl_U8DBData");
					//msg = "采购入库U8数据更新成功";
				}

				tableExist = "if object_id( 'Spl_NCDBData') is not null select 1 else select 0";
				existResult = SqlHelper.ExecuteNonQuerys(tableExist);

				if (existResult == 0)
				{
					createSql = "CREATE TABLE [dbo].[Spl_NCDBData]([Id] [int] IDENTITY (1, 1) NOT NULL,[cbustype] [varchar](200) NULL,[docNo] [varchar](200) NULL,[cRdCode] [varchar](200) NULL,[cInvCode] [varchar](200) NULL,[iQuantity] [decimal](18, 2) NULL,[publicsec1] [varchar](200) NULL,[publicsec2] [varchar](200) NULL,[publicsec3] [varchar](200) NULL,[publicsec4] [varchar](200) NULL,[CreateTime] [datetime] NOT NULL)";
					SqlHelper.ExecuteNonQuery(createSql);
					//StringBuilder strp = DataSetToArrayList.DataSetToArrayLists(PurBillVouchs, "Spl_NCDBData");
					//SqlHelper.ExecuteNonQuery(strp.ToString());

					SqlBulkCopyHelper.ImportTempTableDataIndexForXs(PurBillVouchs, "Spl_NCDBData");
					// msg = "采购入库NC数据插入成功";
				}
				else
				{
					string delstr = "delete from Spl_NCDBData where cbustype = '销售发票'";
					//delete from [Spl_NCU8Compare] where cbustype = '采购入库'
					SqlHelper.ExecuteNonQuerys(delstr);
					//StringBuilder strps = DataSetToArrayList.DataSetToArrayLists(PurBillVouchs, "Spl_NCDBData");
					//SqlHelper.ExecuteNonQuery(strps.ToString());

					SqlBulkCopyHelper.ImportTempTableDataIndexForXs(PurBillVouchs, "Spl_NCDBData");
					// msg = "采购入库NC数据更新成功";
				}
				//StringBuilder str = DataSetToArrayList.DataSetToArrayLists(PurBillVouch, "Spl_U8DBData");
				//SqlHelper.ExecuteNonQuery(str.ToString());

				//StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(PurBillVouchs, "Spl_NCDBData");
				//SqlHelper.ExecuteNonQuery(strs.ToString());
			}
			catch (Exception e)
			{

				result = "销售发票插入错误：" + e.Message;
			}
			return result;
		}


		/// <summary>
		/// 从U8获取凭证数据
		/// 创建人：lvhe
		/// 创建时间：2020-4-12 22:05:47
		/// </summary>
		/// <returns></returns>
		[WebMethod]
		private string GetGlDetailVouchToSql()
		{
			state = 1;
			string result = "";
			string sql = "";
			string tableExist = "";
			string createSql = "";
			int existResult = 0;
			StringBuilder strbu = new StringBuilder();
			try
			{
				//获取采购发票数据
				sql = "select  '科目余额' as cbustype, A.ccode as cInvCode,A1.ccode_name as publicsec3,sum(a.md) as iQuantity,sum(a.mc) as publicsec1,GETDATE() as CreateTime from GL_accvouch a left join code A1 on A.ccode=A1.ccode and A1.iyear='" + year + "' where a.iYPeriod='" + date + "' and A.ccode in (" + connU8KmdzCode + ") and isnull(a.iFlag,0)<>1 group by A.ccode,A1.ccode_name order by a.ccode desc";

				//获取采购发票数据
				DataSet GlDetail = SqlHelperForU8.ExecuteDataset(conneU8ctionString, CommandType.Text, sql);

				//获取采购发票数据
				sql = "select '科目余额' as cbustype,case when A.accountcode='22020102' then '220201' else  A.accountcode end as cInvCode,A1.name as publicsec3,sum(A.debitamount) as iQuantity,sum(A.creditamount) as publicsec1,to_char(sysdate,'yyyy-mm-dd hh24:mi:ss')  as CreateTime from gl_detail A left join bd_accasoa A1 on A1.Pk_Accasoa=A.Pk_Accasoa where  A.accountcode in (" + connNCKmdzCode + ")  and A.yearv='" + year + "' and A.periodv='" + month + "' AND a.pk_org='0001A110000000001V70' group by A.accountcode,A1.name order by  A.accountcode desc";

				//获取采购发票数据
				DataSet GlDetails = OracleHelper.ExecuteDataset(sql);


				//判断当前表是否存在 1存在 0 不存在
				tableExist = "if object_id( 'Spl_U8DBData') is not null select 1 else select 0";
				existResult = SqlHelper.ExecuteNonQuerys(tableExist);

				if (existResult == 0)
				{
					createSql = "CREATE TABLE [dbo].[Spl_U8DBData]([Id] [int] IDENTITY (1, 1) NOT NULL,[cbustype] [varchar](200) NULL,[cRdCode] [varchar](200) NULL,[cInvCode] [varchar](200) NULL,[iQuantity] [decimal](18, 2) NULL,[publicsec1] [varchar](200) NULL,[publicsec2] [varchar](200) NULL,[publicsec3] [varchar](200) NULL,[publicsec4] [varchar](200) NULL,[CreateTime] [datetime] NOT NULL)";
					SqlHelper.ExecuteNonQuery(createSql);
					//StringBuilder strp = DataSetToArrayList.DataSetToArrayLists(GlDetail, "Spl_U8DBData");
					//SqlHelper.ExecuteNonQuery(strp.ToString());

					SqlBulkCopyHelper.ImportTempTableDataIndexForKM(GlDetail, "Spl_U8DBData");
					//msg = "采购入库U8数据插入成功";
				}
				else
				{
					string delstr = "delete from Spl_U8DBData where cbustype = '科目余额'";
					//delete from [Spl_NCU8Compare] where cbustype = '采购入库'
					SqlHelper.ExecuteNonQuerys(delstr);
					//StringBuilder strps = DataSetToArrayList.DataSetToArrayLists(GlDetail, "Spl_U8DBData");
					//SqlHelper.ExecuteNonQuery(strps.ToString());

					SqlBulkCopyHelper.ImportTempTableDataIndexForKM(GlDetail, "Spl_U8DBData");
					//msg = "采购入库U8数据更新成功";
				}

				tableExist = "if object_id( 'Spl_NCDBData') is not null select 1 else select 0";
				existResult = SqlHelper.ExecuteNonQuerys(tableExist);

				if (existResult == 0)
				{
					createSql = "CREATE TABLE [dbo].[Spl_NCDBData]([Id] [int] IDENTITY (1, 1) NOT NULL,[cbustype] [varchar](200) NULL,[cRdCode] [varchar](200) NULL,[cInvCode] [varchar](200) NULL,[iQuantity] [decimal](18, 2) NULL,[publicsec1] [varchar](200) NULL,[publicsec2] [varchar](200) NULL,[publicsec3] [varchar](200) NULL,[publicsec4] [varchar](200) NULL,[CreateTime] [datetime] NOT NULL)";
					SqlHelper.ExecuteNonQuery(createSql);
					//StringBuilder strp = DataSetToArrayList.DataSetToArrayLists(GlDetails, "Spl_NCDBData");
					//SqlHelper.ExecuteNonQuery(strp.ToString());

					SqlBulkCopyHelper.ImportTempTableDataIndexForKM(GlDetails, "Spl_NCDBData");
					// msg = "采购入库NC数据插入成功";
				}
				else
				{
					string delstr = "delete from Spl_NCDBData where cbustype = '科目余额'";
					//delete from [Spl_NCU8Compare] where cbustype = '采购入库'
					SqlHelper.ExecuteNonQuerys(delstr);
					//StringBuilder strps = DataSetToArrayList.DataSetToArrayLists(GlDetails, "Spl_NCDBData");
					//SqlHelper.ExecuteNonQuery(strps.ToString());

					SqlBulkCopyHelper.ImportTempTableDataIndexForKM(GlDetails, "Spl_NCDBData");
					// msg = "采购入库NC数据更新成功";
				}
				//StringBuilder str = DataSetToArrayList.DataSetToArrayLists(PurBillVouch, "Spl_U8DBData");
				//SqlHelper.ExecuteNonQuery(str.ToString());

				//StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(PurBillVouchs, "Spl_NCDBData");
				//SqlHelper.ExecuteNonQuery(strs.ToString());
			}
			catch (Exception e)
			{

				result = "科目余额插入错误：" + e.Message;
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
				//sql = "select '转库入库' cbustype,A1.cInvCode cInvCode,isnull(SUM(A1.iTVQuantity),0) iQuantity,A.ciwhcode publicsec1,A2.cWhName publicsec2,GETDATE() as CreateTime from TransVouch A left join TransVouchs A1 on A.ID=A1.ID  left join Warehouse A2 on A2.cWhCode=A.ciwhcode WHERE Convert(nvarchar(7),A.dTVDate,121)='" + queryDate + "' group by A1.cInvCode,A.ciwhcode,A2.cWhName";
				sql = "select '调拨单' cbustype,A.cTVCode docNo,A1.cInvCode cInvCode,isnull(SUM(A1.iTVQuantity),0) iQuantity,A.cOWhCode publicsec1,A2.cWhName publicsec2,GETDATE() as CreateTime from TransVouch A left join TransVouchs A1 on A.ID=A1.ID  left join Warehouse A2 on A2.cWhCode=A.cOWhCode WHERE Convert(nvarchar(7),A.dTVDate,121)='" + queryDate + "'group by A1.cInvCode,A.cOWhCode,A2.cWhName,A.cTVCode";

				//获取转库入库数据
				DataSet TransVouch = SqlHelperForU8.ExecuteDataset(conneU8ctionString, CommandType.Text, sql);

				//获取转库入库数据
				sql = "select '调拨单' cbustype,A.vbillcode docNo,A2.code cInvCode,nvl(sum(A1.nassistnum),0) iQuantity,A5.code publicsec1,A5.name publicsec2,to_char(sysdate,'yyyy-mm-dd hh24:mi:ss')  as CreateTime FROM ic_whstrans_h A left join ic_whstrans_b A1 on A1.cspecialhid = A.cspecialhid and A1.dr != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc left join bd_stordoc A5 on A5.pk_stordoc = A.cwarehouseid where A.PK_ORG = '0001A110000000001V70' AND substr(A.dbilldate,0,7)= '" + queryDate + "' and A.fbillflag = 4 and substr(A2.code,0,4) != '0915' and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY') and A.cotherwhid  not in('1001A1100000000T5S5Z','1001A11000000003CYSY') GROUP BY A2.code,A5.code,A5.name,A.vbillcode";

				//获取转库入库数据
				DataSet TransVouchs = OracleHelper.ExecuteDataset(sql);



				//StringBuilder str = DataSetToArrayList.DataSetToArrayLists(TransVouch, "Spl_U8DBData");
				//SqlHelper.ExecuteNonQuery(str.ToString());

				SqlBulkCopyHelper.ImportTempTableDataIndexForCrk(TransVouch, "Spl_U8DBData");

				//StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(TransVouchs, "Spl_NCDBData");
				//SqlHelper.ExecuteNonQuery(strs.ToString());

				SqlBulkCopyHelper.ImportTempTableDataIndexForCrk(TransVouchs, "Spl_NCDBData");
			}
			catch (Exception e)
			{

				result = "调拨单插入错误：" + e.Message;
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
				sql = "select '其他出库单' cbustype,A.cCode docNo,A1.cInvCode cInvCode,SUM(A1.iQuantity) iQuantity,A.cWhCode publicsec1,A2.cWhName publicsec2,GETDATE() as CreateTime from RdRecord09 A left join rdrecords09 A1 on A.ID=A1.ID left join Warehouse A2 on A2.cWhCode=A.cWhCode WHERE Convert(nvarchar(7),A.dDate,121)='" + queryDate + "' and A.cRdCode NOT IN ('207','210') group by A1.cInvCode,A.cWhCode,A2.cWhName,A.cCode";

				//获取其他出库单数据
				DataSet Generalout = SqlHelperForU8.ExecuteDataset(conneU8ctionString, CommandType.Text, sql);

				//获取其他出库单数据
				sql = "select '其他出库单' cbustype,A.vbillcode docNo,A2.code cInvCode,nvl(sum(A1.nassistnum),0) iQuantity,A6.code publicsec1,A6.name publicsec2,to_char(sysdate,'yyyy-mm-dd hh24:mi:ss')  as CreateTime FROM ic_generalout_h A left join ic_generalout_b A1 on A1.cgeneralhid = A.cgeneralhid and A1.dr != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 on A3.pk_marbasclass = A2.pk_marbasclass left join bd_measdoc A4 on A4.pk_measdoc = A2.pk_measdoc left join bd_billtype A5 on A5.pk_billtypeid = A.ctrantypeid left join bd_stordoc A6 on A6.pk_stordoc = A.cwarehouseid where  (A.vtrantypecode not IN ('4I-02','4I-06')) AND substr(A.dbilldate,0,7)= '" + queryDate + "' and A2.CODE NOT LIKE '0915%' and A6.CODE NOT IN('PPG','STJGC') and A.PK_ORG='0001A110000000001V70' group by A2.code,A6.code,A6.name,A.vbillcode";

				//获取其他出库单数据
				DataSet GeneraloutLine = OracleHelper.ExecuteDataset(sql);

				//StringBuilder str = DataSetToArrayList.DataSetToArrayLists(Generalout, "Spl_U8DBData");
				//SqlHelper.ExecuteNonQuery(str.ToString());

				SqlBulkCopyHelper.ImportTempTableDataIndexForCrk(Generalout, "Spl_U8DBData");

				//StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(GeneraloutLine, "Spl_NCDBData");
				//SqlHelper.ExecuteNonQuery(strs.ToString());

				SqlBulkCopyHelper.ImportTempTableDataIndexForCrk(GeneraloutLine, "Spl_NCDBData");
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
				sql = "select '其他入库单' cbustype,A.cCode docNo,A1.cInvCode cInvCode,SUM(A1.iQuantity) iQuantity,A.cWhCode publicsec1,A2.cWhName publicsec2,GETDATE() as CreateTime from RdRecord08 A left join rdrecords08 A1 on A.ID=A1.ID left join Warehouse A2 on A2.cWhCode=A.cWhCode WHERE Convert(nvarchar(7),A.dDate,121)='" + queryDate + "' and A.cRdCode NOT IN ('107','105') group by A1.cInvCode,A.cWhCode,A2.cWhName,A.cCode";

				//获取其他出库单数据
				DataSet Generalout = SqlHelperForU8.ExecuteDataset(conneU8ctionString, CommandType.Text, sql);

				//获取其他出库单数据
				sql = "select '其他入库单' cbustype,A.vbillcode docNo,A2.code cInvCode,nvl(sum(A1.nassistnum),0) iQuantity,A6.code publicsec1,A6.name publicsec2,to_char(sysdate,'yyyy-mm-dd hh24:mi:ss')  as CreateTime FROM ic_generalin_h A left join ic_generalin_b A1 on A1.cgeneralhid = A.cgeneralhid and A1.dr != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 on A3.pk_marbasclass = A2.pk_marbasclass left join bd_measdoc A4 on A4.pk_measdoc = A2.pk_measdoc left join bd_billtype A5 on A5.pk_billtypeid = A.ctrantypeid left join bd_stordoc A6 on A6.pk_stordoc = A.cwarehouseid where  (A.vtrantypecode not IN ('4A-02','4A-06')) AND substr(A.dbilldate,0,7)= '" + queryDate + "' and A2.CODE NOT LIKE '0915%' and A6.CODE NOT IN('PPG','STJGC') and A.PK_ORG='0001A110000000001V70' group by A2.code,A6.code,A6.name,A.vbillcode";

				//获取其他出库单数据
				DataSet GeneraloutLine = OracleHelper.ExecuteDataset(sql);

				//StringBuilder str = DataSetToArrayList.DataSetToArrayLists(Generalout, "Spl_U8DBData");
				//SqlHelper.ExecuteNonQuery(str.ToString());

				SqlBulkCopyHelper.ImportTempTableDataIndexForCrk(Generalout, "Spl_U8DBData");

				//StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(GeneraloutLine, "Spl_NCDBData");
				//SqlHelper.ExecuteNonQuery(strs.ToString());

				SqlBulkCopyHelper.ImportTempTableDataIndexForCrk(GeneraloutLine, "Spl_NCDBData");
			}
			catch (Exception e)
			{

				result = "其他入库插入错误：" + e.Message;
			}
			return result;
		}


		/// <summary>
		/// 从u8获取产成品入库单单据
		/// 创建人：lvhe
		/// 创建时间：2021-01-23 21:59:56
		/// </summary>
		/// <returns></returns>
		private string GetFinprodInToSql()
		{
			string result = "";
			string sql = "";
			StringBuilder strbu = new StringBuilder();
			try
			{
				//获取其他出库单数据
				sql = "select '产成品入库' cbustype,A.cCode docNo,A1.cInvCode cInvCode,SUM(A1.iQuantity) iQuantity,A.cWhCode publicsec1,A2.cWhName publicsec2,GETDATE() as CreateTime from rdrecord10 A left join rdrecords10 A1 on A.ID=A1.ID left join Warehouse A2 on A2.cWhCode=A.cWhCode WHERE Convert(nvarchar(7),A.dDate,121)='" + queryDate + "' group by A1.cInvCode,A.cWhCode,A2.cWhName,A.cCode";

				//获取其他出库单数据
				DataSet FinprodIn = SqlHelperForU8.ExecuteDataset(conneU8ctionString, CommandType.Text, sql);

				//获取其他出库单数据
				sql = "SELECT '产成品入库' cbustype,A.vbillcode docNo,A2.code cinvcode,nvl(sum(A1.nassistnum),0) iQuantity,A5.code publicsec1,A5.name publicsec2,to_char(sysdate,'yyyy-mm-dd hh24:mi:ss')  as CreateTime FROM ic_finprodin_h A left join ic_finprodin_b A1 on A1.cgeneralhid = A.cgeneralhid and A1.dr != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc  left join bd_stordoc A5 on A5.pk_stordoc=A.cwarehouseid where A.PK_ORG = '0001A110000000001V70' AND substr(A.dbilldate,0,7)= '" + queryDate + "' and A.fbillflag = 3 and substr(A2.code,0,4) != '0915' and A.cwarehouseid not in('1001A1100000000T5S5Z','1001A11000000003CYSY') group by A2.code,A5.code,A5.name,A.vbillcode";

				//获取其他出库单数据
				DataSet FinprodIns = OracleHelper.ExecuteDataset(sql);

				//StringBuilder str = DataSetToArrayList.DataSetToArrayLists(FinprodIn, "Spl_U8DBData");
				//SqlHelper.ExecuteNonQuery(str.ToString());

				SqlBulkCopyHelper.ImportTempTableDataIndexForCrk(FinprodIn, "Spl_U8DBData");

				//StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(FinprodIns, "Spl_NCDBData");
				//SqlHelper.ExecuteNonQuery(strs.ToString());

				SqlBulkCopyHelper.ImportTempTableDataIndexForCrk(FinprodIns, "Spl_NCDBData");
			}
			catch (Exception e)
			{

				result = "产成品入库插入错误：" + e.Message;
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
				sql = "select '形态转换单' cbustype,A.cAVCode docNo,A1.cInvCode cInvCode,SUM(A1.iAVQuantity) iQuantity,A1.cWhCode publicsec1,A2.cWhName publicsec2,GETDATE() as CreateTime from AssemVouch A left join AssemVouchs A1 on A.ID=A1.ID left join Warehouse A2 on A2.cWhCode=A1.cWhCode WHERE Convert(nvarchar(7),A.dAVDate,121)='" + queryDate + "' group by A1.cInvCode,A1.cWhCode,A2.cWhName,A.cAVCode";

				//获取其他出库单数据
				DataSet Generalout = SqlHelperForU8.ExecuteDataset(conneU8ctionString, CommandType.Text, sql);

				//获取其他出库单数据
				sql = "select '形态转换单' cbustype,A.vbillcode docNo,A2.code cInvCode,nvl(sum(A1.nassistnum),0) iQuantity,A5.code publicsec1,A5.name publicsec2,to_char(sysdate,'yyyy-mm-dd hh24:mi:ss')  as CreateTime FROM ic_transform_h A left join ic_transform_b A1 on A1.cspecialhid = A.cspecialhid and A1.dr != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc left join bd_stordoc A5 on A5.pk_stordoc = A1.cbodywarehouseid where A.PK_ORG = '0001A110000000001V70' AND substr(A.dbilldate,0,7)= '" + queryDate + "' and substr(A2.code,0,4) != '0915' and A5.code not in('1001A1100000000T5S5Z','1001A11000000003CYSY') group by A2.code,A5.code,A5.name,A.vbillcode";

				//获取其他出库单数据
				DataSet GeneraloutLine = OracleHelper.ExecuteDataset(sql);

				//StringBuilder str = DataSetToArrayList.DataSetToArrayLists(Generalout, "Spl_U8DBData");
				//SqlHelper.ExecuteNonQuery(str.ToString());

				SqlBulkCopyHelper.ImportTempTableDataIndexForCrk(Generalout, "Spl_U8DBData");

				//StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(GeneraloutLine, "Spl_NCDBData");
				//SqlHelper.ExecuteNonQuery(strs.ToString());

				SqlBulkCopyHelper.ImportTempTableDataIndexForCrk(GeneraloutLine, "Spl_NCDBData");
			}
			catch (Exception e)
			{

				result = "形态转换单插入错误：" + e.Message;
			}
			return result;
		}


		/// <summary>
		/// 从U8获取材料出库单数据
		/// 创建人：lvhe
		/// 创建时间：2021-01-26 23:42:19
		/// </summary>
		/// <returns></returns>
		[WebMethod]
		private string GetMaterialToSql()
		{
			string result = "";
			string sql = "";
			StringBuilder strbu = new StringBuilder();
			try
			{
				//获取材料出库单数据
				sql = "select '材料出库' cbustype,A.cCode docNo,A1.cInvCode cInvCode,SUM(A1.iQuantity) iQuantity,A.cWhCode publicsec1,A2.cWhName publicsec2,GETDATE() as CreateTime from RdRecord11 A left join RdRecords11 A1 on A.ID = A1.ID left join Warehouse A2 on A2.cWhCode = A.cWhCode WHERE Convert(nvarchar(7), A.dDate, 121) = '" + queryDate + "' group by A1.cInvCode,A.cWhCode,A2.cWhName,A.cCode";

				//获取材料出库单数据
				DataSet Material = SqlHelperForU8.ExecuteDataset(conneU8ctionString, CommandType.Text, sql);

				//获取材料出库单数据
				sql = "select '材料出库' cbustype,A.vbillcode docNo,A2.code cInvCode,nvl(sum(A1.nassistnum),0) iQuantity,A5.code publicsec1,A5.name publicsec2,to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') as CreateTime FROM ic_material_h A left join ic_material_b A1 on A1.cgeneralhid = A.cgeneralhid and A1.dr != 1 left join bd_material A2 on A1.cmaterialvid = A2.pk_material left join bd_marbasclass A3 ON A2.PK_MARBASCLASS = A3.PK_MARBASCLASS left join bd_measdoc A4 on A2.PK_MEASDOC = A4.pk_measdoc left join bd_stordoc A5 on A5.pk_stordoc = A1.cbodywarehouseid where A.PK_ORG = '0001A110000000001V70' AND substr(A.dbilldate,0,7)= '" + queryDate + "' and substr(A2.code,0,4) != '0915' and A5.code not in('1001A1100000000T5S5Z', '1001A11000000003CYSY') group by A2.code,A5.code,A5.name,A.vbillcode";

				//获取材料出库单数据
				DataSet Materials = OracleHelper.ExecuteDataset(sql);

				//StringBuilder str = DataSetToArrayList.DataSetToArrayLists(Material, "Spl_U8DBData");
				//SqlHelper.ExecuteNonQuery(str.ToString());

				SqlBulkCopyHelper.ImportTempTableDataIndexForCrk(Material, "Spl_U8DBData");

				//StringBuilder strs = DataSetToArrayList.DataSetToArrayLists(Materials, "Spl_NCDBData");
				//SqlHelper.ExecuteNonQuery(strs.ToString());

				SqlBulkCopyHelper.ImportTempTableDataIndexForCrk(Materials, "Spl_NCDBData");
			}
			catch (Exception e)
			{

				result = "材料出库单插入错误：" + e.Message;
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
			string sql = "";
			string sql2 = "";
			try
			{
				string delstrApps = "delete from  Spl_NCU8Compare where cbustype!='采购发票' and cbustype!='销售发票'";
				SqlHelperForApps.ExecuteNonQuerys(delstrApps);
				sql = " select isnull(A.cbustype,A1.cbustype) cbustype,isnull(A.cinvcode,A1.cinvcode) cInvCode,isnull(A.iquantity,0) iQuantity,isnull(A1.iquantity,0) nciQuantity,isnull(A.publicsec1,A1.publicsec1) publicsec1,isnull(A.publicsec2,A1.publicsec2) publicsec2,isnull(A.publicsec3,A1.publicsec3) publicsec3,isnull(A.publicsec4,A1.publicsec4) publicsec4,isnull(A.docno,0) publicsec8,isnull(A1.docno,0) publicsec9,GETDATE() as CreateTime from [Spl_U8DBData] A left join [Spl_ncDBData] A1 on A.cbustype=A1.cbustype and A.cinvcode = A1.cinvcode and  A.publicsec1=A1.publicsec1 and A.docNo=A1.docno where A.cbustype!='采购发票' and A1.cbustype!='采购发票' and A.cbustype!='销售发票' and A1.cbustype!='销售发票' ";
				//采购入库
				sql += " union all ";
				sql += " select cbustype,cInvCode,iQuantity,0 nciQuantity,publicsec1,publicsec2,publicsec3,publicsec4,docNo publicsec8,'' publicsec9,GETDATE() as CreateTime from [Spl_U8DBData] where docNo not in(select docNo from [Spl_NCDBData] where cbustype = '采购入库') and cbustype = '采购入库' ";
				sql += " union all ";
				sql += " select cbustype,cInvCode,0 iQuantity,iQuantity nciQuantity,publicsec1,publicsec2,publicsec3,publicsec4,'' publicsec8,docNo publicsec9,GETDATE() as CreateTime from [Spl_NCDBData] where docNo not in(select docNo from [Spl_U8DBData] where cbustype = '采购入库') and cbustype = '采购入库'  ";
				//销售出库
				sql += " union all ";
				sql += " select cbustype,cInvCode,iQuantity,0 nciQuantity,publicsec1,publicsec2,publicsec3,publicsec4,docNo publicsec8,'' publicsec9,GETDATE() as CreateTime from [Spl_U8DBData] where docNo not in(select docNo from [Spl_NCDBData] where cbustype = '销售出库') and cbustype = '销售出库' ";
				sql += " union all ";
				sql += " select cbustype,cInvCode,0 iQuantity,iQuantity nciQuantity,publicsec1,publicsec2,publicsec3,publicsec4,'' publicsec8,docNo publicsec9,GETDATE() as CreateTime from [Spl_NCDBData] where docNo not in(select docNo from [Spl_U8DBData] where cbustype = '销售出库') and cbustype = '销售出库'  ";
				//转库入库
				sql += " union all ";
				sql += " select cbustype,cInvCode,iQuantity,0 nciQuantity,publicsec1,publicsec2,publicsec3,publicsec4,docNo publicsec8,'' publicsec9,GETDATE() as CreateTime from [Spl_U8DBData] where docNo not in(select docNo from [Spl_NCDBData] where cbustype = '调拨单') and cbustype = '调拨单' ";
				sql += " union all ";
				sql += " select cbustype,cInvCode,0 iQuantity,iQuantity nciQuantity,publicsec1,publicsec2,publicsec3,publicsec4,'' publicsec8,docNo publicsec9,GETDATE() as CreateTime from [Spl_NCDBData] where docNo not in(select docNo from [Spl_U8DBData] where cbustype = '调拨单') and cbustype = '调拨单'  ";
				//其他出库单
				sql += " union all ";
				sql += " select cbustype,cInvCode,iQuantity,0 nciQuantity,publicsec1,publicsec2,publicsec3,publicsec4,docNo publicsec8,'' publicsec9,GETDATE() as CreateTime from [Spl_U8DBData] where docNo not in(select docNo from [Spl_NCDBData] where cbustype = '其他出库单') and cbustype = '其他出库单' ";
				sql += " union all ";
				sql += " select cbustype,cInvCode,0 iQuantity,iQuantity nciQuantity,publicsec1,publicsec2,publicsec3,publicsec4,'' publicsec8,docNo publicsec9,GETDATE() as CreateTime from [Spl_NCDBData] where docNo not in(select docNo from [Spl_U8DBData] where cbustype = '其他出库单') and cbustype = '其他出库单'  ";
				//其他入库单
				sql += " union all ";
				sql += " select cbustype,cInvCode,iQuantity,0 nciQuantity,publicsec1,publicsec2,publicsec3,publicsec4,docNo publicsec8,'' publicsec9,GETDATE() as CreateTime from [Spl_U8DBData] where docNo not in(select docNo from [Spl_NCDBData] where cbustype = '其他入库单') and cbustype = '其他入库单' ";
				sql += " union all ";
				sql += " select cbustype,cInvCode,0 iQuantity,iQuantity nciQuantity,publicsec1,publicsec2,publicsec3,publicsec4,'' publicsec8,docNo publicsec9,GETDATE() as CreateTime from [Spl_NCDBData] where docNo not in(select docNo from [Spl_U8DBData] where cbustype = '其他入库单') and cbustype = '其他入库单'  ";
				//形态转换单
				sql += " union all ";
				sql += " select cbustype,cInvCode,iQuantity,0 nciQuantity,publicsec1,publicsec2,publicsec3,publicsec4,docNo publicsec8,'' publicsec9,GETDATE() as CreateTime from [Spl_U8DBData] where docNo not in(select docNo from [Spl_NCDBData] where cbustype = '形态转换单') and cbustype = '形态转换单' ";
				sql += " union all ";
				sql += " select cbustype,cInvCode,0 iQuantity,iQuantity nciQuantity,publicsec1,publicsec2,publicsec3,publicsec4,'' publicsec8,docNo publicsec9,GETDATE() as CreateTime from [Spl_NCDBData] where docNo not in(select docNo from [Spl_U8DBData] where cbustype = '形态转换单') and cbustype = '形态转换单'  ";

				//材料出库
				sql += " union all ";
				sql += " select cbustype,cInvCode,iQuantity,0 nciQuantity,publicsec1,publicsec2,publicsec3,publicsec4,docNo publicsec8,'' publicsec9,GETDATE() as CreateTime from [Spl_U8DBData] where docNo not in(select docNo from [Spl_NCDBData] where cbustype = '材料出库') and cbustype = '材料出库' ";
				sql += " union all ";
				sql += " select cbustype,cInvCode,0 iQuantity,iQuantity nciQuantity,publicsec1,publicsec2,publicsec3,publicsec4,'' publicsec8,docNo publicsec9,GETDATE() as CreateTime from [Spl_NCDBData] where docNo not in(select docNo from [Spl_U8DBData] where cbustype = '材料出库') and cbustype = '材料出库'  ";
				//产成品入库
				sql += " union all ";
				sql += " select cbustype,cInvCode,iQuantity,0 nciQuantity,publicsec1,publicsec2,publicsec3,publicsec4,docNo publicsec8,'' publicsec9,GETDATE() as CreateTime from [Spl_U8DBData] where docNo not in(select docNo from [Spl_NCDBData] where cbustype = '产成品入库') and cbustype = '产成品入库' ";
				sql += " union all ";
				sql += " select cbustype,cInvCode,0 iQuantity,iQuantity nciQuantity,publicsec1,publicsec2,publicsec3,publicsec4,'' publicsec8,docNo publicsec9,GETDATE() as CreateTime from [Spl_NCDBData] where docNo not in(select docNo from [Spl_U8DBData] where cbustype = '产成品入库') and cbustype = '产成品入库'  ";

				DataSet ncu8compareDs = SqlHelper.ExecuteDataset(connectionString, CommandType.Text, sql);
				//StringBuilder strt = DataSetToArrayList.DataSetToArrayLists(ncu8compareDs, "Spl_NCU8Compare");
				//SqlHelperForApps.ExecuteNonQuery(strt.ToString());

				SqlBulkCopyHelperForApps.ImportTempTableDataIndex(ncu8compareDs, "Spl_NCU8Compare");
			}
			catch (Exception e)
			{
				msg = "数据对比失败：" + e.Message;
			}
			return msg;
		}


		/// <summary>
		/// 采购发票对比
		/// 创建人：lvhe
		/// 创建时间：2020-4-12 22:19:10
		/// </summary>
		/// <returns></returns>
		private string Ncu8CompareForCGFP()
		{
			string msg = "";
			//string sql = "";
			try
			{
				string delstrApps = "delete from  Spl_NCU8Compare where cbustype='采购发票'";
				SqlHelperForApps.ExecuteNonQuerys(delstrApps);
				//string sql2 = " select isnull(A.cbustype,A1.cbustype) cbustype,isnull(A.cinvcode,A1.cinvcode) cInvCode,isnull(A.iquantity,0) iQuantity,isnull(A1.iquantity,0) nciQuantity,isnull(A.publicsec1,A1.publicsec1) publicsec1,A.publicsec2 publicsec2,A1.publicsec2 publicsec5,isnull(A.publicsec3,A1.publicsec3) publicsec3,isnull(A.publicsec4,A1.publicsec4) publicsec4,isnull(A.docno,0) publicsec8,isnull(A1.docno,0) publicsec9,GETDATE() as CreateTime from [Spl_U8DBData] A left join [Spl_ncDBData] A1 on A.cbustype=A1.cbustype and A.cinvcode = A1.cinvcode and  A.publicsec1=A1.publicsec1 where (A.cbustype='采购发票' or A1.cbustype='采购发票') and A.docNo = A1.docNo ";

				//sql2 += " union all ";
				//sql2 += " select cbustype,cInvCode,iQuantity,0 nciQuantity,publicsec1,publicsec2,'0' publicsec5,publicsec3,publicsec4,docNo publicsec8,'' publicsec9,GETDATE() as CreateTime from [Spl_U8DBData] where docNo not in(select docNo from [Spl_NCDBData] where cbustype = '采购发票') and cbustype = '采购发票' ";
				//sql2 += " union all ";
				//sql2 += " select cbustype,cInvCode,0 iQuantity,iQuantity nciQuantity,publicsec1,'0' publicsec2,publicsec2 publicsec5,publicsec3,publicsec4,'' publicsec8,docNo publicsec9,GETDATE() as CreateTime from[Spl_NCDBData] where docNo not in(select docNo from [Spl_U8DBData] where cbustype = '采购发票') and cbustype = '采购发票'  ";


				string sql2 = "select isnull(A.cbustype,A1.cbustype) cbustype,isnull(A.cinvcode,A1.cinvcode) cInvCode,isnull(A.iquantity,0) iQuantity,isnull(A1.iquantity,0) nciQuantity,isnull(A.publicsec1,A1.publicsec1) publicsec1,isnull(A.publicsec2,'0.00') publicsec2,isnull(A1.publicsec2,'0.00') publicsec5,isnull(A.publicsec3,A1.publicsec3) publicsec3,isnull(A.publicsec4,A1.publicsec4) publicsec4,isnull(A.docno,'') publicsec8,isnull(A1.docno,'') publicsec9,GETDATE() as CreateTime from [Spl_U8DBData] A full join [Spl_ncDBData] A1 on A.cbustype=A1.cbustype and A.cinvcode = A1.cinvcode and  A.publicsec1=A1.publicsec1 and A.docNo = A1.docNo where(A.cbustype = '采购发票' or A1.cbustype = '采购发票') ";
				//获取采购入库汇总数据
				DataSet ncu8compareDs = SqlHelper.ExecuteDataset(connectionString, CommandType.Text, sql2);
				//StringBuilder strt = DataSetToArrayList.DataSetToArrayLists(ncu8compareDs, "Spl_NCU8Compare");
				//SqlHelperForApps.ExecuteNonQuery(strt.ToString());

				SqlBulkCopyHelperForApps.ImportTempTableDataIndexForCGFP(ncu8compareDs, "Spl_NCU8Compare");
			}
			catch (Exception e)
			{
				msg = "采购发票数据对比失败：" + e.Message;
			}
			return msg;
		}

		/// <summary>
		/// 销售发票对比
		/// 创建人：lvhe
		/// 创建时间：2020-4-12 22:18:48
		/// </summary>
		/// <returns></returns>
		private string Ncu8CompareForXSFP()
		{
			string msg = "";
			//string sql = "";
			try
			{
				string delstrApps = "delete from  Spl_NCU8Compare where cbustype='销售发票'";
				SqlHelperForApps.ExecuteNonQuerys(delstrApps);
				//string sql2 = " select isnull(A.cbustype,A1.cbustype) cbustype,isnull(A.cinvcode,A1.cinvcode) cInvCode,isnull(A.iquantity,0) iQuantity,isnull(A1.iquantity,0) nciQuantity,isnull(A.publicsec1,A1.publicsec1) publicsec1,A.publicsec2 publicsec2,A1.publicsec2 publicsec5,isnull(A.publicsec3,A1.publicsec3) publicsec3,isnull(A.publicsec4,A1.publicsec4) publicsec4,isnull(A.docno,0) publicsec8,isnull(A1.docno,0) publicsec9,GETDATE() as CreateTime from [Spl_U8DBData] A left join [Spl_ncDBData] A1 on A.cbustype = A1.cbustype and A.cinvcode = A1.cinvcode  and A.publicsec1 = A1.publicsec1 where (A.cbustype = '销售发票' or A1.cbustype = '销售发票') and A.docNo = A1.docNo ";
				////销售发票
				//sql2 += " union all ";
				//sql2 += " select cbustype,cInvCode,iQuantity,0 nciQuantity,publicsec1,publicsec2,'0.00' publicsec5,publicsec3,publicsec4,docNo publicsec8,'' publicsec9,GETDATE() as CreateTime from [Spl_U8DBData] where docNo not in(select docNo from [Spl_NCDBData] where cbustype = '销售发票') and cbustype = '销售发票' ";
				//sql2 += " union all ";
				//sql2 += " select cbustype,cInvCode,0 iQuantity,iQuantity nciQuantity,publicsec1,'0.00' publicsec2,publicsec2 publicsec5,publicsec3,publicsec4,'' publicsec8,docNo publicsec9,GETDATE() as CreateTime from[Spl_NCDBData] where docNo not in(select docNo from [Spl_U8DBData] where cbustype = '销售发票') and cbustype = '销售发票'  ";


				string sql2 = "select isnull(A.cbustype,A1.cbustype) cbustype,isnull(A.cinvcode,A1.cinvcode) cInvCode,isnull(A.iquantity,0) iQuantity,isnull(A1.iquantity,0) nciQuantity,isnull(A.publicsec1,A1.publicsec1) publicsec1,isnull(A.publicsec2,'0.00') publicsec2,isnull(A1.publicsec2,'0.00') publicsec5,isnull(A.publicsec3,A1.publicsec3) publicsec3,isnull(A.publicsec4,A1.publicsec4) publicsec4,isnull(A.docno,'') publicsec8,isnull(A1.docno,'') publicsec9,GETDATE() as CreateTime from[Spl_U8DBData] A full join[Spl_ncDBData] A1 on A.cbustype = A1.cbustype and A.cinvcode = A1.cinvcode  and A.publicsec1 = A1.publicsec1 and A.docNo = A1.docNo where(A.cbustype = '销售发票' or A1.cbustype = '销售发票') ";

				//获取采购入库汇总数据
				DataSet ncu8compareDs = SqlHelper.ExecuteDataset(connectionString, CommandType.Text, sql2);
				//StringBuilder strt = DataSetToArrayList.DataSetToArrayLists(ncu8compareDs, "Spl_NCU8Compare");
				//SqlHelperForApps.ExecuteNonQuery(strt.ToString());


				SqlBulkCopyHelperForApps.ImportTempTableDataIndexForXSFP(ncu8compareDs, "Spl_NCU8Compare");
			}
			catch (Exception e)
			{
				msg = "销售发票数据对比失败：" + e.Message;
			}
			return msg;
		}


		/// <summary>
		/// 凭证对比
		/// 创建人：lvhe
		/// 创建时间：2020-4-12 22:18:29
		/// </summary>
		/// <returns></returns>
		private string Ncu8CompareForPz()
		{
			string msg = "";
			//string sql = "";
			try
			{
				string delstrApps = "delete from  Spl_NCU8Compare where cbustype='科目余额'";
				SqlHelperForApps.ExecuteNonQuerys(delstrApps);
				string sql2 = " select isnull(A.cbustype,A1.cbustype) cbustype,isnull(A.cInvCode,A1.cInvCode) cInvCode,isnull(A.publicsec3,A1.publicsec3) publicsec3,isnull(A.iquantity,0) iQuantity,isnull(A1.iquantity,0) nciQuantity,isnull(A.publicsec1,0) publicsec1,isnull(A1.publicsec1,0) publicsec2,GETDATE() as CreateTime from [Spl_U8DBData] A full join [Spl_ncDBData] A1 on A.cbustype=A1.cbustype and A.cinvcode = A1.cinvcode  where  A.cbustype='科目余额' or A1.cbustype='科目余额' order by A.cInvCode desc";
				//获取采购入库汇总数据
				DataSet ncu8compareDs = SqlHelper.ExecuteDataset(connectionString, CommandType.Text, sql2);
				//StringBuilder strt = DataSetToArrayList.DataSetToArrayLists(ncu8compareDs, "Spl_NCU8Compare");
				//SqlHelperForApps.ExecuteNonQuery(strt.ToString());

				SqlBulkCopyHelperForApps.ImportTempTableDataIndexForKM(ncu8compareDs, "Spl_NCU8Compare");
			}
			catch (Exception e)
			{
				msg = "科目余额数据对比失败：" + e.Message;
			}
			return msg;
		}


		/// <summary>
		/// 比对同一月  生产入库所耗用的材料与实际材料出库差异
		/// 创建人：lvhe
		/// 创建时间：2020-4-16 23:43:10
		/// </summary>
		/// <returns></returns>
		private string WarehousingCompareMaterialOut()
		{
			string msg = "";
			try
			{
				string delstrApps = "delete from  Spl_NCU8Compare where cbustype='生产领料'";
				SqlHelperForApps.ExecuteNonQuerys(delstrApps);
				ParameterKeyValuesEntity pk = new ParameterKeyValuesEntity();
				pk.Key = "iperiod";
				pk.Value = queryDate;
				DataSet ds = SqlHelperForU8.Sql_GetStoredProcedureFunction(conneU8ctionString, "EFCust_SP_GetIssueQty", 10, out ResponseBool, out ResponseMsg, pk);
				//StringBuilder strt = DataSetToArrayList.DataSetToArrayLists(ds, "Spl_NCU8Compare");
				//SqlHelperForApps.ExecuteNonQuery(strt.ToString());

				SqlBulkCopyHelperForApps.ImportTempTableDataIndexForSCLL(ds, "Spl_NCU8Compare");
			}
			catch (Exception e)
			{
				msg = "生产领料：" + e.Message;
			}
			return msg;
		}
	}
}
