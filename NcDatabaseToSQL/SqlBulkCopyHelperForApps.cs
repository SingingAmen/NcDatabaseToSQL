using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Web;


namespace NcDatabaseToSQL
{

	public static class SqlBulkCopyHelperForApps
	{
		/// <summary>
		/// 数据库连接字符串
		/// </summary>
		private static string connectionString = ConfigurationManager.ConnectionStrings["AppsDBCoon"].ToString();

		/// <summary>
		/// 插入方法
		/// </summary>
		/// <param name="ds">插入数据集合</param>
		/// <param name="TempTableName">要插入的表</param>
		public static void ImportTempTableDataIndex(DataSet ds, string TempTableName)
		{

			//获取写入连接
			//string str = strSqlConnection;
			//SqlConnectionStringBuilder sb = new SqlConnectionStringBuilder(connectionString);
			//string DataSource = sb.DataSource;
			//string PersistSecurityInfo = sb.PersistSecurityInfo.ToString();
			//string Pwd = sb.Password;
			//string UserID = sb.UserID;
			//string basestr = sb.InitialCatalog;
			//string InitialCatalog = sb.InitialCatalog;

			//开始写入数据
			//str = $"Data Source={DataSource};database={basestr};user={UserID};password={Pwd}";
			using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(connectionString, SqlBulkCopyOptions.UseInternalTransaction))
			{
				//DataSet与数据库表一一对应时不需要写对应关系
				sqlBulkCopy.ColumnMappings.Add("cbustype", "cbustype");
				sqlBulkCopy.ColumnMappings.Add("cInvCode", "cInvCode");
				sqlBulkCopy.ColumnMappings.Add("iQuantity", "iQuantity");
				sqlBulkCopy.ColumnMappings.Add("nciQuantity", "nciQuantity");
				sqlBulkCopy.ColumnMappings.Add("publicsec1", "publicsec1");
				sqlBulkCopy.ColumnMappings.Add("publicsec2", "publicsec2");
				sqlBulkCopy.ColumnMappings.Add("publicsec3", "publicsec3");
				sqlBulkCopy.ColumnMappings.Add("publicsec4", "publicsec4");
				//sqlBulkCopy.ColumnMappings.Add("publicsec5", "publicsec5");
				//sqlBulkCopy.ColumnMappings.Add("publicsec6", "publicsec6");
				//sqlBulkCopy.ColumnMappings.Add("publicsec7", "publicsec7");
				//sqlBulkCopy.ColumnMappings.Add("publicsec7", "publicsec7");
				sqlBulkCopy.ColumnMappings.Add("publicsec8", "publicsec8");
				sqlBulkCopy.ColumnMappings.Add("publicsec9", "publicsec9");
				sqlBulkCopy.ColumnMappings.Add("CreateTime", "CreateTime");
				//sqlBulkCopy.EnableStreaming = true;
				sqlBulkCopy.DestinationTableName = $"{TempTableName}";
				sqlBulkCopy.WriteToServer(ds.Tables[0]);
				sqlBulkCopy.Close();
			}
			ds.Dispose();
		}



		public static void ImportTempTableDataIndexForCGFP(DataSet ds, string TempTableName)
		{

			//获取写入连接
			//string str = strSqlConnection;
			//SqlConnectionStringBuilder sb = new SqlConnectionStringBuilder(connectionString);
			//string DataSource = sb.DataSource;
			//string PersistSecurityInfo = sb.PersistSecurityInfo.ToString();
			//string Pwd = sb.Password;
			//string UserID = sb.UserID;
			//string basestr = sb.InitialCatalog;
			//string InitialCatalog = sb.InitialCatalog;

			//开始写入数据
			//str = $"Data Source={DataSource};database={basestr};user={UserID};password={Pwd}";
			using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(connectionString, SqlBulkCopyOptions.UseInternalTransaction))
			{
				//DataSet与数据库表一一对应时不需要写对应关系
				sqlBulkCopy.ColumnMappings.Add("cbustype", "cbustype");
				sqlBulkCopy.ColumnMappings.Add("cInvCode", "cInvCode");
				sqlBulkCopy.ColumnMappings.Add("iQuantity", "iQuantity");
				sqlBulkCopy.ColumnMappings.Add("nciQuantity", "nciQuantity");
				sqlBulkCopy.ColumnMappings.Add("publicsec1", "publicsec1");
				sqlBulkCopy.ColumnMappings.Add("publicsec2", "publicsec2");
				sqlBulkCopy.ColumnMappings.Add("publicsec3", "publicsec3");
				sqlBulkCopy.ColumnMappings.Add("publicsec4", "publicsec4");
				sqlBulkCopy.ColumnMappings.Add("publicsec5", "publicsec5");
				//sqlBulkCopy.ColumnMappings.Add("publicsec6", "publicsec6");
				//sqlBulkCopy.ColumnMappings.Add("publicsec7", "publicsec7");
				//sqlBulkCopy.ColumnMappings.Add("publicsec7", "publicsec7");
				sqlBulkCopy.ColumnMappings.Add("publicsec8", "publicsec8");
				sqlBulkCopy.ColumnMappings.Add("publicsec9", "publicsec9");
				sqlBulkCopy.ColumnMappings.Add("CreateTime", "CreateTime");
				//sqlBulkCopy.EnableStreaming = true;
				sqlBulkCopy.DestinationTableName = $"{TempTableName}";
				sqlBulkCopy.WriteToServer(ds.Tables[0]);
				sqlBulkCopy.Close();
			}
			ds.Dispose();
		}


		public static void ImportTempTableDataIndexForXSFP(DataSet ds, string TempTableName)
		{

			//获取写入连接
			//string str = strSqlConnection;
			//SqlConnectionStringBuilder sb = new SqlConnectionStringBuilder(connectionString);
			//string DataSource = sb.DataSource;
			//string PersistSecurityInfo = sb.PersistSecurityInfo.ToString();
			//string Pwd = sb.Password;
			//string UserID = sb.UserID;
			//string basestr = sb.InitialCatalog;
			//string InitialCatalog = sb.InitialCatalog;

			//开始写入数据
			//str = $"Data Source={DataSource};database={basestr};user={UserID};password={Pwd}";
			using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(connectionString, SqlBulkCopyOptions.UseInternalTransaction))
			{
				//DataSet与数据库表一一对应时不需要写对应关系
				sqlBulkCopy.ColumnMappings.Add("cbustype", "cbustype");
				sqlBulkCopy.ColumnMappings.Add("cInvCode", "cInvCode");
				sqlBulkCopy.ColumnMappings.Add("iQuantity", "iQuantity");
				sqlBulkCopy.ColumnMappings.Add("nciQuantity", "nciQuantity");
				sqlBulkCopy.ColumnMappings.Add("publicsec1", "publicsec1");
				sqlBulkCopy.ColumnMappings.Add("publicsec2", "publicsec2");
				sqlBulkCopy.ColumnMappings.Add("publicsec3", "publicsec3");
				sqlBulkCopy.ColumnMappings.Add("publicsec4", "publicsec4");
				sqlBulkCopy.ColumnMappings.Add("publicsec5", "publicsec5");
				//sqlBulkCopy.ColumnMappings.Add("publicsec6", "publicsec6");
				//sqlBulkCopy.ColumnMappings.Add("publicsec7", "publicsec7");
				//sqlBulkCopy.ColumnMappings.Add("publicsec7", "publicsec7");
				sqlBulkCopy.ColumnMappings.Add("publicsec8", "publicsec8");
				sqlBulkCopy.ColumnMappings.Add("publicsec9", "publicsec9");
				sqlBulkCopy.ColumnMappings.Add("CreateTime", "CreateTime");
				//sqlBulkCopy.EnableStreaming = true;
				sqlBulkCopy.DestinationTableName = $"{TempTableName}";
				sqlBulkCopy.WriteToServer(ds.Tables[0]);
				sqlBulkCopy.Close();
			}
			ds.Dispose();
		}



		public static void ImportTempTableDataIndexForKM(DataSet ds, string TempTableName)
		{

			//获取写入连接
			//string str = strSqlConnection;
			//SqlConnectionStringBuilder sb = new SqlConnectionStringBuilder(connectionString);
			//string DataSource = sb.DataSource;
			//string PersistSecurityInfo = sb.PersistSecurityInfo.ToString();
			//string Pwd = sb.Password;
			//string UserID = sb.UserID;
			//string basestr = sb.InitialCatalog;
			//string InitialCatalog = sb.InitialCatalog;

			//开始写入数据
			//str = $"Data Source={DataSource};database={basestr};user={UserID};password={Pwd}";
			using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(connectionString, SqlBulkCopyOptions.UseInternalTransaction))
			{
				//DataSet与数据库表一一对应时不需要写对应关系
				sqlBulkCopy.ColumnMappings.Add("cbustype", "cbustype");
				sqlBulkCopy.ColumnMappings.Add("cInvCode", "cInvCode");
				sqlBulkCopy.ColumnMappings.Add("iQuantity", "iQuantity");
				sqlBulkCopy.ColumnMappings.Add("nciQuantity", "nciQuantity");
				sqlBulkCopy.ColumnMappings.Add("publicsec1", "publicsec1");
				sqlBulkCopy.ColumnMappings.Add("publicsec2", "publicsec2");
				sqlBulkCopy.ColumnMappings.Add("publicsec3", "publicsec3");
				//sqlBulkCopy.ColumnMappings.Add("publicsec4", "publicsec4");
				//sqlBulkCopy.ColumnMappings.Add("publicsec5", "publicsec5");
				//sqlBulkCopy.ColumnMappings.Add("publicsec6", "publicsec6");
				//sqlBulkCopy.ColumnMappings.Add("publicsec7", "publicsec7");
				//sqlBulkCopy.ColumnMappings.Add("publicsec7", "publicsec7");
				//sqlBulkCopy.ColumnMappings.Add("publicsec8", "publicsec8");
				//sqlBulkCopy.ColumnMappings.Add("publicsec9", "publicsec9");
				sqlBulkCopy.ColumnMappings.Add("CreateTime", "CreateTime");
				//sqlBulkCopy.EnableStreaming = true;
				sqlBulkCopy.DestinationTableName = $"{TempTableName}";
				sqlBulkCopy.WriteToServer(ds.Tables[0]);
				sqlBulkCopy.Close();
			}
			ds.Dispose();
		}


		public static void ImportTempTableDataIndexForSCLL(DataSet ds, string TempTableName)
		{

			//获取写入连接
			//string str = strSqlConnection;
			//SqlConnectionStringBuilder sb = new SqlConnectionStringBuilder(connectionString);
			//string DataSource = sb.DataSource;
			//string PersistSecurityInfo = sb.PersistSecurityInfo.ToString();
			//string Pwd = sb.Password;
			//string UserID = sb.UserID;
			//string basestr = sb.InitialCatalog;
			//string InitialCatalog = sb.InitialCatalog;

			//开始写入数据
			//str = $"Data Source={DataSource};database={basestr};user={UserID};password={Pwd}";
			using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(connectionString, SqlBulkCopyOptions.UseInternalTransaction))
			{
				//DataSet与数据库表一一对应时不需要写对应关系
				sqlBulkCopy.ColumnMappings.Add("cbustype", "cbustype");
				sqlBulkCopy.ColumnMappings.Add("cRdCode", "cRdCode");
				sqlBulkCopy.ColumnMappings.Add("cInvCode", "cInvCode");
				sqlBulkCopy.ColumnMappings.Add("iQuantity", "iQuantity");
				sqlBulkCopy.ColumnMappings.Add("nciQuantity", "nciQuantity");
				sqlBulkCopy.ColumnMappings.Add("publicsec1", "publicsec1");
				//sqlBulkCopy.ColumnMappings.Add("publicsec2", "publicsec2");
				//sqlBulkCopy.ColumnMappings.Add("publicsec3", "publicsec3");
				//sqlBulkCopy.ColumnMappings.Add("publicsec4", "publicsec4");
				//sqlBulkCopy.ColumnMappings.Add("publicsec5", "publicsec5");
				//sqlBulkCopy.ColumnMappings.Add("publicsec6", "publicsec6");
				//sqlBulkCopy.ColumnMappings.Add("publicsec7", "publicsec7");
				//sqlBulkCopy.ColumnMappings.Add("publicsec7", "publicsec7");
				//sqlBulkCopy.ColumnMappings.Add("publicsec8", "publicsec8");
				//sqlBulkCopy.ColumnMappings.Add("publicsec9", "publicsec9");
				sqlBulkCopy.ColumnMappings.Add("CreateTime", "CreateTime");
				//sqlBulkCopy.EnableStreaming = true;
				sqlBulkCopy.DestinationTableName = $"{TempTableName}";
				sqlBulkCopy.WriteToServer(ds.Tables[0]);
				sqlBulkCopy.Close();
			}
			ds.Dispose();
		}
	}
}