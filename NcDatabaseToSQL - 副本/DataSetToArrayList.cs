using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Web;

namespace NcDatabaseToSQL
{
    public static class DataSetToArrayList
    {
        /// <summary>
        /// 由数据集DataSet自动生成Insert的SQL语句集合
        /// </summary>
        /// <param name="ds">数据集</param>
        /// <param name="TableName">表名</param>
        /// <returns></returns>
        public static StringBuilder DataSetToArrayLists(DataSet ds, String TableName)
        {
            //ArrayList allSql = new ArrayList();
            StringBuilder allSql = new StringBuilder();
            //
            string FieldAll = "";
            //获取列名集合
            for (int i = 0; i < ds.Tables[0].Columns.Count; i++)
            {
                FieldAll = FieldAll + ds.Tables[0].Columns[i].ColumnName.ToString() + ",";
            }
            FieldAll = FieldAll.Substring(0, FieldAll.Length - 1);//去掉最后一个“，”

            DataView dv = ds.Tables[0].DefaultView;
            string ValueAll = "";
            //判断字段类型，其实也可以全部用单引号引起来，只是数据库处理时，内部需要转化类型
            for (int n = 0; n < dv.Count; n++)//  用于循环写insert
            {
                for (int m = 0; m < ds.Tables[0].Columns.Count; m++)//
                {
                    //switch (dv[n][m].GetType().ToString())
                    //{
                    //    case "System.DateTime":
                    //        ValueAll += "'" + (Convert.ToDateTime(dv[n][m])).ToString("yyyy-MM-dd") + "'";
                    //        ValueAll += ",";
                    //        break;
                    //    case "System.String":
                    //        ValueAll += "'" + dv[n][m].ToString() + "'";
                    //        ValueAll += ",";
                    //        break;
                    //    case "System.Int32":
                    //        ValueAll += Convert.ToInt32(dv[n][m]);
                    //        ValueAll += ",";
                    //        break;
                    //    case "System.Single":
                    //        ValueAll += Convert.ToSingle(dv[n][m]);
                    //        ValueAll += ",";
                    //        break;
                    //    case "System.Double":
                    //        ValueAll += Convert.ToDouble(dv[n][m]);
                    //        ValueAll += ",";
                    //        break;
                    //    case "System.Decimal":
                    //        ValueAll += Convert.ToDecimal(dv[n][m]);
                    //        ValueAll += ",";
                    //        break;
                    //    default:
                    //        ValueAll += "'" + dv[n][m].ToString() + "'";
                    //        ValueAll += ",";
                    //        break;
                    //}
                    //ValueAll += "''" + dv[n][m].ToString() + "''";
                    ValueAll += "'" + dv[n][m].ToString().Replace("'", "''") + "'";
                    ValueAll += ",";
                }
                ValueAll = ValueAll.Substring(0, ValueAll.Length - 1); //去掉最后一个“，”
                allSql.Append("insert into " + TableName + " (" + FieldAll + ") values(" + ValueAll + ");");//insert
                ValueAll = "";//清空  继续循环
            }
            return allSql; //返回语句
        }
    }
}