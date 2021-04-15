using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Web;
using System.Web.Services;

namespace NcDatabaseToSQL
{
    /// <summary>
    /// GetU8AccountCode 的摘要说明
    /// </summary>
    [WebService(Namespace = "http://tempuri.org/")]
    [WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
    [System.ComponentModel.ToolboxItem(false)]
    // 若要允许使用 ASP.NET AJAX 从脚本中调用此 Web 服务，请取消注释以下行。 
    // [System.Web.Script.Services.ScriptService]
    public class GetU8AccountCode : System.Web.Services.WebService
    {

        private static string connectionString = ConfigurationManager.ConnectionStrings["U8DataConnCode"].ToString();

        [WebMethod]
        public string GetU8Code()
        {
            return connectionString;
        }
    }
}
