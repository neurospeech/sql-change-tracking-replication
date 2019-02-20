using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlReplicatorConsole
{
    class Program
    {
        static void Main(string[] args)
        {
            SqlServerTypes1.Utilities.LoadNativeAssemblies(AppDomain.CurrentDomain.BaseDirectory);

            // start the task...
            SqlReplicator.Core.DatabaseReplicatorTask task = new SqlReplicator.Core.DatabaseReplicatorTask();
            var config = task.Job = new SqlReplicator.Core.ConfigJob
            {
                Destination = new SqlReplicator.Core.ConfigDatabase { },
                Source = new SqlReplicator.Core.ConfigDatabase { }
            };

            var settings = System.Configuration.ConfigurationManager.AppSettings;

            LoadConfig(config.Source, settings, "Source.");
            LoadConfig(config.Destination, settings, "Destination.");

            RunAsync(task).Wait();
        }

        private static async Task RunAsync(SqlReplicator.Core.DatabaseReplicatorTask task)
        {
            try
            {
                await task.ReplicateAsync();
            } catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        private static void LoadConfig(
            SqlReplicator.Core.ConfigDatabase cs, 
            NameValueCollection settings, string source)
        {
            foreach (var key in settings.AllKeys)
            {
                if (key.StartsWith(source))
                {
                    var name = key.Substring(source.Length );
                    var prop = cs.GetType().GetProperty(name);
                    var value = settings[key];
                    var v = Convert.ChangeType(value, prop.PropertyType);
                    prop.SetValue(cs, v);
                }
            }
        }
    }
}
