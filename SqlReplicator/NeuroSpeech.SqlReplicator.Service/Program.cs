using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace NeuroSpeech.SqlReplicator.Service
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main(string[] args)
        {

            if (args.Length > 0) {
                if (args.Any(x => string.Equals(x, "-install", StringComparison.OrdinalIgnoreCase))) {
                    InstallService();
                    return;
                }
                if (args.Any(x => string.Equals(x, "-uninstall", StringComparison.OrdinalIgnoreCase)))
                {
                    UnInstallService();
                    return;
                }
            }


            ServiceBase[] ServicesToRun;
            ServicesToRun = new ServiceBase[]
            {
                new Service1()
            };
            ServiceBase.Run(ServicesToRun);
        }


        const string serviceName = "NeuroSpeech.SqlReplicator.Serivce";
        const string serviceTitle = "NeuroSpeech SQL Replication based on Change Tracking";
        const string serviceDescription = "Replication based on Change Tracking";


        private static void UnInstallService()
        {
            IntegratedServiceInstaller Inst = new IntegratedServiceInstaller();
            Inst.Uninstall(serviceName);
        }

        private static void InstallService()
        {
            IntegratedServiceInstaller Inst = new IntegratedServiceInstaller();
            Inst.Install(serviceName, serviceTitle, serviceDescription,
                // System.ServiceProcess.ServiceAccount.LocalService,      // this is more secure, but only available in XP and above and WS-2003 and above
                System.ServiceProcess.ServiceAccount.LocalSystem,       // this is required for WS-2000
                System.ServiceProcess.ServiceStartMode.Automatic);
        }
    }
}
