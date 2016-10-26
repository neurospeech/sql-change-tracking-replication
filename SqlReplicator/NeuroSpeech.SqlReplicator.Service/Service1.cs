using Quartz;
using Quartz.Impl;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace NeuroSpeech.SqlReplicator.Service
{
    public partial class Service1 : ServiceBase
    {
        private IScheduler _sched;

        public Service1()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            ISchedulerFactory sf = new StdSchedulerFactory();
            _sched = sf.GetScheduler();

            _sched.StartDelayed(TimeSpan.FromSeconds(10));
        }

        protected override void OnStop()
        {
            _sched.Shutdown();
        }
    }
}
