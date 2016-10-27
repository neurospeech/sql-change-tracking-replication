using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlReplicator.Core
{

    public class SqlTable {

        public string Name { get; set; }

        public string PrimaryKey { get; set; }

        public long ID { get; set; }

        public long LastVersion { get; set; }

        public List<SqlColumn> Columns { get; }
            = new List<SqlColumn>();

    }
}
