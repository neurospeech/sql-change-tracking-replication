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

        public bool HasIdentity { get; set; }

        private IEnumerable<SqlColumn> _PrimaryKey = null;
        public IEnumerable<SqlColumn> PrimaryKey
            => _PrimaryKey ?? (_PrimaryKey = Columns.Where(x => x.IsPrimaryKey).ToList());

        public long ID { get; set; }

        public long LastVersion { get; set; }

        public List<SqlColumn> Columns { get; }
            = new List<SqlColumn>();

    }
}
