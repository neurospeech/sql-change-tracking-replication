using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace SqlReplicator.Core
{
    public class Config: AtomModel
    {


        public ObservableCollection<ConfigJob> Jobs { get; }
            = new ObservableCollection<ConfigJob>();



    }

    public class ConfigJob : AtomModel {

        #region Property Source

        private ConfigDatabase _Source = new ConfigDatabase();

        public ConfigDatabase Source
        {
            get
            {
                return _Source;
            }
            set
            {
                SetProperty(ref _Source, value);
            }
        }
        #endregion

        #region Property Destination

        private ConfigDatabase _Destination = new ConfigDatabase();

        public ConfigDatabase Destination
        {
            get
            {
                return _Destination;
            }
            set
            {
                SetProperty(ref _Destination, value);
            }
        }
        #endregion

        #region Property Active

        private bool _Active = true;

        public bool Active
        {
            get
            {
                return _Active;
            }
            set
            {
                SetProperty(ref _Active, value);
            }
        }
        #endregion

        [JsonIgnore]
        public List<SqlTable> Tables { get; }
            = new List<SqlTable>();

    }

    public class ConfigDatabase : AtomModel {

        public ConfigDatabase()
        {
            SecurePassword = Protect("");   
        }

        #region Property Server

        private string _Server = "";

        public string Server
        {
            get
            {
                return _Server;
            }
            set
            {
                SetProperty(ref _Server, value);
            }
        }
        #endregion

        #region Property Username

        private string _Username = "";

        public string Username
        {
            get
            {
                return _Username;
            }
            set
            {
                SetProperty(ref _Username, value);
            }
        }
        #endregion

        #region Property Password

        
        [JsonIgnore]
        public string Password
        {
            get
            {
                if (SecurePassword == null)
                    return null;
                
                return Unprotect(SecurePassword);
            }
            set
            {
                string old = this.Password;
                if (old == value)
                    return;

                SecurePassword = Protect(value);
                OnPropertyChanged();
            }
        }
        #endregion


        #region Property Provider

        private string _Provider = "System.Data.SqlClient";

        public string Provider
        {
            get
            {
                return _Provider;
            }
            set
            {
                SetProperty(ref _Provider, value);
            }
        }
        #endregion



        public string SecurePassword { get; set; }


        #region Property Database

        private string _Database = "";

        public string Database
        {
            get
            {
                return _Database;
            }
            set
            {
                SetProperty(ref _Database, value);
            }
        }

        public string ConnectionString { get;  set; }
        public bool TrustedConnection { get;  set; }
        public bool Encrypt { get; internal set; }
        public bool TrustCertificate { get;  set; }
        #endregion

        private static readonly byte[] s_aditionalEntropy = new byte[] { 1,9,7,9};

        private static string Protect(string text)
        {
            try
            {
                // Encrypt the data using DataProtectionScope.CurrentUser. The result can be decrypted
                //  only by the same current user.
                var data = System.Text.Encoding.UTF8.GetBytes(text ?? "");
                data = ProtectedData.Protect(data, s_aditionalEntropy, DataProtectionScope.LocalMachine);
                return Convert.ToBase64String(data);
            }
            catch (CryptographicException e)
            {
                Console.WriteLine("Data was not encrypted. An error occurred.");
                Console.WriteLine(e.ToString());
                return null;
            }
        }

        private static string Unprotect(string text)
        {
            try
            {
                //Decrypt the data using DataProtectionScope.CurrentUser.
                var data = Convert.FromBase64String(text);
                data = ProtectedData.Unprotect(data, s_aditionalEntropy, DataProtectionScope.LocalMachine);
                return System.Text.Encoding.UTF8.GetString(data);
            }
            catch (CryptographicException e)
            {
                Console.WriteLine("Data was not decrypted. An error occurred.");
                Console.WriteLine(e.ToString());
                return null;
            }
        }

        internal virtual async Task<SqlQuery> OpenAsync()
        {
            if (Provider.Equals("System.Data.SqlClient", StringComparison.OrdinalIgnoreCase)) {
                var q = new SqlServerQuery(this);
                await q.Open();
                return q;
            }

            if (Provider.Equals("MySql.Data.MySqlClient", StringComparison.OrdinalIgnoreCase))
            {
                var q = new MySqlQuery(this);
                await q.Open();
                return q;
            }

            throw new NotImplementedException();
        }
    }
}
