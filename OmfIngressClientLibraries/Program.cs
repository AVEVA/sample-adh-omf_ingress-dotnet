using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using OSIsoft.Data.Http;
using OSIsoft.Identity;
using OSIsoft.OmfIngress;
using OSIsoft.OmfIngress.Models;

namespace OmfIngressClientLibraries
{
    public static class Program
    {
        private static Device _omfDevice;
        private static IConfiguration _config;
        private static IOmfIngressService _omfIngressService;

        public static string Resource { get; set; }
        public static string TenantId { get; set; }
        public static string NamespaceId { get; set; }
        public static string ClientId { get; set; }
        public static string ClientSecret { get; set; }
        public static string ConnectionName { get; set; }
        public static string StreamId { get; set; }
        public static string DeviceClientId { get; set; }
        public static string DeviceClientSecret { get; set; }

        public static void Main()
        {
            Setup();
            Exception toThrow = null;
            OmfConnection omfConnection = null;
            try
            {
                // Create the Connection, send OMF
                omfConnection = CreateOmfConnectionAsync().GetAwaiter().GetResult();
                SendTypeContainerAndDataAsync().GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                toThrow = ex;
            }
            finally
            {
                // Delete the Type and Stream
                try
                {
                    DeleteTypeAndContainerAsync().GetAwaiter().GetResult();
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    toThrow = ex;
                }

                if (omfConnection != null)
                {
                    // Delete the Connection
                    try
                    {
                        DeleteOmfConnectionAsync(omfConnection).GetAwaiter().GetResult();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                        toThrow = ex;
                    }
                }

                Console.WriteLine("Complete!");
            }

            if (toThrow != null)
            {
                throw toThrow;
            }
        }

        public static void Setup()
        {
            IConfigurationBuilder builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json");
            _config = builder.Build();

            // ==== Client constants ====
            TenantId = _config["TenantId"];
            NamespaceId = _config["NamespaceId"];
            Resource = _config["Resource"];
            ClientId = _config["ClientId"];
            ClientSecret = _config["ClientSecret"];
            ConnectionName = _config["ConnectionName"];
            StreamId = _config["StreamId"];
            DeviceClientId = _config["DeviceClientId"];
            DeviceClientSecret = _config["DeviceClientSecret"];

            _omfDevice = new Device(Resource, TenantId, NamespaceId, DeviceClientId, DeviceClientSecret);

            // Get Ingress Services to communicate with server and handle ingress management
            AuthenticationHandler authenticationHandler = new (new Uri(Resource), ClientId, ClientSecret);
            OmfIngressService baseOmfIngressService = new (new Uri(Resource), HttpCompressionMethod.None, authenticationHandler);
            _omfIngressService = baseOmfIngressService.GetOmfIngressService(TenantId, NamespaceId);

            Console.WriteLine($"ADH endpoint at {Resource}");
            Console.WriteLine();            
        }

        public static async Task<OmfConnection> CreateOmfConnectionAsync()
        {
            // Create the Connection
            // Begin creation of OmfConnection
            Console.WriteLine($"Creating an OMF Connection in Namespace {NamespaceId} for Client with Id {DeviceClientId}");
            Console.WriteLine();
            OmfConnectionCreate omfConnectionCreate = new ()
            {
                Name = ConnectionName,
                Description = "This is a sample Connection",
            };
            omfConnectionCreate.ClientIds.Add(DeviceClientId);
            OmfConnection omfConnection = await _omfIngressService.BeginCreateOmfConnectionAsync(omfConnectionCreate).ConfigureAwait(false);

            while (!omfConnection.State.Equals("Active"))
            {
                omfConnection = await _omfIngressService.GetOmfConnectionAsync(omfConnection.Id).ConfigureAwait(false);
                Task.Delay(1000).Wait();
            }

            return omfConnection;
        }

        public static async Task SendTypeContainerAndDataAsync()
        {
            // Create the Type and Stream
            await _omfDevice.CreateDataPointTypeAsync().ConfigureAwait(false);
            await _omfDevice.CreateStreamAsync(StreamId).ConfigureAwait(false);

            // Send random data points
            Random rand = new ();
            Console.WriteLine("Sending 5 OMF Data Messages.");
            for (int i = 0; i < 5; i++)
            {
                DataPointType dataPoint = new () { Timestamp = DateTime.UtcNow, Value = rand.NextDouble() };
                await _omfDevice.SendValueAsync(StreamId, dataPoint).ConfigureAwait(false);
                await Task.Delay(1000).ConfigureAwait(false);
            }
        }

        public static async Task DeleteTypeAndContainerAsync()
        {
            // Delete the Type and Stream
            await _omfDevice.DeleteStreamAsync(StreamId).ConfigureAwait(false);
            await _omfDevice.DeleteDataPointTypeAsync().ConfigureAwait(false);
        }

        public static async Task DeleteOmfConnectionAsync(OmfConnection omfConnection)
        {
            // Delete the Connection           
            if (omfConnection == null)
            {
                throw new ArgumentException("Omf Connection cannot be null", nameof(omfConnection));
            }

            // Delete the Connection
            Console.WriteLine($"Deleting the OMF Connection with Id {omfConnection.Id}");
            Console.WriteLine();

            await _omfIngressService.BeginDeleteOmfConnectionAsync(omfConnection.Id).ConfigureAwait(false);

            bool deleted = false;
            while (!deleted)
            {
                OmfConnections omfConnections = await _omfIngressService.GetOmfConnectionsAsync().ConfigureAwait(false);
                bool found = false;
                foreach (OmfConnection connection in omfConnections.Results)
                {
                    if (string.Equals(connection.Id, omfConnection.Id, StringComparison.InvariantCultureIgnoreCase))
                    {
                        deleted = connection.State.Equals("Deleted");
                        found = true;
                    }
                }

                if (!found)
                {
                    deleted = true;
                }

                Task.Delay(1000).Wait();
            }
        }
    }
}
