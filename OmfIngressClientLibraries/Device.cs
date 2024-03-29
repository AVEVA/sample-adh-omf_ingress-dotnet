﻿using System;
using System.Threading.Tasks;
using OSIsoft.Data.Http;
using OSIsoft.Identity;
using OSIsoft.Omf;
using OSIsoft.OmfIngress;

namespace OmfIngressClientLibraries
{
    public class Device
    {
        private readonly IOmfIngressService _deviceOmfIngressService;

        public Device(string address, string tenantId, string namespaceId, string clientId, string clientSecret)
        {
            // Create the AuthenticationHandler and IngressSerice to use to send data
            AuthenticationHandler deviceAuthenticationHandler = new (new Uri(address), clientId, clientSecret);

            OmfIngressService deviceBaseOmfIngressService = new (new Uri(address), HttpCompressionMethod.None, deviceAuthenticationHandler);
            _deviceOmfIngressService = deviceBaseOmfIngressService.GetOmfIngressService(tenantId, namespaceId);
        }

        public async Task CreateDataPointTypeAsync()
        {
            // Create a DataPointType
            Console.WriteLine($"Creating Type with Id {typeof(DataPointType).Name}");
            Console.WriteLine();

            OmfTypeMessage typeMessage = OmfMessageCreator.CreateTypeMessage(typeof(DataPointType));
            await SendOmfMessageAsync(typeMessage).ConfigureAwait(false);
        }

        public async Task CreateStreamAsync(string streamId)
        {
            // Create container
            Console.WriteLine($"Creating Container with Id {streamId}");
            Console.WriteLine();

            OmfContainerMessage containerMessage = OmfMessageCreator.CreateContainerMessage(streamId, typeof(DataPointType));
            await SendOmfMessageAsync(containerMessage).ConfigureAwait(false);
        }

        public async Task SendValueAsync(string streamId, DataPointType value)
        {
            if (value == null)
            {
                throw new ArgumentException("Value cannot be null.", nameof(value));
            }

            // Send DataPointType values
            OmfDataMessage dataMessage = OmfMessageCreator.CreateDataMessage(streamId, value);
            await SendOmfMessageAsync(dataMessage).ConfigureAwait(false);
            Console.WriteLine($"Sent data point: Time: {value.Timestamp}, Value: {value.Value}");
        }

        public async Task DeleteDataPointTypeAsync()
        {
            // Delete type
            Console.WriteLine($"Deleting Type with Id {typeof(DataPointType).Name}");
            Console.WriteLine();
            OmfTypeMessage typeMessage = OmfMessageCreator.CreateTypeMessage(typeof(DataPointType));
            typeMessage.ActionType = ActionType.Delete;
            await SendOmfMessageAsync(typeMessage).ConfigureAwait(false);
        }

        public async Task DeleteStreamAsync(string streamId)
        {
            // Delete container
            Console.WriteLine($"Deleting Container with Id {streamId}");
            Console.WriteLine();
            OmfContainerMessage containerMessage = OmfMessageCreator.CreateContainerMessage(streamId, typeof(DataPointType));
            containerMessage.ActionType = ActionType.Delete;
            await SendOmfMessageAsync(containerMessage).ConfigureAwait(false);  
        }
        
        private async Task SendOmfMessageAsync(OmfMessage omfMessage)
        {
            SerializedOmfMessage serializedOmfMessage = OmfMessageSerializer.Serialize(omfMessage);
            await _deviceOmfIngressService.SendOmfMessageAsync(serializedOmfMessage).ConfigureAwait(false);
        }
    }
}
