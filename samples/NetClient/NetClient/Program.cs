
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using PushTechnology.DiffusionCore.Connection.Interfaces;
using PushTechnology.DiffusionCore.Messaging.Topic;
using PushTechnology.DiffusionExternalClient;
using PushTechnology.DiffusionCore.Connection.Connectors.Connection;
using System.Threading;

namespace NetClient
{
    class DiffusionClient : IServerConnectionListener, ITopicListener
    {
        private ExternalClient theClient;
 
        public DiffusionClient()
        {
            var connectionDetails =
                ConnectionFactory.CreateConnectionDetails("dpt://localhost:8080", "http://localhost:8080");
 
            theClient = new PushTechnology.DiffusionExternalClient.ExternalClient(this,connectionDetails);


            // Add a topic listener - we're listening to all messages for this example, but individual topics can
            // also be used as selectors
            theClient.AddGlobalTopicListener(this);
 
            // Now connect - this is an asynchronous process, so we have to wait until 'ServerConnected' is invoked
            theClient.Connect();
        }




        public void ServerConnected(IDiffusionClientConnector connector)
        {
            System.Console.WriteLine("Server Connected");
            theClient.Subscribe("Techmagnets//");
        }

        public void ServerConnectionAttemptFailed(IDiffusionClientConnector connector, PushTechnology.DiffusionCore.Messaging.ServerClosedEventArgs args)
        {
            System.Console.WriteLine("Server Connection Failed");
        }

        public void ServerDisconnected(IDiffusionClientConnector connector, PushTechnology.DiffusionCore.Messaging.ServerClosedEventArgs args)
        {
            System.Console.WriteLine("Server Disconnected");
        }

        public void ServerRejectedCredentials(IDiffusionClientConnector connector, PushTechnology.DiffusionCore.Credentials credentials)
        {
            System.Console.WriteLine("Server Rejected Credentials");
        }

        public void ServerTopicStatusChanged(IDiffusionClientConnector connector, string topicName, PushTechnology.DiffusionCore.Messaging.Enums.TopicStatus statusType)
        {
            System.Console.WriteLine("Topic Status Changed {0}", topicName);
        }

        public bool HandleTopicMessage(PushTechnology.DiffusionCore.Messaging.IMessageSource source, ITopicMessage message)
        {
            System.Console.WriteLine("Topic Message Data : {0}", message.AsString());
            return (true);
        }
    }
    class Program
    {
        static void Main(string[] args)
        {

            System.Console.WriteLine("Starting Diffusion Client");
            DiffusionClient Client = new DiffusionClient();
            while (true)
            {
                Thread.Sleep(1000);

            }
  
        }


    }
}
