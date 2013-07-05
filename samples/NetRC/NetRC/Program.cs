using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

using PushTechnology.DiffusionRemoting;
using PushTechnology.DiffusionCore.Connection.Connectors.Connection;
using PushTechnology.DiffusionCore.Messaging.Enums;
using PushTechnology.DiffusionCore.Messaging.Topic;
using PushTechnology.DiffusionRemoting.Topics;
using PushTechnology.DiffusionRemoting.Args;

namespace NetRC
{
    class DiffusionRC
    {
        private IRemoteService theRemoteService;
        private EventWaitHandle m_event = new ManualResetEvent(false);

        public DiffusionRC(string url, string controlTopic, string rootTopic)
        {
            theRemoteService = RemoteServiceFactory.CreateRemoteService(ConnectionFactory.CreateServerDetails(url),controlTopic,rootTopic);
            theRemoteService.Registered += Registered;
            theRemoteService.ClientConnected += clientConnected;
            theRemoteService.ClientDisconnected += clientDisconnected;
            theRemoteService.ClientSubscribe += clientSubscribe;
            theRemoteService.Closed += closed;
            theRemoteService.MessageFromClient += messageFromClient;
            theRemoteService.RegisterFailed += RegisterFailed;
            theRemoteService.TopicAddFailed += TopicAddFailed;
            theRemoteService.TopicSubscribeFailed += TopicSubscribeFailed;
            theRemoteService.Register();

        }

        private void clientConnected(object sender, ClientDetailsEventArgs arg0)
        {
            System.Console.WriteLine("Client Connected");
        }

  
        private void clientDisconnected(object sender, ClientDisconnectedEventArgs arg0)
        {
            System.Console.WriteLine("Client DisConnected");
 
        }

 
        private void clientSubscribe(object sender, ClientSubscribeEventArgs arg0)
        {
            System.Console.WriteLine("Client Subscribe");
            //Allow client to subscribe
            theRemoteService.SubscribeClient(arg0.ClientDetails.ClientId, arg0.TopicName);
  
        }

  
        private void closed(object sender, ServiceClosedEventArgs reason)
        {
            System.Console.WriteLine("Remote Service Closed");
        }

        private void messageFromClient(object sender, ClientMessageEventArgs arg)
        {
            System.Console.WriteLine("Message From Client on Topic {0}", arg.TopicName);
            if (arg.TopicName == "ECHO")
            {
               theRemoteService.SendToClient(arg.ClientDetails.ClientId, arg.Message);
            }

        }
  
        private void TopicAddFailed(object sender, TopicAddFailedEventArgs arg)
        {
            System.Console.WriteLine("Topic add failed: " + arg.TopicName + ", " + arg.ErrorMessage);
        }

        private void TopicSubscribeFailed(object sender, TopicSubscribeFailedEventArgs arg)
        {
            System.Console.WriteLine("Topic subscribed failed: " + arg.ClientId + ", " + arg.ErrorMessage + ", " + arg.TopicName);
        }

  
        private void Registered(object sender, EventArgs args)
        {
              System.Console.WriteLine("RC Registered");
              m_event.Set();

        }
        private void RegisterFailed(object sender, RemoteServiceRegistrationFailedEventArgs args)
        {
            System.Console.WriteLine("Register Failed");
            m_event.Set();

        }

        public void awaitRegister()
        {
            m_event.WaitOne();
        }

        public void addSimpleTopic(string myTopic)
        {
            theRemoteService.AddTopic(myTopic, new SimpleTopicSpecification());

        }
        public void updateSimpleTopic(string myTopic, string myData)
        {
            ITopicMessage msg=theRemoteService.CreateDeltaMessage(myTopic, 200);
            msg.Put(myData);
            theRemoteService.Publish(msg);
        }

    }

    class Program
    {
        public const string ROOT_TOPIC = "NET_RC_Topics";
        public const string CONTROL_TOPIC = "RemoteControl";
        public const string URL = "dpt://localhost:8080";



        static void Main(string[] args)
        {
            System.Console.WriteLine("Starting Remote Service {0}", DateTime.Now.ToLongTimeString());
            DiffusionRC remoteControl = new DiffusionRC(URL, CONTROL_TOPIC, ROOT_TOPIC);
            remoteControl.awaitRegister();

            //Create Topic for time
            remoteControl.addSimpleTopic("TIME");

            //Create Topic for ECHO
            remoteControl.addSimpleTopic("ECHO");

            while (true)
            {
                remoteControl.updateSimpleTopic("TIME", DateTime.Now.ToLongTimeString());
                System.Console.WriteLine("Updating Topic TIME {0}", DateTime.Now.ToLongTimeString());
                Thread.Sleep(1000);

            }
       }
    }
}
