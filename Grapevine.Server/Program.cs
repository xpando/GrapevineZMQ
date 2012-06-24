using System;
using System.Threading;
using Grapevine.Core;
using ZeroMQ;
using ZeroMQ.Sockets;
using ZeroMQ.Sockets.Devices;

namespace Grapevine.Server
{
    class Program
    {
        static void Main(string[] args)
        {
            // TODO: turn this into a TopShelf service.

            using (var context = ZmqContext.Create())
            using (var forwarder = ForwarderDevice.CreateThreaded(context))
            {
                forwarder.ConfigureFrontend().BindTo("tcp://*:5560").SubscribeToAll();
                forwarder.ConfigureBackend().BindTo("tcp://*:5559");

                Console.WriteLine("Starting server...");
                forwarder.Start();

                Console.WriteLine("Server started. Press any key to exit.");
                while (!Console.KeyAvailable)
                    Thread.Yield();

                forwarder.Stop();
            }
        }
    }
}
