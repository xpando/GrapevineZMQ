using System;
using System.Text;
using Grapevine.Core;
using ZeroMQ;
using ZeroMQ.Devices;

namespace Grapevine.Server
{
    class Program
    {
        static void Main(string[] args)
        {
            // TODO: turn this into a TopShelf service.

            using (var context = ZmqContext.Create())
            using (var forwarder = new ForwarderDevice(context, "tcp://*:18901", "tcp://*:18902", ZeroMQ.Devices.DeviceMode.Threaded))
            {
                // forward all messages
                forwarder.FrontendSetup.SubscribeAll();

                Console.WriteLine("Starting server...");
                forwarder.Start();
                Console.WriteLine("Server started. Press any key to exit.");
                Console.ReadKey();
            }
        }
    }
}
