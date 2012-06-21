using System;
using Grapevine.Core;
using ZMQ;

namespace Grapevine.Server
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var context = new Context())
            {
                var hub = new Hub(context, "tcp://*:5559", "tcp://*:5560");

                Console.WriteLine("Starting hub...");
                hub.Start();
                Console.WriteLine("Hub started. Press any key to exit.");
                Console.ReadKey();
                hub.Stop();
            }
        }
    }
}
