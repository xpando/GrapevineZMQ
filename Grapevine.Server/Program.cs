﻿using System;
using Grapevine.Core;
using ZMQ;

namespace Grapevine.Server
{
    class Program
    {
        static void Main(string[] args)
        {
            // TODO: turn this into a TopShelf service.

            using (var context = new Context())
            {
                var hub = new Forwarder(context, "tcp://*:18901", "tcp://*:18902");

                Console.WriteLine("Starting server...");
                hub.Start();
                Console.WriteLine("Server started. Press any key to exit.");
                Console.ReadKey();
                hub.Stop();
            }
        }
    }
}
