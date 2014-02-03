using System;
using System.Reactive.Linq;
using System.Runtime.Serialization;
using Grapevine.Client;

namespace Grapevine.Demo.Chat
{
    [DataContract(Namespace="http://mkcorp.com/chat", Name="message")]
    public class ChatRoomMessage
    {
        [DataMember(Order = 1)]
        public string Room { get; set; }

        [DataMember(Order = 2)]
        public string From { get; set; }

        [DataMember(Order = 3)]
        public string Message { get; set; }
    }

    class Program
    {
        static void Main(string[] args)
        {
            Console.Write("Enter user name: ");
            var userName = Console.ReadLine();
            if (string.IsNullOrWhiteSpace(userName))
                userName = string.Format(@"{0}\{1}", Environment.UserDomainName, Environment.UserName);

            Console.Write("Enter room name: ");
            var room = Console.ReadLine();

            using (var client = new GrapevineClient("tcp://localhost:18902", "tcp://localhost:18901"))
            {
                Console.WriteLine("Connecting to chat server...");

                var topic = string.Format("chat:{0}", room.ToLower());
                var messages = client.Receive<ChatRoomMessage>(topic)
                    .Where(m => m.From != userName)
                    .Subscribe(m => Console.WriteLine("[{0}] {1}", m.From, m.Message));

                Console.WriteLine("Connected. Type your chat messages and press enter to send or type quit to exit.");

                using (messages)
                {
                    bool done = false;
                    while (!done)
                    {
                        var input = Console.ReadLine();
                        if (string.Equals(input, "quit", StringComparison.CurrentCultureIgnoreCase))
                            done = true;

                        if (!done)
                            client.Send(new ChatRoomMessage { Room = room, From = userName, Message = input }, topic);
                    };
                }
            }
        }
    }
}
