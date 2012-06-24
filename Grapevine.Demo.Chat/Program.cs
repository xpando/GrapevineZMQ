using System;
using System.Runtime.Serialization;
using Grapevine.Client;

namespace Grapevine.Demo.Chat
{
    [DataContract(Name="chat-message")]
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
            //MessageTypeRegistry.Register<ChatRoomMessage>();

            Console.Write("Enter user name: ");
            var userName = Console.ReadLine();
            if (string.IsNullOrWhiteSpace(userName))
                userName = string.Format(@"{0}\{1}", Environment.UserDomainName, Environment.UserName);

            Console.Write("Enter room name: ");
            var room = Console.ReadLine();

            var client = new GrapevineClient("tcp://localhost:5560", "tcp://localhost:5559") as IGrapevineClient;

            Console.WriteLine("Connecting to chat server...");
            
            var sub = client
                .Receive<ChatRoomMessage>
                (
                    message => 
                        message.From != userName && 
                        string.Equals(message.Room, room, StringComparison.CurrentCultureIgnoreCase)
                )
                .Subscribe
                (
                    m => 
                    Console.WriteLine("[{0}] {1}", m.From, m.Message)
                );

            Console.WriteLine("Connected. Type your chat messages and press enter to send or type quit to exit.");

            bool done = false;
            while (!done)
            {
                var input = Console.ReadLine();
                if (string.Equals(input, "quit", StringComparison.CurrentCultureIgnoreCase))
                    done = true;

                if (!done)
                    client.Send(new ChatRoomMessage { Room = room, From = userName, Message = input });
            };

            sub.Dispose();
        }
    }
}
