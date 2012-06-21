using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using Newtonsoft.Json;
using ProtoBuf;
using ZMQ;

namespace Grapevine.Client
{
    public static class MessageTypeRegistry
    {
        public static readonly ConcurrentDictionary<string, Type> MessageTypes = new ConcurrentDictionary<string, Type>();

        public static void Register<T>()
        {
            var messageType = typeof(T);

            var dataContract = messageType.GetCustomAttributes(typeof(DataContractAttribute), false).FirstOrDefault() as DataContractAttribute;
            if (dataContract == null)
                throw new InvalidOperationException(string.Format("Missing [DataContract] attribute on message type '{0}'.", messageType.FullName));

            var name = dataContract.Name;
            if (string.IsNullOrWhiteSpace(name))
                name = messageType.FullName;

            MessageTypes.TryAdd(name, messageType);
        }
    }

    public static class ExtendSocket
    {
        static ConcurrentDictionary<Type, string> _typeCache = new ConcurrentDictionary<Type, string>();

        static string GetTypeName(Type messageType)
        {
            return _typeCache.GetOrAdd
            (
                messageType,
                _ =>
                {
                    var dataContract = messageType.GetCustomAttributes(typeof(DataContractAttribute), false).FirstOrDefault() as DataContractAttribute;
                    if (dataContract == null)
                        throw new InvalidOperationException(string.Format("Missing [DataContract] attribute on message type '{0}'.", messageType.FullName));

                    var name = dataContract.Name;
                    if (string.IsNullOrWhiteSpace(name))
                        name = messageType.FullName;

                    return name;
                }
            );
        }

        public static void SendProtoBuf<T>(this Socket socket, T message)
        {
            var messageType = GetTypeName(typeof(T));

            socket.SendMore(messageType, Encoding.UTF8);

            using (var ms = new MemoryStream())
            {
                Serializer.Serialize(ms, message);
                ms.Flush();
                socket.Send(ms.ToArray());
            }
        }
    
        public static object RecvProtoBuf(this Socket socket)
        {
            // read the type name as frame 1
            var typeName = socket.Recv(Encoding.UTF8);
            var type     = MessageTypeRegistry.MessageTypes[typeName];

            // ready message data as frame 2
            using (var ms = new MemoryStream(socket.Recv()))
            using (var r = new StreamReader(ms, Encoding.Unicode))
            {
                return Serializer.NonGeneric.Deserialize(type, ms);
            }
        }

        public static object RecvJson(this Socket socket)
        {
            // read the type name as frame 1
            var typeName = socket.Recv(Encoding.UTF8, SendRecvOpt.SNDMORE);
            var type     = MessageTypeRegistry.MessageTypes[typeName];

            var json    = socket.Recv(Encoding.UTF8);
            var message = JsonConvert.DeserializeObject(json, type);

            return message;
        }
    }
}
