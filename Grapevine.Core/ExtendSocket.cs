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
        static readonly ConcurrentDictionary<string, Type> _messageTypes = new ConcurrentDictionary<string, Type>();
        static ConcurrentDictionary<Type, string> _typeNameCache = new ConcurrentDictionary<Type, string>();

        public static string GetTypeName(Type messageType)
        {
            return _typeNameCache.GetOrAdd
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

        public static Type Resolve(string typeName)
        {
            if (_messageTypes.ContainsKey(typeName))
                return _messageTypes[typeName];

            return null;
        }

        public static void Register<T>()
        {
            var messageType = typeof(T);

            var dataContract = messageType.GetCustomAttributes(typeof(DataContractAttribute), false).FirstOrDefault() as DataContractAttribute;
            if (dataContract == null)
                throw new InvalidOperationException(string.Format("Missing [DataContract] attribute on message type '{0}'.", messageType.FullName));

            var name = dataContract.Name;
            if (string.IsNullOrWhiteSpace(name))
                name = messageType.FullName;

            _messageTypes.TryAdd(name, messageType);
        }
    }

    public static class ExtendSocket
    {
        public static void SendProtoBuf<T>(this Socket socket, T message)
        {
            var typeName = MessageTypeRegistry.GetTypeName(typeof(T));

            socket.SendMore(typeName, Encoding.Unicode);

            using (var ms = new MemoryStream())
            {
                Serializer.Serialize(ms, message);
                ms.Flush();
                socket.Send(ms.ToArray());
            }
        }
    
        public static object RecvProtoBuf(this Socket socket)
        {
            var typeName = socket.Recv(Encoding.Unicode);
            var data = socket.Recv();

            var type = MessageTypeRegistry.Resolve(typeName);
            if (type != null)
            {
                using (var ms = new MemoryStream(data))
                using (var r = new StreamReader(ms))
                {
                    return Serializer.NonGeneric.Deserialize(type, ms);
                }
            }

            return null;
        }

        public static object RecvJson(this Socket socket)
        {
            var typeName = socket.Recv(Encoding.Unicode);
            var json = socket.Recv(Encoding.Unicode);

            var type = MessageTypeRegistry.Resolve(typeName);
            if (type != null)
                return JsonConvert.DeserializeObject(json, type);

            return null;
        }
    }
}
