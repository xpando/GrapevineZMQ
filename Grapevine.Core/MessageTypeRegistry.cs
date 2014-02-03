using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Runtime.Serialization;

namespace Grapevine.Core
{
    public static class MessageTypeRegistry
    {
        static readonly ConcurrentDictionary<string, Type> _messageTypes = new ConcurrentDictionary<string, Type>();
        static ConcurrentDictionary<Type, string> _typeNameCache = new ConcurrentDictionary<Type, string>();

        public static bool IsRegistered<MessageType>()
        {
            return IsRegistered(typeof(MessageType));
        }

        public static bool IsRegistered(Type messageType)
        {
            return _typeNameCache.ContainsKey(messageType);
        }

        public static string GetTypeName(Type messageType)
        {
            return _typeNameCache[messageType];
        }

        public static Type Resolve(string typeName)
        {
            if (_messageTypes.ContainsKey(typeName))
                return _messageTypes[typeName];

            return null;
        }

        public static void Register<MessageType>()
        {
            Register(typeof(MessageType));
        }

        public static void Register(Type messageType)
        {
            var dataContract = messageType.GetCustomAttributes(typeof(DataContractAttribute), false).FirstOrDefault() as DataContractAttribute;
            if (dataContract == null)
                throw new InvalidOperationException(string.Format("Missing [DataContract] attribute on message type '{0}'.", messageType.FullName));

            var ns = dataContract.Namespace ?? "http://tempuri.org/";
            if (!ns.EndsWith("/")) ns = ns + "/";
            var name = ns + dataContract.Name ?? messageType.FullName;
            _messageTypes.GetOrAdd(name, messageType);
            _typeNameCache.GetOrAdd(messageType, name);
        }

    }
}
