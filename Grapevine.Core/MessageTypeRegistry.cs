using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace Grapevine.Core
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
}
