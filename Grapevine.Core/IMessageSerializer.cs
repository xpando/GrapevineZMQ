using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Grapevine.Core
{
    public interface IMessageSerializer
    {
        byte[] Serialize(object message);
        object Deserialize(byte[] buffer, Type type);
    }

    public static class ExtendMessageSerializer
    {
        public static T Deserialize<T>(this IMessageSerializer serializer, byte[] buffer) where T : class
        {
            return serializer.Deserialize(buffer, typeof(T)) as T;
        }
    }
}
