using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using ProtoBuf;

namespace Grapevine.Core
{
    public class ProtobufMessageSerializer : IMessageSerializer
    {
        public byte[] Serialize(object message)
        {
            using (var ms = new MemoryStream())
            {
                Serializer.NonGeneric.Serialize(ms, message);
                ms.Flush();
                return ms.ToArray();
            }
        }

        public object Deserialize(byte[] buffer, Type type)
        {
            using (var ms = new MemoryStream(buffer))
            {
                return Serializer.NonGeneric.Deserialize(type, ms);
            }
        }
    }
}
