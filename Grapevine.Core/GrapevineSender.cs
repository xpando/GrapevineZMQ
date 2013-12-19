using System;
using System.Text;
using ZeroMQ;

namespace Grapevine.Core
{
    public class GrapevineSender : IDisposable
    {
        IMessageSerializer _serializer;
        ZmqContext _context;
        ZmqSocket _pubSocket;

        public GrapevineSender(ZmqContext context, string subAddress, IMessageSerializer serializer)
        {
            _serializer = serializer;
            _context    = context;
            _pubSocket  = _context.CreateSocket(SocketType.PUB);

            _pubSocket.Connect(subAddress);
        }

        public void Send(object message, string topic = null)
        {
            var typeName = MessageTypeRegistry.GetTypeName(message.GetType());
            var data     = _serializer.Serialize(message);

            if (topic == null)
                topic = typeName;

            var msg = new ZmqMessage();
            msg.Append(new Frame(ZmqContext.DefaultEncoding.GetBytes(topic)));
            msg.Append(new Frame(ZmqContext.DefaultEncoding.GetBytes(typeName)));
            msg.Append(new Frame(data));

            _pubSocket.SendMessage(msg);
        }

        public void Dispose()
        {
            if (_pubSocket != null)
                _pubSocket.Dispose();

            _pubSocket = null;
        }
    }
}
