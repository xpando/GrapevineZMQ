using System;
using System.Text;
using ZMQ;

namespace Grapevine.Core
{
    public class GrapevineSender : IDisposable
    {
        IMessageSerializer _serializer;
        Context _context;
        Socket _socket;

        public GrapevineSender(Context context, string address, IMessageSerializer serializer)
        {
            _serializer = serializer;
            _context = context;
            _socket = _context.Socket(SocketType.PUSH);
            _socket.Connect(address);
        }

        public void Send(object message)
        {
            var typeName = MessageTypeRegistry.GetTypeName(message.GetType());
            var data = _serializer.Serialize(message);

            _socket.SendMore(typeName, Encoding.Unicode);
            _socket.Send(data);
        }

        public void Dispose()
        {
            if (_socket != null)
                _socket.Dispose();

            _socket = null;
        }
    }
}
