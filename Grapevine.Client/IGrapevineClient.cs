using System;
using System.Linq.Expressions;
using System.Reactive.Linq;
using Grapevine.Core;
using ZeroMQ;
using ZeroMQ.Sockets;

namespace Grapevine.Client
{
    /// <summary>
    /// 
    /// </summary>
    public interface IGrapevineClient
    {
        /// <summary>
        /// Publishes a message to the Grapevine server.
        /// </summary>
        void Send<MessageType>(MessageType message);

        /// <summary>
        /// Creates an observable listener for the specified message type. 
        /// No message filtering is performed on the server.
        /// 
        /// NOTE: call Subscribe on the returned observable to receive messages
        /// and dont forget to dispose your subscription when you are done.
        /// 
        /// If you are not using the Rx framework then be sure to add a 
        /// using MaryKay.Grapevine.Extensions to make calling Subscribe
        /// on the observable much easier.
        /// </summary>
        IObservable<MessageType> Receive<MessageType>();

        /// <summary>
        /// Creates an observable listener for the specified message type
        /// and filters messages on the server using the provided filter
        /// expression.
        /// 
        /// NOTE: call Subscribe on the returned observable to receive messages
        /// and dont forget to dispose your subscription when you are done.
        /// 
        /// If you are not using the Rx framework then be sure to add a 
        /// using MaryKay.Grapevine.Extensions to make calling Subscribe
        /// on the observable much easier.
        /// </summary>
        IObservable<MessageType> Receive<MessageType>(Expression<Func<MessageType,bool>> filter);
    }

    public sealed class GrapevineClient : IGrapevineClient, IDisposable
    {
        IMessageSerializer _serializer = new ProtobufMessageSerializer();
        IZmqContext _context = ZmqContext.Create();
        GrapevineSender _sender;
        IGrapevineReceiver _receiver;

        public GrapevineClient(string pubAddress, string subAddress)
        {
            var receiverFactory = new GrapevineReceiverFactory(_context, _serializer);

            _sender = new GrapevineSender(_context, pubAddress, _serializer);
            _receiver = receiverFactory.Create(subAddress);
        }

        void IGrapevineClient.Send<MessageType>(MessageType message)
        {
            _sender.Send(message);
        }

        public IObservable<MessageType> Receive<MessageType>()
        {
            MessageTypeRegistry.Register<MessageType>();
            var typeName = MessageTypeRegistry.GetTypeName(typeof(MessageType));
            _receiver.AddTopic(typeName);
            return _receiver.Messages.OfType<MessageType>();
        }

        public IObservable<MessageType> Receive<MessageType>(Expression<Func<MessageType,bool>> filter)
        {
            return Receive<MessageType>().Where(filter.Compile());
        }

        public void Dispose()
        {
            _sender.Dispose();
 	        _context.Dispose();
        }
    }
}