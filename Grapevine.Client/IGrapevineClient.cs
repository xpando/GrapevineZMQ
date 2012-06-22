using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq.Expressions;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Grapevine.Core;
using ZMQ;

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
        Context _context = new Context();
        GrapevineSender _sender;
        GrapevineReceiver _receiver;

        public GrapevineClient(string pubAddress, string subAddress)
        {
            _sender = new GrapevineSender(_context, pubAddress, _serializer);
            _receiver = new GrapevineReceiver(_context, subAddress, _serializer);
            _receiver.Connect();
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
            return _receiver.OfType<MessageType>();//.SubscribeOn(Scheduler.TaskPool);
        }

        public IObservable<MessageType> Receive<MessageType>(Expression<Func<MessageType,bool>> filter)
        {
            return Receive<MessageType>().Where(filter.Compile());
        }

        public void Dispose()
        {
            _sender.Dispose();
            _receiver.Dispose();
 	        _context.Dispose();
        }
    }
}